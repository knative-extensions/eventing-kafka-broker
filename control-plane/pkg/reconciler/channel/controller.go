/*
 * Copyright 2021 The Knative Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package channel

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/eventingtls"
	serviceinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/network"

	messagingv1beta "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	kafkachannelinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/informers/messaging/v1beta1/kafkachannel"
	kafkachannelreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"

	subscriptioninformer "knative.dev/eventing/pkg/client/injection/informers/messaging/v1/subscription"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	"knative.dev/eventing/pkg/apis/feature"

	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	consumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/pkg/controller"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/consumergroup"
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *config.Env) *controller.Impl {

	configmapInformer := configmapinformer.Get(ctx)
	channelInformer := kafkachannelinformer.Get(ctx)
	consumerGroupInformer := consumergroupinformer.Get(ctx)

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(conditionSet)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			ContractConfigMapName:       configs.ContractConfigMapName,
			ContractConfigMapFormat:     configs.ContractConfigMapFormat,
			DataPlaneNamespace:          configs.SystemNamespace,
			ReceiverLabel:               base.ChannelReceiverLabel,
		},
		ConsumerGroupLister: consumerGroupInformer.Lister(),
		InternalsClient:     consumergroupclient.Get(ctx),
		Env:                 configs,
		ConfigMapLister:     configmapInformer.Lister(),
		ServiceLister:       serviceinformer.Get(ctx).Lister(),
		SubscriptionLister:  subscriptioninformer.Get(ctx).Lister(),
		KafkaFeatureFlags:   apisconfig.DefaultFeaturesConfig(),
	}

	clientPool := clientpool.Get(ctx)
	if clientPool == nil {
		reconciler.GetKafkaClusterAdmin = clientpool.DisabledGetKafkaClusterAdminFunc
	} else {
		reconciler.GetKafkaClusterAdmin = clientPool.GetClusterAdmin
	}

	logger := logging.FromContext(ctx)

	_, err := reconciler.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		logger.Fatal("Failed to get or create data plane config map",
			zap.String("configmap", configs.DataPlaneConfigMapAsString()),
			zap.Error(err),
		)
	}

	var globalResync func(obj interface{})
	featureStore := feature.NewStore(logger.Named("feature-config-store"), func(name string, value interface{}) {
		if globalResync != nil {
			globalResync(nil)
		}
		err = reconciler.UpdateReceiverConfigFeaturesUpdatedAnnotation(ctx, logger.Desugar())
		if err != nil {
			logger.Warn("config-features updated, but the receiver pods were not successfully annotated. This may lead to features not working as expected.", zap.Error(err))
		}
	})
	featureStore.WatchConfigs(watcher)

	impl := kafkachannelreconciler.NewImpl(ctx, reconciler,
		func(impl *controller.Impl) controller.Options {
			return controller.Options{
				ConfigStore: featureStore,
			}
		})

	globalResync = func(obj interface{}) {
		impl.GlobalResync(channelInformer.Informer())
	}

	kafkaConfigStore := apisconfig.NewStore(ctx, func(name string, value *apisconfig.KafkaFeatureFlags) {
		reconciler.KafkaFeatureFlags.Reset(value)
		impl.GlobalResync(channelInformer.Informer())
	})
	kafkaConfigStore.WatchConfigs(watcher)

	IPsLister := prober.IdentityIPsLister()
	reconciler.IngressHost = network.GetServiceHostname(configs.IngressName, configs.SystemNamespace)
	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialTLSContext = func(ctx context.Context, net, addr string) (net.Conn, error) {
		clientConfig := eventingtls.NewDefaultClientConfig()
		clientConfig.TrustBundleConfigMapLister = configmapInformer.Lister().ConfigMaps(reconciler.SystemNamespace)
		clientConfig.CACerts, _ = reconciler.getCaCerts()

		tlsConfig, err := eventingtls.GetTLSClientConfig(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS client config: %w", err)
		}
		return network.DialTLSWithBackOff(ctx, net, addr, tlsConfig)
	}
	reconciler.Prober, err = prober.NewComposite(ctx, &http.Client{Transport: transport}, "", "", IPsLister, impl.EnqueueKey)
	if err != nil {
		logger.Fatal("Failed to create prober", zap.Error(err))
	}

	reconciler.Tracker = impl.Tracker

	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.Tracker.OnChanged))

	configmapinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		// Call the tracker's OnChanged method, but we've seen the objects
		// coming through this path missing TypeMeta, so ensure it is properly
		// populated.
		controller.EnsureTypeMeta(
			reconciler.Tracker.OnChanged,
			corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		),
	))

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterWithLabel(eventingtls.TrustBundleLabelKey, eventingtls.TrustBundleLabelValue),
		Handler:    controller.HandleAll(globalResync),
	})

	channelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	// ConsumerGroup changes and enqueue associated channel
	consumerGroupInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: consumergroup.Filter("kafkachannel"),
		Handler:    controller.HandleAll(consumergroup.Enqueue("kafkachannel", impl.EnqueueKey)),
	})

	return impl
}

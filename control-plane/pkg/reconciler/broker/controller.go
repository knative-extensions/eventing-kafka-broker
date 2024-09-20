/*
 * Copyright 2020 The Knative Authors
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

package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/eventingtls"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/network"
	"knative.dev/pkg/resolver"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	DefaultNumPartitions     = 20
	DefaultReplicationFactor = 5
)

func NewController(ctx context.Context, watcher configmap.Watcher, env *config.Env) *controller.Impl {

	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	configmapInformer := configmapinformer.Get(ctx)
	featureFlags := apisconfig.DefaultFeaturesConfig()

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
			ContractConfigMapName:       env.ContractConfigMapName,
			ContractConfigMapFormat:     env.ContractConfigMapFormat,
			DataPlaneNamespace:          env.SystemNamespace,
			DispatcherLabel:             base.BrokerDispatcherLabel,
			ReceiverLabel:               base.BrokerReceiverLabel,
		},
		ConfigMapLister:   configmapInformer.Lister(),
		Env:               env,
		Counter:           counter.NewExpiringCounter(ctx),
		KafkaFeatureFlags: featureFlags,
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
			zap.String("configmap", env.DataPlaneConfigMapAsString()),
			zap.Error(err),
		)
	}

	var globalResync func(interface{})

	featureStore := feature.NewStore(logging.FromContext(ctx).Named("feature-config-store"), func(_ string, obj interface{}) {
		if globalResync != nil {
			globalResync(obj)
		}
		err = reconciler.UpdateReceiverConfigFeaturesUpdatedAnnotation(ctx, logger.Desugar())
		if err != nil {
			logger.Warn("config-features updated, but the receiver pods were not successfully annotated. This may lead to features not working as expected.", zap.Error(err))
		}
	})
	featureStore.WatchConfigs(watcher)

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.BrokerClass, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			ConfigStore:       featureStore,
			PromoteFilterFunc: kafka.BrokerClassFilter()}
	})

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	IPsLister := prober.IPsListerFromService(types.NamespacedName{Namespace: reconciler.DataPlaneNamespace, Name: env.IngressName})

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialTLSContext = func(ctx context.Context, net, addr string) (net.Conn, error) {
		clientConfig := eventingtls.NewDefaultClientConfig()
		clientConfig.TrustBundleConfigMapLister = reconciler.ConfigMapLister.ConfigMaps(reconciler.SystemNamespace)
		clientConfig.CACerts, _ = reconciler.getCaCerts()

		tlsConfig, err := eventingtls.GetTLSClientConfig(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS client config: %w", err)
		}
		return network.DialTLSWithBackOff(ctx, net, addr, tlsConfig)
	}

	reconciler.Prober, err = prober.NewComposite(ctx, &http.Client{Transport: transport}, env.IngressPodPort, env.IngressPodTlsPort, IPsLister, impl.EnqueueKey)
	if err != nil {
		logger.Fatal("Failed to create prober", zap.Error(err))
	}

	brokerInformer := brokerinformer.Get(ctx)

	kafkaConfigStore := apisconfig.NewStore(ctx, func(name string, value *apisconfig.KafkaFeatureFlags) {
		reconciler.KafkaFeatureFlags.Reset(value)
		impl.GlobalResync(brokerInformer.Informer())
	})
	kafkaConfigStore.WatchConfigs(watcher)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	globalResync = func(_ interface{}) {
		impl.GlobalResync(brokerInformer.Informer())
	}

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterWithNameAndNamespace(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				globalResync(obj)
			},
			DeleteFunc: func(obj interface{}) {
				globalResync(obj)
			},
		},
	})

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
	parts := strings.Split(eventingtls.TrustBundleLabelSelector, "=")
	configmapinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterWithLabel(parts[0], parts[1]),
		Handler:    controller.HandleAll(globalResync),
	})

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: reconciler.OnDeleteObserver,
		},
	})

	return impl
}

func ValidateDefaultBackoffDelayMs(env config.Env) error {
	if env.DefaultBackoffDelayMs == 0 {
		return errors.New("default backoff delay cannot be 0")
	}
	return nil
}

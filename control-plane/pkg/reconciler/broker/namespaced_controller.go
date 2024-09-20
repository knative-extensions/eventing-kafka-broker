/*
 * Copyright 2022 The Knative Authors
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
	"fmt"
	"net"
	"net/http"
	"time"

	"knative.dev/eventing/pkg/eventingtls"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/util"

	mfclient "github.com/manifestival/client-go-client"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/tools/cache"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	kubeclient "knative.dev/pkg/client/injection/kube/client"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	deploymentinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/deployment"
	statefulsetinformer "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	namespaceinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/namespace"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"
	serviceinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/service"
	serviceaccountinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/serviceaccount"
	clusterrolebindinginformer "knative.dev/pkg/client/injection/kube/informers/rbac/v1/clusterrolebinding"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const (
	// NamespacedBrokerAdditionalResourcesConfigMapName is the ConfigMap name for the ConfigMap that holds additional
	// resources to be propagated to the target namespace like Prometheus ServiceMonitors, etc.
	NamespacedBrokerAdditionalResourcesConfigMapName = "config-namespaced-broker-resources"
)

func NewNamespacedController(ctx context.Context, watcher configmap.Watcher, env *config.Env) *controller.Impl {
	logger := logging.FromContext(ctx)

	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	configmapInformer := configmapinformer.Get(ctx)

	cfg := injection.GetConfig(ctx)
	mfc, err := mfclient.NewClient(cfg)
	if err != nil {
		logger.Fatal("unable to create Manifestival client-go client", zap.Error(err))
	}

	reconciler := &NamespacedReconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                   kubeclient.Get(ctx),
			PodLister:                    podinformer.Get(ctx).Lister(),
			SecretLister:                 secretinformer.Get(ctx).Lister(),
			DataPlaneConfigConfigMapName: env.DataPlaneConfigConfigMapName,
			DataPlaneConfigMapNamespace:  env.DataPlaneConfigMapNamespace,
			ContractConfigMapName:        env.ContractConfigMapName,
			ContractConfigMapFormat:      env.ContractConfigMapFormat,
			DataPlaneNamespace:           env.SystemNamespace,
			DispatcherLabel:              base.BrokerDispatcherLabel,
			ReceiverLabel:                base.BrokerReceiverLabel,
		},
		NamespaceLister:                    namespaceinformer.Get(ctx).Lister(),
		ConfigMapLister:                    configmapInformer.Lister(),
		ServiceAccountLister:               serviceaccountinformer.Get(ctx).Lister(),
		ServiceLister:                      serviceinformer.Get(ctx).Lister(),
		ClusterRoleBindingLister:           clusterrolebindinginformer.Get(ctx).Lister(),
		StatefulSetLister:                  statefulsetinformer.Get(ctx).Lister(),
		DeploymentLister:                   deploymentinformer.Get(ctx).Lister(),
		BrokerLister:                       brokerinformer.Get(ctx).Lister(),
		Env:                                env,
		Counter:                            counter.NewExpiringCounter(ctx),
		ManifestivalClient:                 mfc,
		DataplaneLifecycleLocksByNamespace: util.NewExpiringLockMap[string](ctx, time.Minute*30),
		KafkaFeatureFlags:                  apisconfig.DefaultFeaturesConfig(),
	}

	clientPool := clientpool.Get(ctx)
	if clientPool == nil {
		reconciler.GetKafkaClusterAdmin = clientpool.DisabledGetKafkaClusterAdminFunc
	} else {
		reconciler.GetKafkaClusterAdmin = clientPool.GetClusterAdmin
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.NamespacedBrokerClass, func(impl *controller.Impl) controller.Options {
		return controller.Options{PromoteFilterFunc: kafka.NamespacedBrokerClassFilter()}
	})

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	reconciler.IPsLister = prober.NewIPListerWithMapping()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialTLSContext = func(ctx context.Context, net, addr string) (net.Conn, error) {
		clientConfig := eventingtls.NewDefaultClientConfig()
		clientConfig.TrustBundleConfigMapLister = configmapInformer.Lister().ConfigMaps(reconciler.SystemNamespace)

		tlsConfig, err := eventingtls.GetTLSClientConfig(clientConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to get TLS client config: %w", err)
		}
		return network.DialTLSWithBackOff(ctx, net, addr, tlsConfig)
	}

	reconciler.Prober, err = prober.NewComposite(ctx, &http.Client{Transport: transport}, env.IngressPodPort, env.IngressPodTlsPort, reconciler.IPsLister.List, impl.EnqueueKey)
	if err != nil {
		logger.Fatal("Failed to create prober", zap.Error(err))
	}

	brokerInformer := brokerinformer.Get(ctx)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.NamespacedBrokerClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	reconciler.Tracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(controller.EnsureTypeMeta(
		reconciler.Tracker.OnChanged,
		corev1.SchemeGroupVersion.WithKind("Secret"),
	)))

	globalResync := func(_ interface{}) {
		impl.GlobalResync(brokerInformer.Informer())
	}

	configmapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterWithLabel(kafka.NamespacedBrokerDataplaneLabelKey, kafka.NamespacedBrokerDataplaneLabelValue),
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				globalResync(obj)
			},
			DeleteFunc: func(obj interface{}) {
				globalResync(obj)
			},
		},
	})

	watcher.Watch(NamespacedBrokerAdditionalResourcesConfigMapName, func(configMap *corev1.ConfigMap) {
		globalResync(configMap)
	})

	statefulsetinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterAny(
			kafka.FilterWithLabel("app", "kafka-broker-dispatcher"),
		),
		Handler: controller.HandleAll(controller.EnsureTypeMeta(
			globalResync,
			appsv1.SchemeGroupVersion.WithKind("StatefulSet"),
		)),
	})
	deploymentinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.FilterAny(
			kafka.FilterWithLabel("app", "kafka-broker-receiver"),
		),
		Handler: controller.HandleAll(controller.EnsureTypeMeta(
			globalResync,
			appsv1.SchemeGroupVersion.WithKind("Deployment"),
		)),
	})

	// we set a label for each resource we create and filter things based on that
	filterFunc := pkgreconciler.LabelFilterFunc(
		kafka.NamespacedBrokerDataplaneLabelKey,
		kafka.NamespacedBrokerDataplaneLabelValue,
		false, // allowUnset
	)

	clusterrolebindinginformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler: controller.HandleAll(controller.EnsureTypeMeta(
			globalResync,
			rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"),
		)),
	})

	serviceaccountinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler: controller.HandleAll(controller.EnsureTypeMeta(
			globalResync,
			corev1.SchemeGroupVersion.WithKind("ServiceAccount"),
		)),
	})

	serviceinformer.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: filterFunc,
		Handler: controller.HandleAll(controller.EnsureTypeMeta(
			globalResync,
			corev1.SchemeGroupVersion.WithKind("Service"),
		)),
	})

	reconciler.Tracker = impl.Tracker
	configmapinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		// Call the tracker's OnChanged method, but we've seen the objects
		// coming through this path missing TypeMeta, so ensure it is properly
		// populated.
		controller.EnsureTypeMeta(
			reconciler.Tracker.OnChanged,
			corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		),
	))

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.NamespacedBrokerClassFilter(),
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: reconciler.OnDeleteObserver,
		},
	})

	return impl
}

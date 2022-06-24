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

package brokeryolo

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/Shopify/sarama"
	mfclient "github.com/manifestival/client-go-client"
	mf "github.com/manifestival/manifestival"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/injection"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	secretinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/secret"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
)

const DataPlaneManifestDirectoryPath = "/dataplane/config/broker"

func NewController(ctx context.Context, watcher configmap.Watcher, env *config.Env) *controller.Impl {
	logger := logging.FromContext(ctx)

	eventing.RegisterAlternateBrokerConditionSet(base.IngressConditionSet)

	configmapInformer := configmapinformer.Get(ctx)

	cfg := injection.GetConfig(ctx)
	mfc, err := mfclient.NewClient(cfg)
	if err != nil {
		logger.Fatal("unable to create Manifestival client-go client", zap.Error(err))
	}

	manifest, err := getBaseDataPlaneManifest(mfc, env)
	if err != nil {
		logger.Fatal("unable to load base dataplane manifest", zap.Error(err))
	}

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			SecretLister:                secretinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
			DataPlaneNamespace:          env.SystemNamespace,
			DispatcherLabel:             base.BrokerDispatcherLabel,
			ReceiverLabel:               base.BrokerReceiverLabel,
		},
		NewKafkaClusterAdminClient: sarama.NewClusterAdmin,
		ConfigMapLister:            configmapInformer.Lister(),
		Env:                        env,
		BaseDataPlaneManifest:      manifest,
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.BrokerClassYolo, func(impl *controller.Impl) controller.Options {
		return controller.Options{PromoteFilterFunc: kafka.BrokerYoloClassFilter()}
	})

	reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)
	reconciler.IPsLister = prober.NewIPListerWithMapping()
	reconciler.Prober = prober.NewAsync(ctx, http.DefaultClient, env.IngressPodPort, reconciler.IPsLister.List, impl.EnqueueKey)

	brokerInformer := brokerinformer.Get(ctx)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerYoloClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	reconciler.SecretTracker = impl.Tracker
	secretinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(reconciler.SecretTracker.OnChanged))

	reconciler.ConfigMapTracker = impl.Tracker
	configmapinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		// Call the tracker's OnChanged method, but we've seen the objects
		// coming through this path missing TypeMeta, so ensure it is properly
		// populated.
		controller.EnsureTypeMeta(
			reconciler.ConfigMapTracker.OnChanged,
			corev1.SchemeGroupVersion.WithKind("ConfigMap"),
		),
	))

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerYoloClassFilter(),
		Handler: cache.ResourceEventHandlerFuncs{
			DeleteFunc: reconciler.OnDeleteObserver,
		},
	})

	return impl
}

func getBaseDataPlaneManifest(client mf.Client, env *config.Env) (mf.Manifest, error) {
	kodatapath := os.Getenv("KO_DATA_PATH")
	if kodatapath == "" {
		return mf.Manifest{}, fmt.Errorf("KO_DATA_PATH is empty")
	}

	dataplaneManifestPath := kodatapath + DataPlaneManifestDirectoryPath
	manifest, err := mf.ManifestFrom(mf.Path(dataplaneManifestPath), mf.UseClient(client))
	if err != nil {
		return mf.Manifest{}, fmt.Errorf("unable to load dataplane manifest from path '%s': %v", dataplaneManifestPath, err)
	}

	if env.DispatcherImage == "" {
		return mf.Manifest{}, fmt.Errorf("unable to find DispatcherImage env var specified with 'BROKER_DISPATCHER_IMAGE'")
	}
	if env.ReceiverImage == "" {
		return mf.Manifest{}, fmt.Errorf("unable to find DispatcherImage env var specified with 'BROKER_RECEIVER_IMAGE'")
	}

	// replace the ${KNATIVE_KAFKA_DISPATCHER_IMAGE} string in dataplane manifest YAML
	// with the value of KAFKA_DISPATCHER_IMAGE
	return manifest.Transform(setImagesForDeployments(map[string]string{
		"${KNATIVE_KAFKA_DISPATCHER_IMAGE}": env.DispatcherImage,
		"${KNATIVE_KAFKA_RECEIVER_IMAGE}":   env.ReceiverImage,
	}))
}

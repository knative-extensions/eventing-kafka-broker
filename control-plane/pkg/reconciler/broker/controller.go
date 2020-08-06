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
	"fmt"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/logging"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/resolver"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	DefaultTopicNumPartitionConfigMapKey      = "default.topic.partitions"
	DefaultTopicReplicationFactorConfigMapKey = "default.topic.replication.factor"
	BootstrapServersConfigMapKey              = "bootstrap.servers"

	DefaultNumPartitions     = 10
	DefaultReplicationFactor = 1
)

func NewController(ctx context.Context, watcher configmap.Watcher, configs *Configs) *controller.Impl {

	eventing.RegisterAlternateBrokerConditionSet(ConditionSet)

	configmapInformer := configmapinformer.Get(ctx)

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                  kubeclient.Get(ctx),
			PodLister:                   podinformer.Get(ctx).Lister(),
			DataPlaneConfigMapNamespace: configs.DataPlaneConfigMapNamespace,
			DataPlaneConfigMapName:      configs.DataPlaneConfigMapName,
			DataPlaneConfigFormat:       configs.DataPlaneConfigFormat,
			SystemNamespace:             configs.SystemNamespace,
		},
		NewClusterAdmin: sarama.NewClusterAdmin,
		KafkaDefaultTopicDetails: sarama.TopicDetail{
			NumPartitions:     DefaultNumPartitions,
			ReplicationFactor: DefaultReplicationFactor,
		},
		ConfigMapLister: configmapInformer.Lister(),
		Configs:         configs,
	}

	logger := logging.FromContext(ctx)

	_, err := reconciler.GetOrCreateDataPlaneConfigMap()
	if err != nil {
		logger.Fatal("Failed to get or create data plane config map",
			zap.String("configmap", configs.DataPlaneConfigMapAsString()),
			zap.Error(err),
		)
	}

	if configs.BootstrapServers != "" {
		reconciler.SetBootstrapServers(configs.BootstrapServers)
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.BrokerClass)

	reconciler.Resolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	brokerInformer := brokerinformer.Get(ctx)

	logger.Info("Register event handlers")

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	cm, err := reconciler.KubeClient.CoreV1().ConfigMaps(configs.SystemNamespace).Get(configs.GeneralConfigMapName, metav1.GetOptions{})
	if err != nil {
		panic(fmt.Errorf("failed to get config map %s/%s: %w", configs.SystemNamespace, configs.GeneralConfigMapName, err))
	}

	reconciler.ConfigMapUpdated(ctx)(cm)

	watcher.Watch(configs.GeneralConfigMapName, reconciler.ConfigMapUpdated(ctx))

	return impl
}

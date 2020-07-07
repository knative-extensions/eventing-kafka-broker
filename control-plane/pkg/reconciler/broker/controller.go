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
	"strconv"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"

	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1beta1/broker"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1beta1/broker"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	defaultTopicNumPartitionConfigMapKey      = "default.topic.partitions"
	defaultTopicReplicationFactorConfigMapKey = "default.topic.replication.factor"
	bootstrapServersConfigMapKey              = "bootstrap.servers"

	DefaultNumPartitions     = 10
	DefaultReplicationFactor = 1
)

var NewClusterAdmin = sarama.NewClusterAdmin

func NewController(ctx context.Context, watcher configmap.Watcher, configs *Configs) *controller.Impl {

	reconciler := &Reconciler{
		Reconciler: &base.Reconciler{
			KubeClient:                        kubeclient.Get(ctx),
			PodLister:                         podinformer.Get(ctx).Lister(),
			BrokersTriggersConfigMapNamespace: configs.BrokersTriggersConfigMapNamespace,
			BrokersTriggersConfigMapName:      configs.BrokersTriggersConfigMapName,
			Format:                            configs.Format,
			SystemNamespace:                   configs.SystemNamespace,
		},
		KafkaDefaultTopicDetails: sarama.TopicDetail{
			NumPartitions:     DefaultNumPartitions,
			ReplicationFactor: DefaultReplicationFactor,
		},
		Configs:  configs,
		Recorder: controller.GetEventRecorder(ctx),
	}

	if configs.BootstrapServers != "" {
		_ = reconciler.SetBootstrapServers(configs.BootstrapServers)
	}

	impl := brokerreconciler.NewImpl(ctx, reconciler, kafka.BrokerClass)

	reconciler.Resolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	brokerInformer := brokerinformer.Get(ctx)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: kafka.BrokerClassFilter(),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	watcher.Watch(configs.GeneralConfigMapName, func(configMap *corev1.ConfigMap) {

		numPartitionsStr := configMap.Data[defaultTopicNumPartitionConfigMapKey]
		replicationFactorStr := configMap.Data[defaultTopicReplicationFactorConfigMapKey]
		bootstrapServers := configMap.Data[bootstrapServersConfigMapKey]

		logger := logging.FromContext(ctx)

		numPartitions, err := strconv.Atoi(numPartitionsStr)
		if err != nil {
			logger.Warn("failed to read number of partitions from config map %s", configs.GeneralConfigMapName)
			return
		}

		replicationFactor, err := strconv.Atoi(replicationFactorStr)
		if err != nil {
			logger.Warn("failed to read replication factor from config map %s", configs.GeneralConfigMapName)
			return
		}

		reconciler.SetDefaultTopicDetails(sarama.TopicDetail{
			NumPartitions:     int32(numPartitions),
			ReplicationFactor: int16(replicationFactor),
		})

		_ = reconciler.SetBootstrapServers(bootstrapServers)
	})

	return impl
}

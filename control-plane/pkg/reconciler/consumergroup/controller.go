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

package consumergroup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	kafkainformer "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource"
	sources "knative.dev/eventing-kafka/pkg/client/listers/sources/v1beta1"
	"knative.dev/eventing/pkg/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/system"

	statefulsetscheduler "knative.dev/eventing/pkg/scheduler/statefulset"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup"
	cgreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
)

const (
	// KafkaSourceScheduler is the key for the scheduler map for any KafkaSource.
	// Keep this constant lowercase.
	KafkaSourceScheduler = "kafkasource"
	// KafkaTriggerScheduler is the key for the scheduler map for any Kafka Trigger.
	// Keep this constant lowercase.
	KafkaTriggerScheduler = "trigger"
	// KafkaChannelScheduler is the key for the scheduler map for any KafkaChannel.
	// Keep this constant lowercase.
	KafkaChannelScheduler = "kafkachannel"

	// ConfigKafkaSchedulerName is the name of the ConfigMap to configure the scheduler.
	ConfigKafkaSchedulerName = "config-kafka-scheduler"
	// ConfigKafkaDeSchedulerName is the name of the ConfigMap to configure the descheduler.
	ConfigKafkaDeSchedulerName = "config-kafka-descheduler"
)

type SchedulerConfig struct {
	StatefulSetName     string
	RefreshPeriod       time.Duration
	Capacity            int32
	SchedulerPolicyType scheduler.SchedulerPolicyType
	SchedulerPolicy     *scheduler.SchedulerPolicy
	DeSchedulerPolicy   *scheduler.SchedulerPolicy
}

func NewController(ctx context.Context) *controller.Impl {

	// TODO(pierDipi) use env variables to configure these.
	c := SchedulerConfig{
		RefreshPeriod:       2 * time.Minute,
		Capacity:            50,
		SchedulerPolicyType: "",
		SchedulerPolicy:     schedulerPolicyFromConfigMapOrFail(ctx, ConfigKafkaSchedulerName),
		DeSchedulerPolicy:   schedulerPolicyFromConfigMapOrFail(ctx, ConfigKafkaDeSchedulerName),
	}

	kafkaLister := kafkainformer.Get(ctx).Lister()
	schedulers := map[string]scheduler.Scheduler{
		KafkaSourceScheduler: createKafkaSourceScheduler(ctx, c, kafkaLister),
		// TODO(pierDipi) Add Kafka Trigger and KafkaChannel schedulers.
	}

	r := &Reconciler{
		SchedulerFunc:   func(s string) scheduler.Scheduler { return schedulers[strings.ToLower(s)] },
		ConsumerLister:  consumer.Get(ctx).Lister(),
		InternalsClient: internalsclient.Get(ctx).InternalV1alpha1(),
		NameGenerator:   names.SimpleNameGenerator,
		SystemNamespace: system.Namespace(),
	}

	impl := cgreconciler.NewImpl(ctx, r)

	consumergroup.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	consumer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(enqueueConsumerGroupFromConsumer(impl.EnqueueKey)))

	return impl
}

func enqueueConsumerGroupFromConsumer(enqueue func(name types.NamespacedName)) func(obj interface{}) {
	return func(obj interface{}) {
		c := obj.(*kafkainternals.Consumer)
		or := c.GetConsumerGroup()
		if or != nil {
			enqueue(types.NamespacedName{Namespace: c.GetNamespace(), Name: or.Name})
		}
	}
}

func createKafkaSourceScheduler(ctx context.Context, c SchedulerConfig, kafkaLister sources.KafkaSourceLister) scheduler.Scheduler {
	return createStatefulSetScheduler(
		ctx,
		SchedulerConfig{
			StatefulSetName:     "kafka-source-dispatcher",
			RefreshPeriod:       c.RefreshPeriod,
			Capacity:            c.Capacity,
			SchedulerPolicyType: c.SchedulerPolicyType,
			SchedulerPolicy:     c.SchedulerPolicy,
			DeSchedulerPolicy:   c.DeSchedulerPolicy,
		},
		func() ([]scheduler.VPod, error) {
			kafkaSources, err := kafkaLister.List(labels.Everything())
			if err != nil {
				return nil, err
			}
			vpods := make([]scheduler.VPod, len(kafkaSources))
			for i := 0; i < len(kafkaSources); i++ {
				vpods[i] = kafkaSources[i]
			}
			return vpods, nil
		},
	)
}

func createStatefulSetScheduler(ctx context.Context, c SchedulerConfig, lister scheduler.VPodLister) scheduler.Scheduler {
	return statefulsetscheduler.NewScheduler(
		ctx,
		system.Namespace(),
		c.StatefulSetName,
		lister,
		c.RefreshPeriod,
		c.Capacity,
		c.SchedulerPolicyType,
		// TODO add nodes to controller cluster role
		nodeinformer.Get(ctx).Lister(),
		nil, // TODO No evictor, is this ok?
		c.SchedulerPolicy,
		c.DeSchedulerPolicy,
	)
}

// schedulerPolicyFromConfigMapOrFail reads predicates and priorities data from configMap
func schedulerPolicyFromConfigMapOrFail(ctx context.Context, configMapName string) *scheduler.SchedulerPolicy {
	p, err := schedulerPolicyFromConfigMap(ctx, configMapName)
	if err != nil {
		logging.FromContext(ctx).Fatal(zap.Error(err))
	}
	return p
}

// schedulerPolicyFromConfigMap reads predicates and priorities data from configMap
func schedulerPolicyFromConfigMap(ctx context.Context, configMapName string) (*scheduler.SchedulerPolicy, error) {
	policyConfigMap, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("couldn't get scheduler policy config map %s/%s: %v", system.Namespace(), configMapName, err)
	}

	logger := logging.FromContext(ctx).
		Desugar().
		With(zap.String("configmap", configMapName))
	policy := &scheduler.SchedulerPolicy{}

	preds, found := policyConfigMap.Data["predicates"]
	if !found {
		return nil, fmt.Errorf("missing policy config map %s/%s value at key predicates", system.Namespace(), configMapName)
	}
	if err := json.NewDecoder(strings.NewReader(preds)).Decode(&policy.Predicates); err != nil {
		return nil, fmt.Errorf("invalid policy %v: %v", preds, err)
	}

	priors, found := policyConfigMap.Data["priorities"]
	if !found {
		return nil, fmt.Errorf("missing policy config map value at key priorities")
	}
	if err := json.NewDecoder(strings.NewReader(priors)).Decode(&policy.Priorities); err != nil {
		return nil, fmt.Errorf("invalid policy %v: %v", preds, err)
	}

	if errs := validatePolicy(policy); errs != nil {
		return nil, multierr.Combine(err)
	}

	logger.Info("Schedulers policy registration", zap.Any("policy", policy))

	return policy, nil
}

func validatePolicy(policy *scheduler.SchedulerPolicy) []error {
	var validationErrors []error

	for _, priority := range policy.Priorities {
		if priority.Weight < scheduler.MinWeight || priority.Weight > scheduler.MaxWeight {
			validationErrors = append(validationErrors, fmt.Errorf("priority %s should have a positive weight applied to it or it has overflown", priority.Name))
		}
	}
	return validationErrors
}

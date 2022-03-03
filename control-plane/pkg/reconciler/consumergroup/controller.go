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
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/scheduler"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset"
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
		RefreshPeriod:       100 * time.Second,
		Capacity:            20,
		SchedulerPolicyType: "", // old non-HA scheduler
		SchedulerPolicy:     schedulerPolicyFromConfigMapOrFail(ctx, ConfigKafkaSchedulerName),
		DeSchedulerPolicy:   schedulerPolicyFromConfigMapOrFail(ctx, ConfigKafkaDeSchedulerName),
	}

	schedulers := map[string]scheduler.Scheduler{
		KafkaSourceScheduler:  createKafkaScheduler(ctx, c, kafkainternals.SourceStatefulSetName),
		KafkaTriggerScheduler: createKafkaScheduler(ctx, c, kafkainternals.BrokerStatefulSetName),
		KafkaChannelScheduler: createKafkaScheduler(ctx, c, kafkainternals.ChannelStatefulSetName),
	}

	r := &Reconciler{
		SchedulerFunc:   func(s string) scheduler.Scheduler { return schedulers[strings.ToLower(s)] },
		ConsumerLister:  consumer.Get(ctx).Lister(),
		InternalsClient: internalsclient.Get(ctx).InternalV1alpha1(),
		NameGenerator:   names.SimpleNameGenerator,
		SystemNamespace: system.Namespace(),
	}

	impl := cgreconciler.NewImpl(ctx, r)
	consumerInformer := consumer.Get(ctx)

	consumerGroupInformer := consumergroup.Get(ctx)

	consumerGroupInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	consumerInformer.Informer().AddEventHandler(controller.HandleAll(enqueueConsumerGroupFromConsumer(impl.EnqueueKey)))

	globalResync := func(interface{}) {
		impl.GlobalResync(consumerGroupInformer.Informer())
	}

	ResyncOnStatefulSetChange(ctx, globalResync)

	return impl
}

func ResyncOnStatefulSetChange(ctx context.Context, handle func(interface{})) {
	systemNamespace := system.Namespace()

	statefulset.Get(ctx).Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			ss := obj.(*appsv1.StatefulSet)
			return ss.GetNamespace() == systemNamespace && kafkainternals.IsKnownStatefulSet(ss.GetName())
		},
		Handler: controller.HandleAll(handle),
	})
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

func createKafkaScheduler(ctx context.Context, c SchedulerConfig, ssName string) scheduler.Scheduler {
	lister := consumergroup.Get(ctx).Lister()
	return createStatefulSetScheduler(
		ctx,
		SchedulerConfig{
			StatefulSetName:     ssName,
			RefreshPeriod:       c.RefreshPeriod,
			Capacity:            c.Capacity,
			SchedulerPolicyType: c.SchedulerPolicyType,
			SchedulerPolicy:     c.SchedulerPolicy,
			DeSchedulerPolicy:   c.DeSchedulerPolicy,
		},
		func() ([]scheduler.VPod, error) {
			consumerGroups, err := lister.List(labels.SelectorFromSet(getSelectorLabel(ssName)))
			if err != nil {
				return nil, err
			}
			vpods := make([]scheduler.VPod, len(consumerGroups))
			for i := 0; i < len(consumerGroups); i++ {
				vpods[i] = consumerGroups[i]
			}
			return vpods, nil
		},
	)
}

func getSelectorLabel(ssName string) map[string]string {
	//TODO remove hardcoded kinds
	var selectorLabel map[string]string
	switch ssName {
	case kafkainternals.SourceStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "kafkasource",
		}
	case kafkainternals.BrokerStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "trigger",
		}
	case kafkainternals.ChannelStatefulSetName:
		selectorLabel = map[string]string{
			kafkainternals.UserFacingResourceLabelSelector: "kafkachannel",
		}
	}
	return selectorLabel
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
		nodeinformer.Get(ctx).Lister(),
		newEvictor(ctx, zap.String("kafka.eventing.knative.dev/component", "evictor")).evict,
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

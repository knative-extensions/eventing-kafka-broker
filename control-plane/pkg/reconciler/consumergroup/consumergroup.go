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

package consumergroup

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/IBM/sarama"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/pointer"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/metrics"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"

	"knative.dev/eventing/pkg/scheduler"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda"
	internalv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	kafkainternalslisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
	kedaclientset "knative.dev/eventing-kafka-broker/third_party/pkg/client/clientset/versioned"
)

var (
	ErrNoSubscriberURI     = errors.New("no subscriber URI resolved")
	ErrNoDeadLetterSinkURI = errors.New("no dead letter sink URI resolved")

	scheduleLatencyStat = stats.Int64("schedule_latency", "Latency of consumer group schedule operations", stats.UnitMilliseconds)
	// scheduleDistribution defines the bucket boundaries for the histogram of schedule latency metric.
	// Bucket boundaries are 10ms, 100ms, 1s, 10s, 30s and 60s.
	scheduleDistribution = view.Distribution(10, 100, 1000, 10000, 30000, 60000)

	initializeOffsetsLatencyStat = stats.Int64("initialize_offsets_latency", "Latency of consumer group offsets initialization operations", stats.UnitMilliseconds)
	// initializeOffsetsDistribution defines the bucket boundaries for the histogram of initialize offsets latency metric.
	// Bucket boundaries are 10ms, 100ms, 1s, 10s, 30s and 60s.
	initializeOffsetsDistribution = view.Distribution(10, 100, 1000, 10000, 30000, 60000)

	expectedReplicasNum   = stats.Int64("consumer_group_expected_replicas", "Number of expected consumer group replicas", stats.UnitDimensionless)
	expectedReplicasGauge = view.LastValue()

	readyReplicasNum   = stats.Int64("consumer_group_ready_replicas", "Number of ready consumer group replicas", stats.UnitDimensionless)
	readyReplicasGauge = view.LastValue()
)

var (
	ConsumerNameTagKey = tag.MustNewKey("consumer_name")
	ConsumerKindTagKey = tag.MustNewKey("consumer_kind")
)

func init() {
	views := []*view.View{
		{
			Description: "Latency of consumer group schedule operations",
			TagKeys:     []tag.Key{controller.NamespaceTagKey, ConsumerNameTagKey, ConsumerKindTagKey},
			Measure:     scheduleLatencyStat,
			Aggregation: scheduleDistribution,
		},
		{
			Description: "Latency of consumer group offsets initialization operations",
			TagKeys:     []tag.Key{controller.NamespaceTagKey, ConsumerNameTagKey, ConsumerKindTagKey},
			Measure:     initializeOffsetsLatencyStat,
			Aggregation: initializeOffsetsDistribution,
		},
		{
			Description: "Number of expected consumer group replicas",
			TagKeys:     []tag.Key{controller.NamespaceTagKey, ConsumerNameTagKey, ConsumerKindTagKey},
			Measure:     expectedReplicasNum,
			Aggregation: expectedReplicasGauge,
		},
		{
			Description: "Number of expected consumer group replicas",
			TagKeys:     []tag.Key{controller.NamespaceTagKey, ConsumerNameTagKey, ConsumerKindTagKey},
			Measure:     readyReplicasNum,
			Aggregation: readyReplicasGauge,
		},
	}
	if err := view.Register(views...); err != nil {
		panic(err)
	}
}

type NoSchedulerFoundError struct{}

func (NoSchedulerFoundError) Error() string {
	return "no scheduler found"
}

var _ error = NoSchedulerFoundError{}

type Scheduler struct {
	scheduler.Scheduler
	SchedulerConfig
}

type schedulerFunc func(s string) (Scheduler, bool)

type Reconciler struct {
	SchedulerFunc   schedulerFunc
	ConsumerLister  kafkainternalslisters.ConsumerLister
	InternalsClient internalv1alpha1.InternalV1alpha1Interface
	SecretLister    corelisters.SecretLister
	ConfigMapLister corelisters.ConfigMapLister
	PodLister       corelisters.PodLister
	KubeClient      kubernetes.Interface
	Resolver        *resolver.URIResolver

	NameGenerator names.NameGenerator

	// GetKafkaClient creates new sarama Client. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClient clientpool.GetKafkaClientFunc

	// InitOffsetsFunc initialize offsets for a provided set of topics and a provided consumer group id.
	// It's convenient to add this as Reconciler field so that we can mock the function used during the
	// reconciliation loop.
	InitOffsetsFunc kafka.InitOffsetsFunc

	SystemNamespace string
	// GetKafkaClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClusterAdmin clientpool.GetKafkaClusterAdminFunc

	KafkaFeatureFlags *config.KafkaFeatureFlags
	KedaClient        kedaclientset.Interface
	AutoscalerConfig  string

	// DeleteConsumerGroupMetadataCounter is an in-memory counter to count how many times we have
	// tried to delete consumer group metadata from Kafka.
	DeleteConsumerGroupMetadataCounter *counter.Counter

	// InitOffsetLatestInitialOffsetCache is the cache for consumer group offset initialization.
	//
	// When there is high load and multiple consumer group schedule calls, we get many
	// `dial tcp 10.130.4.8:9092: i/o timeout` errors when trying to connect to Kafka.
	// This leads to increased "time to readiness" for consumer groups.
	InitOffsetLatestInitialOffsetCache prober.Cache[string, prober.Status, struct{}]

	EnqueueKey func(key string)
}

func (r *Reconciler) ReconcileKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debugw("Reconciling consumergroup")

	recordExpectedReplicasMetric(ctx, cg)

	r.reconcileStatusSelector(cg)

	logger.Debugw("Reconciling initial offset")
	if err := r.reconcileInitialOffset(ctx, cg); err != nil {
		return cg.MarkInitializeOffsetFailed("InitializeOffset", err)
	}

	logger.Debugw("Scheduling consumergroup")
	if err := r.schedule(ctx, cg); err != nil {
		return err
	}
	cg.MarkScheduleSucceeded()
	logger.Debugw("Scheduling completed", zap.Any("placement", cg.Status.PlaceableStatus))

	if keda.IsEnabled(ctx, r.KafkaFeatureFlags, r.KedaClient, cg) {
		logger.Debugw("Reconciling KEDA objects")
		if err := r.reconcileKedaObjects(ctx, cg); err != nil {
			return cg.MarkAutoscalerFailed("AutoscalerFailed", err)
		}
		cg.MarkAutoscalerSucceeded()
	} else {
		// If KEDA is not installed or autoscaler feature disabled, do nothing
		cg.MarkAutoscalerDisabled()
		if err := r.deleteKedaObjects(ctx, cg); err != nil {
			return err
		}
	}

	logger.Debugw("Reconciling consumers")
	if err := r.reconcileConsumers(ctx, cg); err != nil {
		return err
	}

	logger.Debugw("Propagating consumers status")
	errCondition, err := r.propagateStatus(ctx, cg)
	if err != nil {
		return cg.MarkReconcileConsumersFailed("PropagateConsumerStatus", err)
	}
	if errCondition != nil {
		return cg.MarkReconcileConsumersFailedCondition(errCondition)
	}

	if *cg.Spec.Replicas != 0 {
		if cg.Status.SubscriberURI == nil {
			_ = cg.MarkReconcileConsumersFailed("PropagateSubscriberURI", ErrNoSubscriberURI)
			return nil
		}
		if cg.HasDeadLetterSink() && cg.Status.DeadLetterSinkURI == nil {
			_ = cg.MarkReconcileConsumersFailed("PropagateDeadLetterSinkURI", ErrNoDeadLetterSinkURI)
			return nil
		}
	}
	cg.MarkReconcileConsumersSucceeded()

	logger.Debugw("Reconciliation succeeded")

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debugw("Reconciling consumergroup (finalization)")

	logger.Debugw("Scheduling consumergroup")
	cg.Spec.Replicas = pointer.Int32(0)
	err := r.schedule(ctx, cg) //de-schedule placements
	if err != nil && !errors.Is(err, NoSchedulerFoundError{}) {
		// return an error to 1. update the status. 2. not clear the finalizer
		return cg.MarkScheduleConsumerFailed("Deschedule", fmt.Errorf("failed to unschedule consumer group: %w", err))
	}
	logger.Debugw("Scheduling completed", zap.Any("placement", cg.Status.PlaceableStatus))

	// Get consumers associated with the ConsumerGroup.
	existingConsumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return cg.MarkReconcileConsumersFailed("ListConsumers", err)
	}

	logger.Debugw("Removing consumers", zap.Any("consumers", existingConsumers))
	for _, c := range existingConsumers {
		if err := r.finalizeConsumer(ctx, c); err != nil {
			return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
		}
	}

	logger.Debugw("Deleteing consumergroup metadata from Kafka cluster")
	if err := r.deleteConsumerGroupMetadata(ctx, cg); err != nil {
		// We retry a few times to delete Consumer group metadata from Kafka before giving up.
		if v := r.DeleteConsumerGroupMetadataCounter.Inc(string(cg.GetUID())); v <= 5 {
			return cg.MarkDeleteOffsetFailed("DeleteConsumerGroupOffset", fmt.Errorf("%w (retry num %d)", err, v))
		}
		r.DeleteConsumerGroupMetadataCounter.Del(string(cg.GetUID()))
	}

	r.InitOffsetLatestInitialOffsetCache.Expire(keyOf(cg))

	logger.Debugw("Reconciliation succeeded (finalization)")

	return nil
}

func (r *Reconciler) reconcileStatusSelector(cg *kafkainternals.ConsumerGroup) {
	cg.Status.Selector = labels.SelectorFromValidatedSet(cg.Spec.Selector).String()
}

func (r *Reconciler) deleteConsumerGroupMetadata(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	kafakSecret, err := r.newAuthSecret(ctx, cg)
	if err != nil {
		return fmt.Errorf("failed to get secret for Kafka cluster auth: %w", err)
	}

	bootstrapServers := kafka.BootstrapServersArray(cg.Spec.Template.Spec.Configs.Configs["bootstrap.servers"])

	kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, bootstrapServers, kafakSecret)
	if err != nil {
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	groupId := cg.Spec.Template.Spec.Configs.Configs["group.id"]
	if err = kafkaClusterAdminClient.DeleteConsumerGroup(groupId); err != nil && !errorIsOneOf(err, sarama.ErrUnknownTopicOrPartition, sarama.ErrGroupIDNotFound) {
		return fmt.Errorf("unable to delete the consumer group %s: %w", groupId, err)
	}

	logging.FromContext(ctx).Debug("consumer group deleted", zap.String("id", groupId))
	return nil
}

func (r *Reconciler) reconcileConsumers(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {

	// Get consumers associated with the ConsumerGroup.
	existingConsumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return cg.MarkReconcileConsumersFailed("ListConsumers", err)
	}

	placementConsumers := r.joinConsumersByPlacement(cg.Status.Placements, existingConsumers)

	for _, pc := range placementConsumers {
		if pc.Placement == nil {
			// There is no placement for pc.Consumers, so they need to be finalized.
			for _, c := range pc.Consumers {
				if err := r.finalizeConsumer(ctx, c); err != nil {
					return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
				}
			}
			continue
		}

		if err := r.reconcileConsumersInPlacement(ctx, cg, pc); err != nil {
			return cg.MarkReconcileConsumersFailed("ReconcileConsumer", err)
		}
	}

	return nil
}

func (r *Reconciler) reconcileConsumersInPlacement(ctx context.Context, cg *kafkainternals.ConsumerGroup, pc ConsumersPerPlacement) error {

	placement := *pc.Placement
	consumers := pc.Consumers

	// Check if there is a consumer for the given placement.
	if len(consumers) == 0 {
		return r.createConsumer(ctx, cg, placement)
	}

	// Stable sort consumers so that we give consumers different deletion
	// priorities based on their state (readiness, etc).
	//
	// Consumers at the tail of the list are deleted.
	sort.Stable(kafkainternals.ByReadinessAndCreationTime(consumers))

	for _, c := range consumers[1:] {
		if err := r.finalizeConsumer(ctx, c); err != nil {
			return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
		}
	}

	c := consumers[0]

	expectedSpec := kafkainternals.ConsumerSpec{}
	cg.Spec.Template.Spec.DeepCopyInto(&expectedSpec)

	expectedSpec.VReplicas = pointer.Int32(placement.VReplicas)

	expectedSpec.OIDCServiceAccountName = cg.Spec.OIDCServiceAccountName

	if equality.Semantic.DeepDerivative(expectedSpec, c.Spec) {
		// Consumer is equal to the template.
		return nil
	}

	expectedSpec.PodBind = &kafkainternals.PodBind{
		PodName:      placement.PodName,
		PodNamespace: r.SystemNamespace,
	}

	// Do not modify informer copy.
	expectedC := &kafkainternals.Consumer{
		TypeMeta:   c.TypeMeta,
		ObjectMeta: c.ObjectMeta,
		Spec:       expectedSpec,
		Status:     c.Status,
	}

	// Update existing Consumer.
	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Update(ctx, expectedC, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
	}

	return nil
}

func (r *Reconciler) createConsumer(ctx context.Context, cg *kafkainternals.ConsumerGroup, placement eventingduckv1alpha1.Placement) error {
	c := cg.ConsumerFromTemplate()

	c.Name = r.NameGenerator.GenerateName(cg.GetName() + "-")
	c.Spec.VReplicas = pointer.Int32(placement.VReplicas)
	c.Spec.PodBind = &kafkainternals.PodBind{PodName: placement.PodName, PodNamespace: r.SystemNamespace}

	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Create(ctx, c, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
	}
	return nil
}

func (r *Reconciler) finalizeConsumer(ctx context.Context, consumer *kafkainternals.Consumer) error {
	dOpts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &consumer.UID},
	}
	err := r.InternalsClient.Consumers(consumer.GetNamespace()).Delete(ctx, consumer.GetName(), dOpts)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to remove consumer %s/%s: %w", consumer.GetNamespace(), consumer.GetName(), err)
	}
	return nil
}

func (r *Reconciler) schedule(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	startTime := time.Now()
	defer recordScheduleLatency(ctx, cg, startTime)

	resourceRef := cg.GetUserFacingResourceRef()
	if resourceRef == nil {
		return NoSchedulerFoundError{}
	}

	statefulSetScheduler, ok := r.SchedulerFunc(resourceRef.Kind)
	if !ok {
		return NoSchedulerFoundError{}
	}

	// Ensure Contract configmaps are created before scheduling to avoid having pending pods due to missing
	// volumes.
	// See https://github.com/knative-extensions/eventing-kafka-broker/issues/2750#issuecomment-1304244017
	if err := r.ensureContractConfigmapsExist(ctx, statefulSetScheduler); err != nil {
		return cg.MarkScheduleConsumerFailed("Schedule", err)
	}

	placements, err := statefulSetScheduler.Schedule(cg)
	if err != nil {
		return cg.MarkScheduleConsumerFailed("Schedule", err)
	}
	// Sort placements by pod name.
	sort.SliceStable(placements, func(i, j int) bool {
		return placements[i].PodName < placements[j].PodName
	})

	cg.Status.Placements = placements

	return nil
}

type ConsumersPerPlacement struct {
	Placement *eventingduckv1alpha1.Placement
	Consumers []*kafkainternals.Consumer
}

func (r *Reconciler) joinConsumersByPlacement(placements []eventingduckv1alpha1.Placement, consumers []*kafkainternals.Consumer) []ConsumersPerPlacement {
	placementConsumers := make([]ConsumersPerPlacement, 0, int(math.Max(float64(len(placements)), float64(len(consumers)))))

	// Group consumers by Pod bind.
	consumersByPod := make(map[kafkainternals.PodBind][]*kafkainternals.Consumer, len(consumers))
	for i := range consumers {
		consumersByPod[*consumers[i].Spec.PodBind] = append(consumersByPod[*consumers[i].Spec.PodBind], consumers[i])
	}

	// Group placements by Pod bind.
	placementsByPod := make(map[kafkainternals.PodBind]eventingduckv1alpha1.Placement, len(placements))
	for i := range placements {
		pb := kafkainternals.PodBind{
			PodName:      placements[i].PodName,
			PodNamespace: r.SystemNamespace,
		}

		v := placementsByPod[pb]
		placementsByPod[pb] = eventingduckv1alpha1.Placement{
			PodName:   pb.PodName,
			VReplicas: v.VReplicas + placements[i].VReplicas,
		}
	}

	for k := range placementsByPod {
		if _, ok := consumersByPod[k]; !ok {
			consumersByPod[k] = nil
		}
	}

	for pb := range consumersByPod {

		var p *eventingduckv1alpha1.Placement
		if v, ok := placementsByPod[pb]; ok {
			p = &v
		}

		c := consumersByPod[pb]

		placementConsumers = append(placementConsumers, ConsumersPerPlacement{Placement: p, Consumers: c})
	}

	sort.Slice(placementConsumers, func(i, j int) bool {
		if placementConsumers[i].Placement == nil {
			return true
		}
		if placementConsumers[j].Placement == nil {
			return false
		}
		return placementConsumers[i].Placement.PodName < placementConsumers[j].Placement.PodName
	})

	return placementConsumers
}

func (r *Reconciler) propagateStatus(ctx context.Context, cg *kafkainternals.ConsumerGroup) (*apis.Condition, error) {
	consumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return nil, fmt.Errorf("failed to list consumers for selector %+v: %w", cg.Spec.Selector, err)
	}
	count := int32(0)
	cg.Status.Replicas = pointer.Int32(count)
	var condition *apis.Condition

	for _, c := range consumers {
		if c.IsReady() {
			if c.Spec.VReplicas != nil {
				count += *c.Spec.VReplicas
			}
			if c.Status.SubscriberURI != nil {
				cg.Status.SubscriberURI = c.Status.SubscriberURI
			}
			if c.Status.SubscriberCACerts != nil {
				cg.Status.SubscriberCACerts = c.Status.SubscriberCACerts
			}
			if c.Status.SubscriberAudience != nil {
				cg.Status.SubscriberAudience = c.Status.SubscriberAudience
			}
			if c.Status.DeliveryStatus.DeadLetterSinkURI != nil {
				cg.Status.DeliveryStatus.DeadLetterSinkURI = c.Status.DeadLetterSinkURI
			}
			if c.Status.DeliveryStatus.DeadLetterSinkCACerts != nil {
				cg.Status.DeliveryStatus.DeadLetterSinkCACerts = c.Status.DeadLetterSinkCACerts
			}
			if c.Status.DeliveryStatus.DeadLetterSinkAudience != nil {
				cg.Status.DeliveryStatus.DeadLetterSinkAudience = c.Status.DeadLetterSinkAudience
			}
		} else if condition == nil { // Propagate only a single false condition
			cond := c.GetConditionSet().Manage(c.GetStatus()).GetTopLevelCondition()
			if cond.IsFalse() {
				condition = cond
			}
		}
	}
	cg.Status.Replicas = pointer.Int32(count)

	recordReadyReplicasMetric(ctx, cg)

	if cg.Spec.Replicas != nil && *cg.Spec.Replicas == 0 {
		subscriber, err := r.Resolver.AddressableFromDestinationV1(ctx, cg.Spec.Template.Spec.Subscriber, cg)
		if err != nil {
			return condition, fmt.Errorf("failed to resolve subscribed URI: %w", err)
		}
		cg.Status.SubscriberURI = subscriber.URL
		cg.Status.SubscriberCACerts = subscriber.CACerts
		cg.Status.SubscriberAudience = subscriber.Audience
	}

	return condition, nil
}

func (r *Reconciler) reconcileInitialOffset(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	startTime := time.Now()
	defer recordInitializeOffsetsLatency(ctx, cg, startTime)

	if cg.Spec.Template.Spec.Delivery == nil || cg.Spec.Template.Spec.Delivery.InitialOffset == sources.OffsetEarliest {
		return nil
	}

	if status, ok := r.InitOffsetLatestInitialOffsetCache.Get(keyOf(cg)); ok && status == prober.StatusReady {
		return nil
	}

	kafkaSecret, err := r.newAuthSecret(ctx, cg)
	if err != nil {
		return fmt.Errorf("failed to get secret for Kafka cluster auth: %w", err)
	}

	bootstrapServers := kafka.BootstrapServersArray(cg.Spec.Template.Spec.Configs.Configs["bootstrap.servers"])

	kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, bootstrapServers, kafkaSecret)
	if err != nil {
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	kafkaClient, err := r.GetKafkaClient(ctx, bootstrapServers, kafkaSecret)
	if err != nil {
		return fmt.Errorf("failed to create Kafka cluster client: %w", err)
	}
	defer kafkaClient.Close()

	groupId := cg.Spec.Template.Spec.Configs.Configs["group.id"]
	topics := cg.Spec.Template.Spec.Topics

	if _, err := r.InitOffsetsFunc(ctx, kafkaClient, kafkaClusterAdminClient, topics, groupId); err != nil {
		return fmt.Errorf("failed to initialize offset: %w", err)
	}

	r.InitOffsetLatestInitialOffsetCache.UpsertStatus(keyOf(cg), prober.StatusReady, struct{}{}, func(key string, _ prober.Status, _ struct{}) {
		r.EnqueueKey(key)
	})

	return nil
}

func (r *Reconciler) reconcileKedaObjects(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	var triggerAuthentication *kedav1alpha1.TriggerAuthentication
	var secret *corev1.Secret

	autoscalerDefaults, err := r.autoscalerDefaultsFromConfigMap(ctx, r.AutoscalerConfig)
	if err != nil {
		return err
	}

	if hasSecretSpecConfig(cg.Spec.Template.Spec.Auth) || hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) {
		secretData, err := r.retrieveSecretData(ctx, cg)
		if err != nil {
			return err
		}

		triggerAuthentication, secret, err = keda.GenerateTriggerAuthentication(cg, secretData)
		if err != nil {
			return err
		}
	}

	triggers, err := keda.GenerateScaleTriggers(cg, triggerAuthentication, *autoscalerDefaults)
	if err != nil {
		return err
	}

	scaledObject, err := keda.GenerateScaledObject(cg, cg.GetGroupVersionKind(), keda.GenerateScaleTarget(cg), triggers, *autoscalerDefaults)
	if err != nil {
		return err
	}

	if triggerAuthentication != nil && secret != nil {
		if err = r.reconcileSecret(ctx, secret, cg); err != nil {
			return err
		}

		if err = r.reconcileTriggerAuthentication(ctx, triggerAuthentication, cg); err != nil {
			return err
		}
	}

	if err = r.reconcileScaledObject(ctx, scaledObject, cg); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) retrieveSecretData(ctx context.Context, cg *kafkainternals.ConsumerGroup) (map[string][]byte, error) {
	if hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) && cg.Spec.Template.Spec.Auth.NetSpec.SASL.Enable && cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef != nil {
		secretKeyRefName := cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef.Name
		secret, err := r.KubeClient.CoreV1().Secrets(cg.Namespace).Get(ctx, secretKeyRefName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("unable to get data from secret: \"%s/%s\", %w", cg.Namespace, secretKeyRefName, err)
		}
		return secret.Data, nil
	}

	if hasSecretSpecConfig(cg.Spec.Template.Spec.Auth) && cg.Spec.Template.Spec.Auth.SecretSpec.Ref != nil {
		secretKeyRefName := cg.Spec.Template.Spec.Auth.SecretSpec.Ref.Name
		secretKeyRefNamespace := cg.Spec.Template.Spec.Auth.SecretSpec.Ref.Namespace
		secret, err := r.KubeClient.CoreV1().Secrets(secretKeyRefNamespace).Get(ctx, secretKeyRefName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("unable to get data from secret: \"%s/%s\", %w", secretKeyRefNamespace, secretKeyRefName, err)
		}
		return secret.Data, nil
	}
	return nil, nil
}

func (r *Reconciler) reconcileScaledObject(ctx context.Context, expectedScaledObject *kedav1alpha1.ScaledObject, obj metav1.Object) error {
	scaledObject, err := r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Get(ctx, expectedScaledObject.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get scaledobject %s/%s: %w", expectedScaledObject.Namespace, expectedScaledObject.Name, err)
	}
	if apierrors.IsNotFound(err) {
		_, err = r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Create(ctx, expectedScaledObject, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create scaledobject %s/%s: %w", expectedScaledObject.Namespace, expectedScaledObject.Name, err)
		}
		return nil
	}
	if !metav1.IsControlledBy(scaledObject, obj) {
		return fmt.Errorf("scaledobject %s/%s is not owned by %s/%s", expectedScaledObject.Namespace, expectedScaledObject.Name, obj.GetNamespace(), obj.GetName())
	}
	if !equality.Semantic.DeepDerivative(scaledObject.Spec, expectedScaledObject.Spec) {
		scaledObject.Spec = expectedScaledObject.Spec
		if _, err = r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Update(ctx, scaledObject, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update scaled object %s/%s: %w", expectedScaledObject.Namespace, expectedScaledObject.Name, err)
		}
	}
	return nil
}

func (r *Reconciler) reconcileTriggerAuthentication(ctx context.Context, expectedTriggerAuth *kedav1alpha1.TriggerAuthentication, obj metav1.Object) error {
	triggerAuth, err := r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Get(ctx, expectedTriggerAuth.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get triggerauthentication %s/%s: %w", expectedTriggerAuth.Namespace, expectedTriggerAuth.Name, err)
	}
	if apierrors.IsNotFound(err) {
		_, err = r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Create(ctx, expectedTriggerAuth, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create triggerauthentication  object %s/%s: %w", expectedTriggerAuth.Namespace, expectedTriggerAuth.Name, err)
		}
		return nil
	}
	if !metav1.IsControlledBy(triggerAuth, obj) {
		return fmt.Errorf("triggerauthentication object %s/%s is not owned by %s/%s", expectedTriggerAuth.Namespace, expectedTriggerAuth.Name, obj.GetNamespace(), obj.GetName())
	}
	if !equality.Semantic.DeepDerivative(triggerAuth.Spec, expectedTriggerAuth.Spec) {
		triggerAuth.Spec = expectedTriggerAuth.Spec
		if _, err = r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Update(ctx, triggerAuth, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update triggerauthentication object %s/%s: %w", expectedTriggerAuth.Namespace, expectedTriggerAuth.Name, err)
		}
	}
	return nil
}

func (r *Reconciler) reconcileSecret(ctx context.Context, expectedSecret *corev1.Secret, obj metav1.Object) error {
	secret, err := r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Get(ctx, expectedSecret.Name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get secret %s/%s: %w", expectedSecret.Namespace, expectedSecret.Name, err)
	}
	if apierrors.IsNotFound(err) {
		_, err = r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Create(ctx, expectedSecret, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create secret %s/%s: %w", expectedSecret.Namespace, expectedSecret.Name, err)
		}
		return nil
	}
	if !metav1.IsControlledBy(secret, obj) {
		return fmt.Errorf("secret object %s/%s is not owned by %s/%s", expectedSecret.Namespace, expectedSecret.Name, obj.GetNamespace(), obj.GetName())
	}
	// StringData is not populated on read so for now always update the secret
	secret.StringData = expectedSecret.StringData
	if _, err = r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update secret object %s/%s: %w", expectedSecret.Namespace, expectedSecret.Name, err)
	}
	return nil
}

func (r *Reconciler) ensureContractConfigmapsExist(ctx context.Context, scheduler Scheduler) error {
	selector := labels.SelectorFromSet(map[string]string{
		"app":                    scheduler.StatefulSetName,
		"app.kubernetes.io/kind": "kafka-dispatcher",
	})
	pods, err := r.PodLister.
		Pods(r.SystemNamespace).
		List(selector)
	if err != nil {
		return fmt.Errorf("failed to list statefulset pods with selector %v: %w", selector.String(), err)
	}

	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	for _, p := range pods {
		cmName, err := eventing.ConfigMapNameFromPod(p)
		if err != nil {
			return err
		}
		if err := r.ensureContractConfigMapExists(ctx, p, cmName); err != nil {
			return fmt.Errorf("failed to get ConfigMap %s/%s: %w", r.SystemNamespace, cmName, err)
		}
	}

	return nil
}

func (r *Reconciler) ensureContractConfigMapExists(ctx context.Context, p *corev1.Pod, name string) error {
	// Check if ConfigMap exists in lister cache
	_, err := r.ConfigMapLister.ConfigMaps(r.SystemNamespace).Get(name)
	// ConfigMap already exists, return
	if err == nil {
		return nil
	}

	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get ConfigMap %s/%s: %w", r.SystemNamespace, name, err)
	}

	b := base.Reconciler{
		KubeClient:                    r.KubeClient,
		DataPlaneConfigMapNamespace:   r.SystemNamespace,
		ContractConfigMapName:         name,
		DataPlaneNamespace:            r.SystemNamespace,
		DataPlaneConfigMapTransformer: base.PodOwnerReference(p),
	}

	if _, err := b.GetOrCreateDataPlaneConfigMap(ctx); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create ConfigMap %s/%s: %w", r.SystemNamespace, name, err)
	}
	return nil
}

func errorIsOneOf(err error, errs ...error) bool {
	for _, e := range errs {
		if errors.Is(err, e) {
			return true
		}
	}

	return false
}

var (
	_ consumergroup.Interface = &Reconciler{}
	_ consumergroup.Finalizer = &Reconciler{}
)

func recordScheduleLatency(ctx context.Context, cg *kafkainternals.ConsumerGroup, startTime time.Time) {
	ctx, err := metricTagsOf(ctx, cg)
	if err != nil {
		return
	}
	metrics.Record(ctx, scheduleLatencyStat.M(time.Since(startTime).Milliseconds()))
}

func recordInitializeOffsetsLatency(ctx context.Context, cg *kafkainternals.ConsumerGroup, startTime time.Time) {
	ctx, err := metricTagsOf(ctx, cg)
	if err != nil {
		return
	}
	metrics.Record(ctx, initializeOffsetsLatencyStat.M(time.Since(startTime).Milliseconds()))
}

func recordExpectedReplicasMetric(ctx context.Context, cg *kafkainternals.ConsumerGroup) {
	ctx, err := metricTagsOf(ctx, cg)
	if err != nil {
		return
	}

	r := int32(0)
	if cg.Spec.Replicas != nil {
		r = *cg.Spec.Replicas
	}
	metrics.Record(ctx, expectedReplicasNum.M(int64(r)))
}

func recordReadyReplicasMetric(ctx context.Context, cg *kafkainternals.ConsumerGroup) {
	ctx, err := metricTagsOf(ctx, cg)
	if err != nil {
		return
	}

	r := int32(0)
	if cg.Status.Replicas != nil {
		r = *cg.Status.Replicas
	}
	metrics.Record(ctx, readyReplicasNum.M(int64(r)))
}

func metricTagsOf(ctx context.Context, cg *kafkainternals.ConsumerGroup) (context.Context, error) {
	uf := cg.GetUserFacingResourceRef()
	return tag.New(
		ctx,
		tag.Insert(controller.NamespaceTagKey, cg.Namespace),
		tag.Insert(ConsumerNameTagKey, uf.Name),
		tag.Insert(ConsumerKindTagKey, uf.Kind),
	)
}

func keyOf(cg metav1.Object) string {
	return types.NamespacedName{Namespace: cg.GetNamespace(), Name: cg.GetName()}.String()
}

// deleteKedaObjects is responsible for deleting Keda (Kubernetes-based Event Driven Autoscaling) objects, "scaled
// objects" and "trigger authentications" from all available namespaces.
//
// When Keda creates a scaled object, it also generates a Horizontal Pod Autoscaler. This resource will also be removed
// when the scaled object is deleted.
func (r *Reconciler) deleteKedaObjects(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {

	// get scaled object
	scaledObjectName := keda.GenerateScaledObjectName(cg)
	scaledObject, err := r.KedaClient.KedaV1alpha1().ScaledObjects(cg.Namespace).Get(ctx, scaledObjectName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get Keda scaled object: %w", err)
	}

	if scaledObject == nil {
		return nil
	}

	// if there is a trigger authentication tight to the consumer group, delete it, and then delete the scaled object
	if metav1.IsControlledBy(scaledObject, cg) {
		if len(cg.ObjectMeta.OwnerReferences) == 0 {
			return fmt.Errorf("failed to delete Keda objects, missing owners reference: %w", err)
		}
		triggerAuthName := string(cg.ObjectMeta.OwnerReferences[0].UID)

		err = r.KedaClient.KedaV1alpha1().TriggerAuthentications(cg.Namespace).Delete(ctx, triggerAuthName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Keda trigger authentication: %w", err)
		}

		logging.FromContext(ctx).Infoln("Keda trigger authentication deleted")

		// delete scaled object
		err = r.KedaClient.KedaV1alpha1().ScaledObjects(cg.Namespace).Delete(ctx, scaledObjectName, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Keda scaled object: %w", err)
		}
		if err == nil {
			logging.FromContext(ctx).Infoln("Keda scaled object deleted")
		}
	}

	return nil
}

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

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/pointer"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	"knative.dev/eventing/pkg/scheduler"

	"knative.dev/eventing-autoscaler-keda/pkg/reconciler/keda"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	internalv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumergroup"
	kafkainternalslisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"

	kedav1alpha1 "knative.dev/eventing-autoscaler-keda/third_party/pkg/apis/keda/v1alpha1"
	kedaclientset "knative.dev/eventing-autoscaler-keda/third_party/pkg/client/clientset/versioned"
	kedafunc "knative.dev/eventing-kafka-broker/control-plane/pkg/keda"
)

var (
	ErrNoSubscriberURI     = errors.New("no subscriber URI resolved")
	ErrNoDeadLetterSinkURI = errors.New("no dead letter sink URI resolved")
)

type schedulerFunc func(s string) scheduler.Scheduler

type Reconciler struct {
	SchedulerFunc   schedulerFunc
	ConsumerLister  kafkainternalslisters.ConsumerLister
	InternalsClient internalv1alpha1.InternalV1alpha1Interface
	SecretLister    corelisters.SecretLister
	KubeClient      kubernetes.Interface

	NameGenerator names.NameGenerator

	// NewKafkaClient creates new sarama Client. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClient kafka.NewClientFunc

	// InitOffsetsFunc initialize offsets for a provided set of topics and a provided consumer group id.
	// It's convenient to add this as Reconciler field so that we can mock the function used during the
	// reconciliation loop.
	InitOffsetsFunc kafka.InitOffsetsFunc

	SystemNamespace string
	// NewKafkaClusterAdminClient creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	NewKafkaClusterAdminClient kafka.NewClusterAdminClientFunc

	KafkaFeatureFlags *config.KafkaFeatureFlags
	KedaClient        kedaclientset.Interface
}

func (r Reconciler) ReconcileKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {

	if err := r.reconcileInitialOffset(ctx, cg); err != nil {
		return cg.MarkInitializeOffsetFailed("InitializeOffset", err)
	}

	if err := r.schedule(cg); err != nil {
		return err
	}
	cg.MarkScheduleSucceeded()

	if err := r.reconcileKeda(ctx, cg); err != nil {
		return cg.MarkKedaScalingFailed("KedaScaling", err)
	}
	cg.MarkKedaScalingSucceeded()

	if err := r.reconcileConsumers(ctx, cg); err != nil {
		return err
	}

	errCondition, err := r.propagateStatus(cg)
	if err != nil {
		return cg.MarkReconcileConsumersFailed("PropagateConsumerStatus", err)
	}
	if errCondition != nil {
		return cg.MarkReconcileConsumersFailedCondition(errCondition)
	}

	if *cg.Status.Replicas != 0 {
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

	return nil
}

func (r Reconciler) FinalizeKind(ctx context.Context, cg *kafkainternals.ConsumerGroup) reconciler.Event {

	cg.Spec.Replicas = pointer.Int32Ptr(0)
	err := r.schedule(cg) //de-schedule placements

	if err != nil {
		cg.Status.Placements = nil

		// return an error to 1. update the status. 2. not clear the finalizer
		return errors.New("placement list was not empty")
	}

	// Get consumers associated with the ConsumerGroup.
	existingConsumers, err := r.ConsumerLister.Consumers(cg.GetNamespace()).List(labels.SelectorFromSet(cg.Spec.Selector))
	if err != nil {
		return cg.MarkReconcileConsumersFailed("ListConsumers", err)
	}

	for _, c := range existingConsumers {
		if err := r.finalizeConsumer(ctx, c); err != nil {
			return cg.MarkReconcileConsumersFailed("FinalizeConsumer", err)
		}
	}

	options, err := r.newAuthConfigOption(ctx, cg)
	if err != nil {
		return fmt.Errorf("failed to create config options for Kafka cluster auth: %w", err)
	}

	saramaConfig, err := kafka.GetSaramaConfig(options)
	if err != nil {
		return fmt.Errorf("failed to get cluster admin sarama config: %w", err)
	}
	bootstrapServers := kafka.BootstrapServersArray(cg.Spec.Template.Spec.Configs.Configs["bootstrap.servers"])

	kafkaClusterAdminClient, err := r.NewKafkaClusterAdminClient(bootstrapServers, saramaConfig)
	if err != nil {
		logging.FromContext(ctx).Errorw("unable to create a kafka client", zap.Error(err))
		return err
	}
	defer kafkaClusterAdminClient.Close()

	groupId := cg.Spec.Template.Spec.Configs.Configs["group.id"]
	if err := kafkaClusterAdminClient.DeleteConsumerGroup(groupId); err != nil && !errors.Is(sarama.ErrGroupIDNotFound, err) {
		logging.FromContext(ctx).Errorw("unable to delete the consumer group", zap.String("id", groupId), zap.Error(err))
		return err
	}

	logging.FromContext(ctx).Infow("consumer group deleted", zap.String("id", groupId))
	return nil
}

func (r Reconciler) reconcileConsumers(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {

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

func (r Reconciler) reconcileConsumersInPlacement(ctx context.Context, cg *kafkainternals.ConsumerGroup, pc ConsumersPerPlacement) error {

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

	expectedSpec.VReplicas = pointer.Int32Ptr(placement.VReplicas)

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

func (r Reconciler) createConsumer(ctx context.Context, cg *kafkainternals.ConsumerGroup, placement eventingduckv1alpha1.Placement) error {
	c := cg.ConsumerFromTemplate()

	c.Name = r.NameGenerator.GenerateName(cg.GetName() + "-")
	c.Spec.VReplicas = pointer.Int32Ptr(placement.VReplicas)
	c.Spec.PodBind = &kafkainternals.PodBind{PodName: placement.PodName, PodNamespace: r.SystemNamespace}

	if _, err := r.InternalsClient.Consumers(cg.GetNamespace()).Create(ctx, c, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create consumer %s/%s: %w", c.GetNamespace(), c.GetName(), err)
	}
	return nil
}

func (r Reconciler) finalizeConsumer(ctx context.Context, consumer *kafkainternals.Consumer) error {
	dOpts := metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &consumer.UID},
	}
	err := r.InternalsClient.Consumers(consumer.GetNamespace()).Delete(ctx, consumer.GetName(), dOpts)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to remove consumer %s/%s: %w", consumer.GetNamespace(), consumer.GetName(), err)
	}
	return nil
}

func (r Reconciler) schedule(cg *kafkainternals.ConsumerGroup) error {
	placements, err := r.SchedulerFunc(cg.GetUserFacingResourceRef().Kind).Schedule(cg)
	if err != nil {
		return cg.MarkScheduleConsumerFailed("Schedule", err)
	}
	// Sort placements by pod name.
	sort.SliceStable(placements, func(i, j int) bool { return placements[i].PodName < placements[j].PodName })

	cg.Status.Placements = placements

	return nil
}

type ConsumersPerPlacement struct {
	Placement *eventingduckv1alpha1.Placement
	Consumers []*kafkainternals.Consumer
}

func (r Reconciler) joinConsumersByPlacement(placements []eventingduckv1alpha1.Placement, consumers []*kafkainternals.Consumer) []ConsumersPerPlacement {
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

func (r Reconciler) propagateStatus(cg *kafkainternals.ConsumerGroup) (*apis.Condition, error) {
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
			if c.Status.DeliveryStatus.DeadLetterSinkURI != nil {
				cg.Status.DeliveryStatus.DeadLetterSinkURI = c.Status.DeadLetterSinkURI
			}
		} else if condition == nil { // Propagate only a single false condition
			cond := c.GetConditionSet().Manage(c.GetStatus()).GetTopLevelCondition()
			if cond.IsFalse() {
				condition = cond
			}
		}
	}
	cg.Status.Replicas = pointer.Int32(count)

	return condition, nil
}

func (r Reconciler) reconcileInitialOffset(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	if cg.Spec.Template.Spec.Delivery == nil || cg.Spec.Template.Spec.Delivery.InitialOffset == sources.OffsetEarliest {
		return nil
	}

	options, err := r.newAuthConfigOption(ctx, cg)
	if err != nil {
		return fmt.Errorf("failed to create config options for Kafka cluster auth: %w", err)
	}

	saramaConfig, err := kafka.GetSaramaConfig(options)
	if err != nil {
		return fmt.Errorf("failed to create Admin client config: %w", err)
	}
	bootstrapServers := kafka.BootstrapServersArray(cg.Spec.Template.Spec.Configs.Configs["bootstrap.servers"])

	kafkaClient, err := r.NewKafkaClient(bootstrapServers, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kafka cluster client: %w", err)
	}
	defer kafkaClient.Close()

	kafkaAdminClient, err := sarama.NewClusterAdminFromClient(kafkaClient)
	if err != nil {
		return fmt.Errorf("failed to create Kafka cluster admin client: %w", err)
	}
	defer kafkaAdminClient.Close()

	groupId := cg.Spec.Template.Spec.Configs.Configs["group.id"]
	topics := cg.Spec.Template.Spec.Topics

	if _, err := r.InitOffsetsFunc(ctx, kafkaClient, kafkaAdminClient, topics, groupId); err != nil {
		return fmt.Errorf("failed to initialize offset: %w", err)
	}

	return nil
}

func (r Reconciler) reconcileKeda(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	// If KEDA is not installed, do nothing
	if !r.isKEDAEnabled(ctx, cg.GetNamespace()) {
		logging.FromContext(ctx).Errorw("KEDA not installed, failed to list ScaledObjects")
		return nil
	}

	if hasAuthSpecAuthConfig(cg.Spec.Template.Spec.Auth) || hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) {
		return r.reconcileKedaObjects(ctx, cg)
	}
	return nil
}

func (r Reconciler) reconcileKedaObjects(ctx context.Context, cg *kafkainternals.ConsumerGroup) error {
	var triggerAuthentication *kedav1alpha1.TriggerAuthentication
	var secret *corev1.Secret
	saslType, err := r.retrieveSaslTypeIfPresent(ctx, cg)
	if err != nil {
		return err
	}

	triggerAuthentication, secret, err = kedafunc.GenerateTriggerAuthentication(cg, saslType)
	if err != nil {
		return err
	}

	triggers, err := kedafunc.GenerateScaleTriggers(cg, triggerAuthentication)
	if err != nil {
		return err
	}
	scaledObject, err := keda.GenerateScaledObject(cg, cg.GetGroupVersionKind(), kedafunc.GenerateScaleTarget(cg), triggers)
	if err != nil {
		return err
	}

	if triggerAuthentication != nil && secret != nil {
		err = r.reconcileSecret(ctx, secret, cg)

		// if the event was wrapped inside an error, consider the reconciliation as failed
		if _, isEvent := err.(*reconciler.ReconcilerEvent); !isEvent {
			return err
		}

		err = r.reconcileTriggerAuthentication(ctx, triggerAuthentication, cg)

		// if the event was wrapped inside an error, consider the reconciliation as failed
		if _, isEvent := err.(*reconciler.ReconcilerEvent); !isEvent {
			return err
		}
	}

	err = r.reconcileScaledObject(ctx, scaledObject, cg)

	// if the event was wrapped inside an error, consider the reconciliation as failed
	if _, isEvent := err.(*reconciler.ReconcilerEvent); !isEvent {
		return err
	}

	return nil
}

func (r *Reconciler) retrieveSaslTypeIfPresent(ctx context.Context, cg *kafkainternals.ConsumerGroup) (*string, error) {
	if hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) && cg.Spec.Template.Spec.Auth.NetSpec.SASL.Enable {
		if cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef != nil {
			secretKeyRefName := cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef.Name
			secretKeyRefKey := cg.Spec.Template.Spec.Auth.NetSpec.SASL.Type.SecretKeyRef.Key
			secret, err := r.KubeClient.CoreV1().Secrets(cg.Namespace).Get(ctx, secretKeyRefName, metav1.GetOptions{})
			if err != nil {
				return nil, reconciler.NewEvent(corev1.EventTypeWarning, "SaslTypeSecretUnavailable", "Unable to get SASL type from secret: \"%s/%s\", %w", cg.Namespace, secretKeyRefName, err)
			}
			saslTypeValue := string(secret.Data[secretKeyRefKey])
			return &saslTypeValue, nil
		}
	}
	return nil, nil
}

func (r *Reconciler) reconcileScaledObject(ctx context.Context, expectedScaledObject *kedav1alpha1.ScaledObject, obj metav1.Object) error {
	scaledObject, err := r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Get(ctx, expectedScaledObject.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		scaledObject, err = r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Create(ctx, expectedScaledObject, metav1.CreateOptions{})
		if err != nil {
			return kedafunc.ScaleObjectFailed(expectedScaledObject.Namespace, expectedScaledObject.Name, err)
		}
		return kedafunc.ScaleObjectCreated(scaledObject.Namespace, scaledObject.Name)
	} else if err != nil {
		logging.FromContext(ctx).Errorw("Unable to get an existing ScaledObject", zap.Error(err))
		return err
	} else if !metav1.IsControlledBy(scaledObject, obj) {
		return fmt.Errorf("ScaledObject %q is not owned by %q", scaledObject.Name, obj)
	} else if !equality.Semantic.DeepDerivative(scaledObject.Spec, expectedScaledObject.Spec) {
		logging.FromContext(ctx).Debug(fmt.Sprintf("ScaledObject changed, found: %#v expected: %#v", scaledObject.Spec, expectedScaledObject.Spec))
		scaledObject.Spec = expectedScaledObject.Spec
		if _, err = r.KedaClient.KedaV1alpha1().ScaledObjects(expectedScaledObject.Namespace).Update(ctx, scaledObject, metav1.UpdateOptions{}); err != nil {
			return err
		}
		return kedafunc.ScaleObjectUpdated(scaledObject.Namespace, scaledObject.Name)
	} else {
		logging.FromContext(ctx).Debugw("Reusing existing ScaledObject", zap.Any("ScaledObject", scaledObject))
	}

	return nil
}

func (r *Reconciler) reconcileTriggerAuthentication(ctx context.Context, expectedTriggerAuth *kedav1alpha1.TriggerAuthentication, obj metav1.Object) error {
	triggerAuth, err := r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Get(ctx, expectedTriggerAuth.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		triggerAuth, err = r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Create(ctx, expectedTriggerAuth, metav1.CreateOptions{})
		if err != nil {
			return reconciler.NewEvent(corev1.EventTypeWarning, "TriggerAuthenticationFailed", "TriggerAuthentication deployment failed to: \"%s/%s\", %w",
				expectedTriggerAuth.Namespace, expectedTriggerAuth.Name, err)
		}
		return reconciler.NewEvent(corev1.EventTypeNormal, "TriggerAuthenticationCreated", "TriggerAuthentication created: \"%s/%s\"",
			triggerAuth.Namespace, triggerAuth.Name)
	} else if err != nil {
		logging.FromContext(ctx).Errorw("Unable to get an existing ScaledObject", zap.Error(err))
		return err
	} else if !metav1.IsControlledBy(triggerAuth, obj) {
		return fmt.Errorf("ScaledObject %q is not owned by %q", triggerAuth.Name, obj)
	} else if !equality.Semantic.DeepDerivative(triggerAuth.Spec, expectedTriggerAuth.Spec) {
		logging.FromContext(ctx).Debug(fmt.Sprintf("TriggerAuthentication changed, found: %#v expected: %#v", triggerAuth.Spec, expectedTriggerAuth.Spec))
		triggerAuth.Spec = expectedTriggerAuth.Spec
		if _, err = r.KedaClient.KedaV1alpha1().TriggerAuthentications(expectedTriggerAuth.Namespace).Update(ctx, triggerAuth, metav1.UpdateOptions{}); err != nil {
			return err
		}
		return reconciler.NewEvent(corev1.EventTypeNormal, "TriggerAuthenticationUpdated", "TriggerAuthentication updated: \"%s/%s\"",
			triggerAuth.Namespace, triggerAuth.Name)
	} else {
		logging.FromContext(ctx).Debugw("Reusing existing TriggerAuthentication", zap.Any("TriggerAuthentication", triggerAuth))
	}

	return nil
}

func (r *Reconciler) reconcileSecret(ctx context.Context, expectedSecret *corev1.Secret, obj metav1.Object) error {
	secret, err := r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Get(ctx, expectedSecret.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		secret, err = r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Create(ctx, expectedSecret, metav1.CreateOptions{})
		if err != nil {
			logging.FromContext(ctx).Errorw("Unable to create Secret", zap.Error(err))
			return reconciler.NewEvent(corev1.EventTypeWarning, "SecretDeploymentFailed", "Secret deployment failed to: \"%s/%s\", %w",
				expectedSecret.Namespace, expectedSecret.Name, err)
		}
		return reconciler.NewEvent(corev1.EventTypeNormal, "SecretCreated", "Secret created: \"%s/%s\"", secret.Namespace, secret.Name)
	} else if err != nil {
		logging.FromContext(ctx).Errorw("Unable to get an existing Secret", zap.Error(err))
		return err
	} else if !metav1.IsControlledBy(secret, obj) {
		return fmt.Errorf("secret %q is not owned by %q", secret.Name, obj)
	} else {
		// StringData is not populated on read so for now always update the secret
		logging.FromContext(ctx).Debug("Updating secret")
		secret.StringData = expectedSecret.StringData
		if _, err = r.KubeClient.CoreV1().Secrets(expectedSecret.Namespace).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
			return err
		}
		return reconciler.NewEvent(corev1.EventTypeNormal, "SecretUpdated", "Secret updated: \"%s/%s\"", secret.Namespace, secret.Name)
	}
}

func (r *Reconciler) isKEDAEnabled(ctx context.Context, namespace string) bool {
	// TODO: code below failing unit tests with err: "panic: interface conversion: testing.ActionImpl is not testing.GetAction: missing method GetName"
	/*if err := discovery.ServerSupportsVersion(r.KubeClient.Discovery(), keda.KedaSchemeGroupVersion); err == nil {
		return true
	}*/

	if _, err := r.KedaClient.KedaV1alpha1().ScaledObjects(namespace).List(ctx, metav1.ListOptions{}); err == nil || !apierrors.IsNotFound(err) {
		return true
	}
	return false
}

var (
	_ consumergroup.Interface = &Reconciler{}
)

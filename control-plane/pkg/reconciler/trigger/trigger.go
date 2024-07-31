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

package trigger

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/auth"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	brokerreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	deliveryOrderAnnotation = "kafka.eventing.knative.dev/delivery.order"
)

type FlagsHolder struct {
	Flags     feature.Flags
	FlagsLock sync.RWMutex
}

type Reconciler struct {
	*base.Reconciler
	*FlagsHolder

	BrokerLister         eventinglisters.BrokerLister
	ConfigMapLister      corelisters.ConfigMapLister
	ServiceAccountLister corelisters.ServiceAccountLister
	EventingClient       eventingclientset.Interface
	Resolver             *resolver.URIResolver

	Env *config.Env

	BrokerClass string

	DataPlaneConfigMapLabeler base.ConfigMapOption

	KafkaFeatureFlags *apisconfig.KafkaFeatureFlags

	// GetKafkaClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClusterAdmin clientpool.GetKafkaClusterAdminFunc
	// GetKafkaClient creates new sarama Client. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClient clientpool.GetKafkaClientFunc
	// InitOffsetsFunc initialize offsets for a provided set of topics and a provided consumer group id.
	// It's convenient to add this as Reconciler field so that we can mock the function used during the
	// reconciliation loop.
	InitOffsetsFunc kafka.InitOffsetsFunc
}

func (r *Reconciler) ReconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, trigger)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, trigger)
	errOIDC := auth.SetupOIDCServiceAccount(ctx, r.Flags, r.ServiceAccountLister, r.KubeClient, eventing.SchemeGroupVersion.WithKind("Trigger"), trigger.ObjectMeta, &trigger.Status, func(as *duckv1.AuthStatus) {
		trigger.Status.Auth = as
	})
	statusConditionManager := statusConditionManager{
		Trigger:  trigger,
		Configs:  r.Env,
		Recorder: controller.GetEventRecorder(ctx),
	}
	if trigger.Status.Annotations == nil {
		trigger.Status.Annotations = make(map[string]string, 0)
	}

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return statusConditionManager.failedToGetBroker(err)
	}

	if apierrors.IsNotFound(err) {

		// Actually check if the broker doesn't exist.
		// Note: do not introduce another `broker` variable with `:`
		broker, err = r.EventingClient.EventingV1().Brokers(trigger.Namespace).Get(ctx, trigger.Spec.Broker, metav1.GetOptions{})

		if apierrors.IsNotFound(err) {

			logger.Debug("broker not found", zap.String("finalizeDuringReconcile", "notFound"))
			// The associated broker doesn't exist anymore, so clean up Trigger resources.
			return r.FinalizeKind(ctx, trigger)
		}
	}

	// Ignore Triggers that are associated with a Broker we don't own.
	if hasRelevantBroker, brokerClass := r.hasRelevantBrokerClass(broker); !hasRelevantBroker {
		logger.Debug("Ignoring Trigger", zap.String(eventing.BrokerClassAnnotationKey, brokerClass))
		return nil
	}

	if !broker.GetDeletionTimestamp().IsZero() {

		logger.Debug("broker deleted", zap.String("finalizeDuringReconcile", "deleted"))

		// The associated broker doesn't exist anymore, so clean up Trigger resources and owning consumer group resource.
		return r.FinalizeKind(ctx, trigger)
	}

	statusConditionManager.propagateBrokerCondition(broker)

	if !broker.IsReady() {
		// Trigger will get re-queued once this broker is ready.
		return nil
	}

	if ok, err := r.reconcileConsumerGroup(ctx, broker, trigger); err != nil {
		return statusConditionManager.failedToResolveTriggerConfig(err)
	} else if !ok {
		return statusConditionManager.failedToResolveTriggerConfig(fmt.Errorf("missing broker status annotations, waiting"))
	}

	// Get data plane config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.failedToGetDataPlaneConfigMap(err)
	}

	logger.Debug("Got contract config map")

	// Get data plane config data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil || ct == nil {
		return statusConditionManager.failedToGetDataPlaneConfigFromConfigMap(err)
	}

	logger.Debug(
		"Got contract data from config map",
		zap.Any(base.ContractLogKey, ct),
	)

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	if brokerIndex == coreconfig.NoResource {
		return statusConditionManager.brokerNotFoundInDataPlaneConfigMap()
	}
	triggerIndex := coreconfig.FindEgress(ct.Resources[brokerIndex].Egresses, trigger.UID)

	triggerConfig, err := r.reconcileTriggerEgress(ctx, broker, trigger)
	if err != nil {
		return statusConditionManager.failedToResolveTriggerConfig(err)
	}
	statusConditionManager.subscriberResolved(triggerConfig)

	coreconfig.SetDeadLetterSinkURIFromEgressConfig(&trigger.Status.DeliveryStatus, triggerConfig.EgressConfig)

	coreconfig.AddOrUpdateEgressConfig(ct, brokerIndex, triggerConfig, triggerIndex)
	// Update the configuration map with the new dataPlaneConfig data.
	if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
		trigger.Status.MarkDependencyFailed(string(base.ConditionConfigMapUpdated), err.Error())
		return err
	}

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// Since the dispatcher side is the consumer side, we don't lose availability, and we can consider the Trigger
		// ready. So, log out the error and move on to the next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)

		statusConditionManager.failedToUpdateDispatcherPodsAnnotation(err)
	} else {
		logger.Debug("Updated dispatcher pod annotation")
	}

	logger.Debug("Contract config map updated")
	if errOIDC != nil {
		return errOIDC
	}

	return statusConditionManager.reconciled()
}

func (r *Reconciler) FinalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, trigger)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, trigger *eventing.Trigger) reconciler.Event {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, trigger)

	broker, err := r.BrokerLister.Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get broker from lister: %w", err)
	}

	if apierrors.IsNotFound(err) {
		// If the broker is deleted, resources associated with the Trigger will be deleted.
		return nil
	}

	// Get data plane config map.
	dataPlaneConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get data plane config map %s: %w", r.Env.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got data plane config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, dataPlaneConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	logger.Debug(
		"Got contract data from data plane config map",
		zap.Any(base.ContractLogKey, ct),
	)

	brokerIndex := coreconfig.FindResource(ct, broker.UID)
	if brokerIndex == coreconfig.NoResource {
		// If the broker is not there, resources associated with the Trigger are deleted accordingly.
		return nil
	}

	logger.Debug("Found Broker", zap.Int("brokerIndex", brokerIndex))

	egresses := ct.Resources[brokerIndex].Egresses
	triggerIndex := coreconfig.FindEgress(egresses, trigger.UID)
	if triggerIndex == coreconfig.NoEgress {
		// The trigger is not there, resources associated with the Trigger are deleted accordingly.
		logger.Debug("trigger not found in config map")

		return nil
	}

	logger.Debug("Found Trigger", zap.Int("triggerIndex", brokerIndex))

	// Delete the Trigger from the config map data.
	ct.Resources[brokerIndex].Egresses = deleteTrigger(egresses, triggerIndex)

	// Update data plane config map.
	err = r.UpdateDataPlaneConfigMap(ctx, ct, dataPlaneConfigMap)
	if err != nil {
		return err
	}

	logger.Debug("Updated data plane config map", zap.String("configmap", r.Env.DataPlaneConfigMapAsString()))

	// Update volume generation annotation of dispatcher pods
	if err := r.UpdateDispatcherPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		// Failing to update dispatcher pods annotation leads to config map refresh delayed by several seconds.
		// The delete trigger will eventually be seen by the data plane pods, so log out the error and move on to the
		// next step.
		logger.Warn(
			"Failed to update dispatcher pod annotation to trigger an immediate config map refresh",
			zap.Error(err),
		)
	} else {
		logger.Debug("Updated dispatcher pod annotation successfully")
	}

	return nil
}

func (r *Reconciler) reconcileTriggerEgress(ctx context.Context, broker *eventing.Broker, trigger *eventing.Trigger) (*contract.Egress, error) {
	destination, err := r.Resolver.AddressableFromDestinationV1(ctx, trigger.Spec.Subscriber, trigger)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Trigger.Spec.Subscriber: %w", err)
	}
	trigger.Status.SubscriberURI = destination.URL

	groupId, ok := trigger.Status.Annotations[kafka.GroupIdAnnotation]
	if !ok {
		return nil, fmt.Errorf("trigger.Status.Annotations[%s] not set", kafka.GroupIdAnnotation)
	}

	egress := &contract.Egress{
		Destination:   destination.URL.String(),
		ConsumerGroup: groupId,
		Uid:           string(trigger.UID),
		Reference: &contract.Reference{
			Uuid:      string(trigger.GetUID()),
			Namespace: trigger.GetNamespace(),
			Name:      trigger.GetName(),
		},
	}

	if destination.CACerts != nil {
		egress.DestinationCACerts = *destination.CACerts
	}
	if destination.Audience != nil {
		egress.DestinationAudience = *destination.Audience
	}
	if trigger.Status.Auth != nil && trigger.Status.Auth.ServiceAccountName != nil {
		egress.OidcServiceAccountName = *trigger.Status.Auth.ServiceAccountName
	}

	if len(trigger.Spec.Filters) > 0 {
		dialectedFilters := make([]*contract.DialectedFilter, 0, len(trigger.Spec.Filters))
		for _, f := range trigger.Spec.Filters {
			dialectedFilters = append(dialectedFilters, contract.FromSubscriptionFilter(f))
		}
		egress.DialectedFilter = dialectedFilters
	} else {
		if trigger.Spec.Filter != nil && trigger.Spec.Filter.Attributes != nil {
			egress.Filter = &contract.Filter{
				Attributes: trigger.Spec.Filter.Attributes,
			}
		}
	}

	triggerEgressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, trigger, trigger.Spec.Delivery, r.Env.DefaultBackoffDelayMs)
	if err != nil {
		return nil, fmt.Errorf("[trigger] %w", err)
	}
	brokerEgressConfig, err := coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, broker, broker.Spec.Delivery, r.Env.DefaultBackoffDelayMs)
	if err != nil {
		return nil, fmt.Errorf("[broker] %w", err)
	}
	// Merge Broker and Trigger egress configuration prioritizing the Trigger configuration.
	egress.EgressConfig = coreconfig.MergeEgressConfig(triggerEgressConfig, brokerEgressConfig)

	deliveryOrderAnnotationValue, ok := trigger.Annotations[deliveryOrderAnnotation]
	if ok {
		deliveryOrder, err := deliveryOrderFromString(deliveryOrderAnnotationValue)
		if err != nil {
			return nil, err
		}
		egress.DeliveryOrder = deliveryOrder
	}

	return egress, nil
}

func deleteTrigger(egresses []*contract.Egress, index int) []*contract.Egress {
	if len(egresses) == 1 {
		return nil
	}

	// replace the trigger to be deleted with the last one.
	egresses[index] = egresses[len(egresses)-1]
	// truncate the array.
	return egresses[:len(egresses)-1]
}

func (r *Reconciler) hasRelevantBrokerClass(broker *eventing.Broker) (bool, string) {
	brokerClass := broker.GetAnnotations()[eventing.BrokerClassAnnotationKey]
	return brokerClass == r.BrokerClass, brokerClass
}

func (r *Reconciler) reconcileConsumerGroup(ctx context.Context, broker *eventing.Broker, trigger *eventing.Trigger) (bool, error) {
	// Existing Brokers might not yet have this annotation
	topicName, ok := broker.Status.Annotations[kafka.TopicAnnotation]
	if !ok {
		return false, nil
	}

	namespace := broker.GetNamespace()
	if broker.Spec.Config.Namespace != "" {
		namespace = broker.Spec.Config.Namespace
	}

	secret, err := security.Secret(ctx, &security.AnnotationsSecretLocator{Annotations: broker.Status.Annotations, Namespace: namespace}, r.SecretProviderFunc())
	if err != nil {
		return false, fmt.Errorf("failed to get secret: %w", err)
	}
	if err := r.TrackSecret(secret, broker); err != nil {
		return false, fmt.Errorf("failed to track secret: %w", err)
	}

	bootstrapServers, ok := broker.Status.Annotations[kafka.BootstrapServersConfigMapKey]
	if !ok {
		return false, nil
	}
	bootstrapServersArr := kafka.BootstrapServersArray(bootstrapServers)

	kafkaClient, err := r.GetKafkaClient(ctx, bootstrapServersArr, secret)
	if err != nil {
		return false, fmt.Errorf("cannot obtain Kafka client, %w", err)
	}
	defer kafkaClient.Close()

	kafkaClusterAdmin, err := r.GetKafkaClusterAdmin(ctx, bootstrapServersArr, secret)
	if err != nil {
		return false, fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdmin.Close()

	// Existing Triggers might not yet have this annotation
	groupID, ok := trigger.Status.Annotations[kafka.GroupIdAnnotation]

	if !ok {
		groupID, err = r.KafkaFeatureFlags.ExecuteTriggersConsumerGroupTemplate(trigger.ObjectMeta)
		if err != nil {
			return false, fmt.Errorf("couldn't generate new consumergroup id: %w", err)
		}

		trigger.Status.Annotations[kafka.GroupIdAnnotation] = groupID
	}

	isLatest, err := kafka.IsOffsetLatest(r.ConfigMapLister, r.DataPlaneConfigMapNamespace, r.DataPlaneConfigConfigMapName, brokerreconciler.ConsumerConfigKey)
	if err != nil {
		return false, err
	}
	if isLatest {
		isPresentAndValid, err := kafka.AreTopicsPresentAndValid(kafkaClusterAdmin, topicName)
		if err != nil {
			return false, fmt.Errorf("topic %s doesn't exist or is invalid: %w", topicName, err)
		}
		if !isPresentAndValid {
			return false, fmt.Errorf("topic %s is invalid", topicName)
		}

		if _, err := r.InitOffsetsFunc(ctx, kafkaClient, kafkaClusterAdmin, []string{topicName}, groupID); err != nil {
			return false, fmt.Errorf("failed to initialize initial offsets: %w", err)
		}

		return true, nil
	}

	return true, nil
}

func deliveryOrderFromString(val string) (contract.DeliveryOrder, error) {
	switch strings.ToLower(val) {
	case string(sources.Ordered):
		return contract.DeliveryOrder_ORDERED, nil
	case string(sources.Unordered):
		return contract.DeliveryOrder_UNORDERED, nil
	default:
		return contract.DeliveryOrder_UNORDERED, fmt.Errorf("invalid annotation %s value: %s. Allowed values [ %q | %q ]", deliveryOrderAnnotation, val, sources.Ordered, sources.Unordered)
	}
}

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

package sink

import (
	"context"
	"fmt"
	"time"

	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	eventingv1alpha1listers "knative.dev/eventing/pkg/client/listers/eventing/v1alpha1"

	"k8s.io/utils/ptr"
	"knative.dev/eventing/pkg/auth"
	"knative.dev/pkg/logging"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"

	"knative.dev/eventing/pkg/apis/feature"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/clientpool"

	duckv1 "knative.dev/pkg/apis/duck/v1"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkalogging "knative.dev/eventing-kafka-broker/control-plane/pkg/logging"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

const (
	ExternalTopicOwner       = "external"
	ControllerTopicOwner     = "kafkasink-controller"
	caCertsSecretKey         = "ca.crt"
	sinkIngressTLSSecretName = "kafka-sink-ingress-server-tls" //nolint:gosec // This is not a hardcoded credential
)

type Reconciler struct {
	*base.Reconciler
	*config.Env

	Resolver *resolver.URIResolver

	ConfigMapLister   corelisters.ConfigMapLister
	EventPolicyLister eventingv1alpha1listers.EventPolicyLister

	// GetKafkaClusterAdmin creates new sarama ClusterAdmin. It's convenient to add this as Reconciler field so that we can
	// mock the function used during the reconciliation loop.
	GetKafkaClusterAdmin clientpool.GetKafkaClusterAdminFunc
	Prober               prober.NewProber

	IngressHost string
}

func (r *Reconciler) ReconcileKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.reconcileKind(ctx, ks)
	})
}

func (r *Reconciler) reconcileKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkalogging.CreateReconcileMethodLogger(ctx, ks)
	features := feature.FromContext(ctx)

	statusConditionManager := base.StatusConditionManager{
		Object:     ks,
		SetAddress: ks.Status.SetAddress,
		Env:        r.Env,
		Recorder:   controller.GetEventRecorder(ctx),
	}

	if !r.IsReceiverRunning() {
		return statusConditionManager.DataPlaneNotAvailable()
	}
	statusConditionManager.DataPlaneAvailable()

	if ks.GetStatus().Annotations == nil {
		ks.GetStatus().Annotations = make(map[string]string, 1)
	}

	secret, err := security.Secret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
	if err != nil {
		return fmt.Errorf("failed to get secret: %w", err)
	}
	if secret != nil {
		logger.Debug("Secret reference",
			zap.String("apiVersion", secret.APIVersion),
			zap.String("name", secret.Name),
			zap.String("namespace", secret.Namespace),
			zap.String("kind", secret.Kind),
		)
	}

	if err := r.TrackSecret(secret, ks); err != nil {
		return fmt.Errorf("failed to track secret: %w", err)
	}

	kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, ks.Spec.BootstrapServers, secret)
	if err != nil {
		return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
	}
	defer kafkaClusterAdminClient.Close()

	if ks.Spec.NumPartitions != nil && ks.Spec.ReplicationFactor != nil {

		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ControllerTopicOwner

		topicConfig := topicConfigFromSinkSpec(&ks.Spec)

		topic, err := kafka.CreateTopicIfDoesntExist(kafkaClusterAdminClient, logger, ks.Spec.Topic, topicConfig)
		if err != nil {
			return statusConditionManager.FailedToCreateTopic(topic, err)
		}
	} else {

		// If the topic is externally managed, we need to make sure that the topic exists and it's valid.
		ks.GetStatus().Annotations[base.TopicOwnerAnnotation] = ExternalTopicOwner

		isPresentAndValid, err := kafka.AreTopicsPresentAndValid(kafkaClusterAdminClient, ks.Spec.Topic)
		if err != nil {
			return statusConditionManager.TopicsNotPresentOrInvalidErr([]string{ks.Spec.Topic}, err)
		}
		if !isPresentAndValid {
			// The topic might be invalid.
			return statusConditionManager.TopicsNotPresentOrInvalid([]string{ks.Spec.Topic})
		}
	}
	statusConditionManager.TopicReady(ks.Spec.Topic)

	logger.Debug("Topic created", zap.Any("topic", ks.Spec.Topic))

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return statusConditionManager.FailedToGetConfigMap(err)
	}

	logger.Debug("Got contract config map")

	applyingEventPolicies, err := auth.GetEventPoliciesForResource(r.EventPolicyLister, eventing.SchemeGroupVersion.WithKind("KafkaSink"), ks.ObjectMeta)
	if err != nil {
		return fmt.Errorf("could not get applying eventpolicies for kafkasink: %v", err)
	}

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil && ct == nil {
		return statusConditionManager.FailedToGetDataFromConfigMap(err)
	}
	if ct == nil {
		ct = &contract.Contract{}
	}

	if err := r.setTrustBundles(ct); err != nil {
		return statusConditionManager.FailedToResolveConfig(err)
	}

	logger.Debug(
		"Got contract data from config map",
		zap.Any("contract", ct),
	)

	var audience *string
	if features.IsOIDCAuthentication() {
		audience = ptr.To(auth.GetAudience(eventing.SchemeGroupVersion.WithKind("KafkaSink"), ks.ObjectMeta))
		logging.FromContext(ctx).Debugw("Setting the kafkasinks audience", zap.String("audience", *audience))
	} else {
		logging.FromContext(ctx).Debug("Clearing the kafkasinks audience as OIDC is not enabled")
		audience = nil
	}

	// Get sink configuration.
	sinkConfig := r.getSinkContractResource(ctx, ks, secret, audience, applyingEventPolicies)
	statusConditionManager.ConfigResolved()

	sinkIndex := coreconfig.FindResource(ct, ks.UID)
	// Update contract data with the new sink configuration.
	coreconfig.AddOrUpdateResourceConfig(ct, sinkConfig, sinkIndex, logger)

	// Update the configuration map with the new contract data.
	if err := r.UpdateDataPlaneConfigMap(ctx, ct, contractConfigMap); err != nil {
		logger.Error("failed to update data plane config map", zap.Error(
			statusConditionManager.FailedToUpdateConfigMap(err),
		))
		return err
	}
	statusConditionManager.ConfigMapUpdated()

	logger.Debug("Config map updated")

	// We update receiver pods annotation regardless of our contract changed or not due to the fact  that in a previous
	// reconciliation we might have failed to update one of our data plane pod annotation, so we want to anyway update
	// remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Since we reject events to a non-existing Sink, which means that we cannot consider a Sink Ready if all
	// receivers haven't got the Sink, so update failures to receiver pods is a hard failure.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	logger.Debug("Updated receiver pod annotation")

	err = auth.UpdateStatusWithProvidedEventPolicies(features, &ks.Status.AppliedEventPoliciesStatus, &ks.Status, applyingEventPolicies)
	if err != nil {
		return fmt.Errorf("could not update KafkaSinks status with EventPolicies: %v", err)
	}

	var addressableStatus duckv1.AddressStatus
	if features.IsPermissiveTransportEncryption() {
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}

		httpAddress := receiver.HTTPAddress(r.IngressHost, audience, ks)
		httpsAddress := receiver.HTTPSAddress(r.IngressHost, audience, ks, caCerts)
		// Permissive mode:
		// - status.address http address with path-based routing
		// - status.addresses:
		//   - https address with path-based routing
		//   - http address with path-based routing
		addressableStatus.Address = &httpAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpsAddress, httpAddress}
	} else if features.IsStrictTransportEncryption() {
		// Strict mode: (only https addresses)
		// - status.address https address with path-based routing
		// - status.addresses:
		//   - https address with path-based routing
		caCerts, err := r.getCaCerts()
		if err != nil {
			return err
		}
		httpsAddress := receiver.HTTPSAddress(r.IngressHost, audience, ks, caCerts)

		addressableStatus.Address = &httpsAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpsAddress}
	} else {
		// Disabled mode:
		// Unchange
		httpAddress := receiver.HTTPAddress(r.IngressHost, audience, ks)

		addressableStatus.Address = &httpAddress
		addressableStatus.Addresses = []duckv1.Addressable{httpAddress}
	}

	proberAddressable := prober.ProberAddressable{
		AddressStatus: &addressableStatus,
		ResourceKey: types.NamespacedName{
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
	}

	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusReady); status != prober.StatusReady {
		statusConditionManager.ProbesStatusNotReady(status)
		return nil // Object will get re-queued once probe status changes.
	}

	statusConditionManager.ProbesStatusReady()

	ks.Status.AddressStatus = addressableStatus

	ks.GetConditionSet().Manage(ks.GetStatus()).MarkTrue(base.ConditionAddressable)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, ks *eventing.KafkaSink) reconciler.Event {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		return r.finalizeKind(ctx, ks)
	})
}

func (r *Reconciler) finalizeKind(ctx context.Context, ks *eventing.KafkaSink) error {
	logger := kafkalogging.CreateFinalizeMethodLogger(ctx, ks)

	// Get contract config map.
	contractConfigMap, err := r.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to get contract config map %s: %w", r.DataPlaneConfigMapAsString(), err)
	}

	logger.Debug("Got contract config map")

	// Get contract data.
	ct, err := r.GetDataPlaneConfigMapData(logger, contractConfigMap)
	if err != nil {
		return fmt.Errorf("failed to get contract: %w", err)
	}

	logger.Debug(
		"Got contract data from config map",
		zap.Any("contract", ct),
	)

	if err := r.DeleteResource(ctx, logger, ks.GetUID(), ct, contractConfigMap); err != nil {
		return err
	}

	ks.Status.AddressStatus = duckv1.AddressStatus{}

	// We update receiver pods annotation regardless of our contract changed or not due to the fact  that in a previous
	// reconciliation we might have failed to update one of our data plane pod annotation, so we want to anyway update
	// remaining annotations with the contract generation that was saved in the CM.
	// Note: if there aren't changes to be done at the pod annotation level, we just skip the update.

	// Update volume generation annotation of receiver pods
	if err := r.UpdateReceiverPodsContractGenerationAnnotation(ctx, logger, ct.Generation); err != nil {
		return err
	}

	//  Rationale: after deleting a topic closing a producer ends up blocking and requesting metadata for max.block.ms
	//  because topic metadata aren't available anymore.
	// 	See (under discussions KIPs, unlikely to be accepted as they are):
	// 	- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181306446
	// 	- https://cwiki.apache.org/confluence/display/KAFKA/KIP-286%3A+producer.send%28%29+should+not+block+on+metadata+update
	address := receiver.HTTPAddress(r.IngressHost, nil, ks)
	proberAddressable := prober.ProberAddressable{
		AddressStatus: &duckv1.AddressStatus{
			Address:   &address,
			Addresses: []duckv1.Addressable{address},
		},
		ResourceKey: types.NamespacedName{
			Namespace: ks.GetNamespace(),
			Name:      ks.GetName(),
		},
	}
	if status := r.Prober.Probe(ctx, proberAddressable, prober.StatusNotReady); status != prober.StatusNotReady {
		// Return a requeueKeyError that doesn't generate an event and it re-queues the object
		// for a new reconciliation.
		return controller.NewRequeueAfter(5 * time.Second)
	}

	if ks.GetStatus().Annotations[base.TopicOwnerAnnotation] == ControllerTopicOwner {
		secret, err := security.Secret(ctx, &SecretLocator{KafkaSink: ks}, r.SecretProviderFunc())
		if err != nil {
			return fmt.Errorf("failed to get secret: %w", err)
		}
		if secret != nil {
			logger.Debug("Secret reference",
				zap.String("apiVersion", secret.APIVersion),
				zap.String("name", secret.Name),
				zap.String("namespace", secret.Namespace),
				zap.String("kind", secret.Kind),
			)
		}

		kafkaClusterAdminClient, err := r.GetKafkaClusterAdmin(ctx, ks.Spec.BootstrapServers, secret)
		if err != nil {
			// even in error case, we return `normal`, since we are fine with leaving the
			// topic undeleted e.g. when we lose connection
			return fmt.Errorf("cannot obtain Kafka cluster admin, %w", err)
		}
		defer kafkaClusterAdminClient.Close()

		topic, err := kafka.DeleteTopic(kafkaClusterAdminClient, ks.Spec.Topic)
		if err != nil {
			return err
		}
		logger.Debug("Topic deleted", zap.String("topic", topic))
	}

	return nil
}

func topicConfigFromSinkSpec(kss *eventing.KafkaSinkSpec) *kafka.TopicConfig {
	return &kafka.TopicConfig{
		TopicDetail: sarama.TopicDetail{
			NumPartitions:     *kss.NumPartitions,
			ReplicationFactor: *kss.ReplicationFactor,
		},
		BootstrapServers: kss.BootstrapServers,
	}
}

func (r *Reconciler) getCaCerts() (*string, error) {
	secret, err := r.SecretLister.Secrets(r.SystemNamespace).Get(sinkIngressTLSSecretName)
	if err != nil {
		return nil, fmt.Errorf("failed to get CA certs from %s/%s: %w", r.SystemNamespace, sinkIngressTLSSecretName, err)
	}
	caCerts, ok := secret.Data[caCertsSecretKey]
	if !ok {
		return nil, nil
	}
	return pointer.String(string(caCerts)), nil
}

func (r *Reconciler) setTrustBundles(ct *contract.Contract) error {
	tb, err := coreconfig.TrustBundles(r.ConfigMapLister.ConfigMaps(r.SystemNamespace))
	if err != nil {
		return fmt.Errorf("failed to get trust bundles: %w", err)
	}
	ct.TrustBundles = tb
	return nil
}

func (r *Reconciler) getSinkContractResource(ctx context.Context, kafkaSink *eventingv1alpha1.KafkaSink, secret *corev1.Secret, audience *string, applyingEventPolicies []*v1alpha1.EventPolicy) *contract.Resource {
	features := feature.FromContext(ctx)
	sinkConfig := &contract.Resource{
		Uid:    string(kafkaSink.UID),
		Topics: []string{kafkaSink.Spec.Topic},
		Ingress: &contract.Ingress{
			Path:          receiver.PathFromObject(kafkaSink),
			ContentMode:   coreconfig.ContentModeFromString(*kafkaSink.Spec.ContentMode),
			EventPolicies: coreconfig.ContractEventPoliciesFromEventPolicies(applyingEventPolicies, kafkaSink.Namespace, features),
		},
		FeatureFlags: &contract.FeatureFlags{
			EnableEventTypeAutocreate: features.IsEnabled(feature.EvenTypeAutoCreate),
		},
		BootstrapServers: kafka.BootstrapServersCommaSeparated(kafkaSink.Spec.BootstrapServers),
		Reference: &contract.Reference{
			Uuid:         string(kafkaSink.GetUID()),
			Namespace:    kafkaSink.GetNamespace(),
			Name:         kafkaSink.GetName(),
			Kind:         "KafkaSink",
			GroupVersion: eventingv1alpha1.SchemeGroupVersion.String(),
		},
	}
	if kafkaSink.Spec.HasAuthConfig() {
		sinkConfig.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
	}

	if audience != nil {
		sinkConfig.Ingress.Audience = *audience
	}

	return sinkConfig
}

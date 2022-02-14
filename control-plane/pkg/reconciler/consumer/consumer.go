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

package consumer

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing"
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/reconciler/eventing/v1alpha1/consumer"
	kafkainternalslisters "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/listers/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	coreconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/core/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

type Reconciler struct {
	SerDe contract.FormatSerDe

	Enqueue func(c *kafkainternals.Consumer)

	Resolver            *resolver.URIResolver
	Tracker             tracker.Interface
	ConsumerGroupLister kafkainternalslisters.ConsumerGroupLister
	SecretLister        corelisters.SecretLister
	PodLister           corelisters.PodLister
	KubeClient          kubernetes.Interface
}

var (
	_ consumer.Interface = Reconciler{}
	_ consumer.Finalizer = Reconciler{}
)

func (r Reconciler) ReconcileKind(ctx context.Context, c *kafkainternals.Consumer) reconciler.Event {
	logger := logging.FromContext(ctx).Desugar()

	resourceCt, err := r.reconcileContractResource(ctx, c)
	if err != nil {
		return c.MarkReconcileContractFailed(err)
	}
	c.MarkReconcileContractSucceeded()

	if resourceCt == nil {
		return nil // Resource will get queued once we have all resources to build the contract.
	}

	bound, err := r.schedule(ctx, logger, c, addResource(resourceCt))
	if err != nil {
		return c.MarkBindFailed(err)
	}
	if !bound {
		// Resource will get queued once we have all resources to schedule the Consumer.
		c.MarkBindInProgress()
		return nil
	}
	c.MarkBindSucceeded()

	return nil
}

func (r Reconciler) FinalizeKind(ctx context.Context, c *kafkainternals.Consumer) reconciler.Event {

	logger := logging.FromContext(ctx).Desugar()

	if _, err := r.schedule(ctx, logger, c, removeResource); err != nil {
		return c.MarkBindFailed(err)
	}

	return nil
}

func (r Reconciler) reconcileContractResource(ctx context.Context, c *kafkainternals.Consumer) (*contract.Resource, error) {
	egress, err := r.reconcileContractEgress(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to reconcile egress: %w", err)
	}

	userFacingResourceRef, err := r.reconcileUserFacingResourceRef(c)
	if err != nil {
		return nil, fmt.Errorf("failed to reconcile user facing resource reference: %w", err)
	}
	if userFacingResourceRef == nil {
		// We don't have yet the user-facing resource in the lister cache.
		return nil, nil
	}

	resource := &contract.Resource{
		Uid:                 string(c.UID),
		Topics:              c.Spec.Topics,
		BootstrapServers:    c.Spec.Configs.Configs["bootstrap.servers"],
		Egresses:            []*contract.Egress{egress},
		Auth:                nil, // Auth will be added by reconcileAuth
		CloudEventOverrides: reconcileCEOverrides(c),
		Reference:           userFacingResourceRef,
	}

	if err := r.reconcileAuth(ctx, c, resource); err != nil {
		return nil, fmt.Errorf("failed to reconcile auth: %w", err)
	}

	return resource, nil
}

func (r Reconciler) reconcileContractEgress(ctx context.Context, c *kafkainternals.Consumer) (*contract.Egress, error) {
	destination, err := r.Resolver.URIFromDestinationV1(ctx, c.Spec.Subscriber, c)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve subcriber: %w", err)
	}
	c.Status.SubscriberURI = destination

	egressConfig := &contract.EgressConfig{}
	if c.Spec.Delivery != nil {
		egressConfig, err = coreconfig.EgressConfigFromDelivery(ctx, r.Resolver, c, c.Spec.Delivery.DeliverySpec, 200)
		if err != nil {
			return nil, err
		}
	}
	c.Status.DeliveryStatus.DeadLetterSinkURI, _ = apis.ParseURL(egressConfig.DeadLetter)

	egress := &contract.Egress{
		ConsumerGroup: c.Spec.Configs.Configs["group.id"],
		Destination:   destination.String(),
		ReplyStrategy: nil, // Reply will be added by reconcileReplyStrategy
		Filter:        reconcileFilters(c),
		Uid:           string(c.UID),
		EgressConfig:  egressConfig,
		DeliveryOrder: reconcileDeliveryOrder(c),

		KeyType: 0, // TODO handle key type
	}

	if err := r.reconcileReplyStrategy(ctx, c, egress); err != nil {
		return nil, fmt.Errorf("failed to reconcile reply strategy: %w", err)
	}

	return egress, nil
}

func (r Reconciler) reconcileAuth(ctx context.Context, c *kafkainternals.Consumer, resource *contract.Resource) error {

	if c.Spec.Auth == nil {
		return nil
	}
	if c.Spec.Auth.NetSpec != nil {
		authContext, err := security.ResolveAuthContextFromNetSpec(r.SecretLister, c.GetNamespace(), *c.Spec.Auth.NetSpec)
		if err != nil {
			return fmt.Errorf("failed to resolve auth context: %w", err)
		}
		resource.Auth = &contract.Resource_MultiAuthSecret{MultiAuthSecret: authContext.MultiSecretReference}
		return nil
	}
	if c.Spec.Auth.AuthSpec != nil {
		secret, err := security.Secret(ctx, &SecretLocator{Consumer: c}, r.SecretProviderFunc())
		if err != nil {
			return fmt.Errorf("failed to get secret: %w", err)
		}
		resource.Auth = &contract.Resource_AuthSecret{
			AuthSecret: &contract.Reference{
				Uuid:      string(secret.UID),
				Namespace: secret.Namespace,
				Name:      secret.Name,
				Version:   secret.ResourceVersion,
			},
		}
		ref := tracker.Reference{
			APIVersion: c.APIVersion,
			Kind:       c.Kind,
			Namespace:  c.GetNamespace(),
			Name:       c.GetName(),
		}
		if err := r.Tracker.TrackReference(ref, c); err != nil {
			return fmt.Errorf("failed to track reference %v: %w", ref, err)
		}
		return nil
	}

	return nil
}

func (r Reconciler) SecretProviderFunc() security.SecretProviderFunc {
	return security.DefaultSecretProviderFunc(r.SecretLister, r.KubeClient)
}

func reconcileCEOverrides(c *kafkainternals.Consumer) *contract.CloudEventOverrides {
	if c.Spec.CloudEventOverrides == nil {
		return nil
	}
	return &contract.CloudEventOverrides{Extensions: c.Spec.CloudEventOverrides.Extensions}
}

func (r Reconciler) reconcileUserFacingResourceRef(c *kafkainternals.Consumer) (*contract.Reference, error) {

	cg, err := r.ConsumerGroupLister.ConsumerGroups(c.GetNamespace()).Get(c.GetConsumerGroup().Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get %s: %w", kafkainternals.ConsumerGroupGroupVersionKind.Kind, err)
	}
	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	userFacingResource := cg.GetUserFacingResourceRef()
	ref := &contract.Reference{
		Uuid:      string(userFacingResource.UID),
		Namespace: c.GetNamespace(),
		Name:      userFacingResource.Name,
	}
	return ref, nil
}

func reconcileDeliveryOrder(c *kafkainternals.Consumer) contract.DeliveryOrder {
	if c.Spec.Delivery == nil {
		return contract.DeliveryOrder_UNORDERED
	}
	switch c.Spec.Delivery.Ordering {
	case eventing.Ordered:
		return contract.DeliveryOrder_ORDERED
	case eventing.Unordered:
		return contract.DeliveryOrder_UNORDERED
	}
	return contract.DeliveryOrder_UNORDERED
}

func (r Reconciler) reconcileReplyStrategy(ctx context.Context, c *kafkainternals.Consumer, egress *contract.Egress) error {
	if c.Spec.Reply == nil {
		return nil
	}
	if c.Spec.Reply.NoReply != nil {
		egress.ReplyStrategy = &contract.Egress_DiscardReply{}
		return nil
	}
	if c.Spec.Reply.URLReply != nil {
		destination, err := r.Resolver.URIFromDestinationV1(ctx, c.Spec.Reply.URLReply.Destination, c)
		if err != nil {
			return fmt.Errorf("failed to resolve reply destination: %w", err)
		}
		egress.ReplyStrategy = &contract.Egress_ReplyUrl{
			ReplyUrl: destination.String(),
		}
		return nil
	}
	if c.Spec.Reply.TopicReply != nil {
		egress.ReplyStrategy = &contract.Egress_ReplyToOriginalTopic{}
		return nil
	}

	return nil
}

func reconcileFilters(c *kafkainternals.Consumer) *contract.Filter {
	if c.Spec.Filters == nil {
		return nil
	}
	if c.Spec.Filters.Filter != nil {
		return &contract.Filter{Attributes: c.Spec.Filters.Filter.Attributes}
	}

	return nil
}

type contractMutatorFunc func(logger *zap.Logger, ct *contract.Contract, c *kafkainternals.Consumer) int

func addResource(resource *contract.Resource) contractMutatorFunc {
	return func(logger *zap.Logger, ct *contract.Contract, c *kafkainternals.Consumer) int {
		return coreconfig.AddOrUpdateResourceConfig(ct, resource, coreconfig.FindResource(ct, c.GetUID()), logger)
	}
}

func removeResource(_ *zap.Logger, ct *contract.Contract, c *kafkainternals.Consumer) int {
	idx := coreconfig.FindResource(ct, c.GetUID())
	if idx == coreconfig.NoResource {
		return coreconfig.NoResource
	}
	coreconfig.DeleteResource(ct, idx)
	return idx
}

// schedule mutates the ConfigMap associated with the pod specified by Consumer.Spec.PodBind.
//
// The actual mutation is done by calling the provided contractMutatorFunc.
func (r Reconciler) schedule(ctx context.Context, logger *zap.Logger, c *kafkainternals.Consumer, mutatorFunc contractMutatorFunc) (bool, error) {
	// Get the data plane pod when the Consumer should be scheduled.
	p, err := r.PodLister.Pods(c.Spec.PodBind.PodNamespace).Get(c.Spec.PodBind.PodName)
	if err != nil {
		return false, fmt.Errorf("failed to get pod %s/%s: %w", c.Spec.PodBind.PodNamespace, c.Spec.PodBind.PodName, err)
	}
	if apierrors.IsNotFound(err) {
		// Pod not found, return no error since the Consumer
		// will get re-queued when the pod is added.
		return false, nil
	}

	// Get contract associated with the pod.
	cmName, err := cmNameFromPod(p, c)
	if err != nil {
		return false, err
	}

	b := r.commonReconciler(p, cmName)

	cm, err := b.GetOrCreateDataPlaneConfigMap(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get or create data plane ConfigMap %s/%s: %w", p.GetNamespace(), cmName, err)
	}

	// Check if the pod is running after trying to
	// get or create the associated ConfigMap, since
	// it won't become ready until we have created the
	// ConfigMap
	if p.Status.Phase != corev1.PodRunning {
		return false, nil
	}

	ct, err := b.GetDataPlaneConfigMapData(logger, cm)
	if err != nil {
		return false, fmt.Errorf("failed to get contract from ConfigMap %s/%s: %w", p.GetNamespace(), cmName, err)
	}

	if changed := mutatorFunc(logger, ct, c); changed == coreconfig.ResourceChanged {
		logger.Debug("Contract changed", zap.Int("changed", changed))

		ct.IncrementGeneration()

		if err := b.UpdateDataPlaneConfigMap(ctx, ct, cm); err != nil {
			return false, err
		}
	}

	return true, b.UpdatePodsAnnotation(ctx, logger, "dispatcher" /* component, for logging */, ct.Generation, []*corev1.Pod{p})
}

func (r Reconciler) commonReconciler(p *corev1.Pod, cmName string) base.Reconciler {
	return base.Reconciler{
		KubeClient:                  r.KubeClient,
		PodLister:                   r.PodLister,
		SecretLister:                r.SecretLister,
		SecretTracker:               r.Tracker,
		ConfigMapTracker:            r.Tracker,
		DataPlaneConfigMapNamespace: p.GetNamespace(),
		DataPlaneConfigMapName:      cmName,
		DataPlaneConfigFormat:       string(r.SerDe.Format),
		SystemNamespace:             p.GetNamespace(),
		DispatcherLabel:             "",
		ReceiverLabel:               "",
	}
}

func cmNameFromPod(p *corev1.Pod, c *kafkainternals.Consumer) (string, error) {
	var vDp *corev1.Volume
	for i, v := range p.Spec.Volumes {
		if v.Name == kafkainternals.DispatcherVolumeName && v.ConfigMap != nil && v.ConfigMap.Name != "" {
			vDp = &p.Spec.Volumes[i]
			break
		}
	}
	if vDp == nil {
		return "", fmt.Errorf("failed to get data plane volume %s in pod %s/%s", eventing.ConfigMapVolumeName, c.Spec.PodBind.PodNamespace, c.Spec.PodBind.PodName)
	}
	return vDp.ConfigMap.Name, nil
}

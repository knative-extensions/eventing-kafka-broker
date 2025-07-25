/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	duckv1 "knative.dev/pkg/apis/duck/v1"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/eventing/pkg/apis/feature"
	v1 "knative.dev/eventing/pkg/apis/messaging/v1"
)

// SubscriptionOption enables further configuration of a Subscription.
type SubscriptionOption func(*v1.Subscription)

// NewSubscription creates a Subscription with SubscriptionOptions
func NewSubscription(name, namespace string, so ...SubscriptionOption) *v1.Subscription {
	s := &v1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	for _, opt := range so {
		opt(s)
	}
	s.SetDefaults(context.Background())
	return s
}

// NewSubscriptionWithoutNamespace creates a Subscription with SubscriptionOptions but without a specific namespace
func NewSubscriptionWithoutNamespace(name string, so ...SubscriptionOption) *v1.Subscription {
	s := &v1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	for _, opt := range so {
		opt(s)
	}
	s.SetDefaults(context.Background())
	return s
}

func WithSubscriptionUID(uid types.UID) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.UID = uid
	}
}

func WithSubscriptionGeneration(gen int64) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Generation = gen
	}
}

func WithSubscriptionStatusObservedGeneration(gen int64) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.ObservedGeneration = gen
	}
}

func WithSubscriptionGenerateName(generateName string) SubscriptionOption {
	return func(c *v1.Subscription) {
		c.ObjectMeta.GenerateName = generateName
	}
}

// WithInitSubscriptionConditions initializes the Subscriptions's conditions.
func WithInitSubscriptionConditions(s *v1.Subscription) {
	s.Status.InitializeConditions()
}

func WithSubscriptionReady(s *v1.Subscription) {
	s.Status = *eventingv1.TestHelper.ReadySubscriptionStatus()
}

// TODO: this can be a runtime object
func WithSubscriptionDeleted(s *v1.Subscription) {
	t := metav1.NewTime(time.Unix(1e9, 0))
	s.ObjectMeta.SetDeletionTimestamp(&t)
}

func WithSubscriptionOwnerReferences(ownerReferences []metav1.OwnerReference) SubscriptionOption {
	return func(c *v1.Subscription) {
		c.ObjectMeta.OwnerReferences = ownerReferences
	}
}

func WithSubscriptionLabels(labels map[string]string) SubscriptionOption {
	return func(c *v1.Subscription) {
		c.ObjectMeta.Labels = labels
	}
}

func WithSubscriptionChannel(gvk metav1.GroupVersionKind, name string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			APIVersion: apiVersion(gvk),
			Kind:       gvk.Kind,
			Name:       name,
		}
	}
}

func WithSubscriptionChannelRef(gvk metav1.GroupVersionKind, name string, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			APIVersion: apiVersion(gvk),
			Kind:       gvk.Kind,
			Name:       name,
			Namespace:  namespace,
		}
	}
}

func WithSubscriptionChannelUsingGroup(gvk metav1.GroupVersionKind, name string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			Group: gvk.Group,
			Kind:  gvk.Kind,
			Name:  name,
		}
	}
}

//nolist:staticcheck  // Should be "API"
func WithSubscriptionChannelUsingApiVersionAndGroup(gvk metav1.GroupVersionKind, name string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			APIVersion: apiVersion(gvk),
			Group:      gvk.Group,
			Kind:       gvk.Kind,
			Name:       name,
		}
	}
}

func WithSubscriptionChannelRefUsingGroup(gvk metav1.GroupVersionKind, name string, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			Group:     gvk.Group,
			Kind:      gvk.Kind,
			Name:      name,
			Namespace: namespace,
		}
	}
}

// nolint: staticcheck  // Should be "API"
func WithSubscriptionChannelRefUsingApiVersionAndGroup(gvk metav1.GroupVersionKind, name string, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Channel = duckv1.KReference{
			APIVersion: apiVersion(gvk),
			Group:      gvk.Group,
			Kind:       gvk.Kind,
			Name:       name,
			Namespace:  namespace,
		}
	}
}

func WithSubscriptionSubscriberRef(gvk metav1.GroupVersionKind, name, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Subscriber = &duckv1.Destination{
			Ref: &duckv1.KReference{
				APIVersion: apiVersion(gvk),
				Kind:       gvk.Kind,
				Name:       name,
				Namespace:  namespace,
			},
		}
	}
}

func WithSubscriptionSubscriberRefUsingGroup(gvk metav1.GroupVersionKind, name, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Subscriber = &duckv1.Destination{
			Ref: &duckv1.KReference{
				Group:     gvk.Group,
				Kind:      gvk.Kind,
				Name:      name,
				Namespace: namespace,
			},
		}
	}
}

//nolist:staticcheck  // Should be "API"
func WithSubscriptionSubscriberRefUsingApiVersionAndGroup(gvk metav1.GroupVersionKind, name, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Subscriber = &duckv1.Destination{
			Ref: &duckv1.KReference{
				APIVersion: apiVersion(gvk),
				Group:      gvk.Group,
				Kind:       gvk.Kind,
				Name:       name,
				Namespace:  namespace,
			},
		}
	}
}

func WithSubscriptionDeliveryRef(gvk metav1.GroupVersionKind, name, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Delivery = &eventingduckv1.DeliverySpec{
			DeadLetterSink: &duckv1.Destination{
				Ref: &duckv1.KReference{
					APIVersion: apiVersion(gvk),
					Kind:       gvk.Kind,
					Name:       name,
					Namespace:  namespace,
				},
			},
		}
	}
}

func WithSubscriptionPhysicalSubscriptionSubscriber(subscriber *duckv1.Addressable) SubscriptionOption {
	return func(s *v1.Subscription) {
		if subscriber == nil {
			panic(errors.New("nil subscriber"))
		}
		s.Status.PhysicalSubscription.SubscriberURI = subscriber.URL
		s.Status.PhysicalSubscription.SubscriberCACerts = subscriber.CACerts
		s.Status.PhysicalSubscription.SubscriberAudience = subscriber.Audience
	}
}

func WithSubscriptionPhysicalSubscriptionReply(reply *duckv1.Addressable) SubscriptionOption {
	return func(s *v1.Subscription) {
		if reply == nil {
			panic(errors.New("nil reply"))
		}
		s.Status.PhysicalSubscription.ReplyURI = reply.URL
		s.Status.PhysicalSubscription.ReplyCACerts = reply.CACerts
		s.Status.PhysicalSubscription.ReplyAudience = reply.Audience
	}
}

func WithSubscriptionDeadLetterSink(dls *duckv1.Addressable) SubscriptionOption {
	return func(s *v1.Subscription) {
		if dls == nil {
			panic(errors.New("nil URI"))
		}
		s.Status.PhysicalSubscription.DeliveryStatus = eventingduckv1.NewDeliveryStatusFromAddressable(dls)
	}
}

func WithSubscriptionFinalizers(finalizers ...string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Finalizers = finalizers
	}
}

func MarkSubscriptionReady(s *v1.Subscription) {
	s.Status.MarkChannelReady()
	s.Status.MarkReferencesResolved()
	s.Status.MarkAddedToChannel()
}

func MarkAddedToChannel(s *v1.Subscription) {
	s.Status.MarkAddedToChannel()
}

func MarkNotAddedToChannel(reason, msg string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkNotAddedToChannel(reason, msg)
	}
}

func MarkReferencesResolved(s *v1.Subscription) {
	s.Status.MarkReferencesResolved()
}

func WithSubscriptionReferencesNotResolved(reason, msg string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkReferencesNotResolved(reason, msg)
	}
}

func WithSubscriptionReferencesResolvedUnknown(reason, msg string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkReferencesResolvedUnknown(reason, msg)
	}
}

func WithSubscriptionReply(gvk metav1.GroupVersionKind, name, namespace string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Spec.Reply = &duckv1.Destination{
			Ref: &duckv1.KReference{
				APIVersion: apiVersion(gvk),
				Kind:       gvk.Kind,
				Name:       name,
				Namespace:  namespace,
			},
		}
	}
}

func WithSubscriptionOIDCIdentityCreatedSucceeded() SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkOIDCIdentityCreatedSucceeded()
	}
}

func WithSubscriptionOIDCIdentityCreatedSucceededBecauseOIDCFeatureDisabled() SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkOIDCIdentityCreatedSucceededWithReason(fmt.Sprintf("%s feature disabled", feature.OIDCAuthentication), "")
	}
}

func WithSubscriptionOIDCIdentityCreatedFailed(reason, message string) SubscriptionOption {
	return func(s *v1.Subscription) {
		s.Status.MarkOIDCIdentityCreatedFailed(reason, message)
	}
}

func WithSubscriptionOIDCServiceAccountName(name string) SubscriptionOption {
	return func(s *v1.Subscription) {
		if s.Status.Auth == nil {
			s.Status.Auth = &duckv1.AuthStatus{}
		}

		s.Status.Auth.ServiceAccountName = &name
	}
}

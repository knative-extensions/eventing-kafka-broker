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

package config

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/runtime/protoimpl"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1alpha1 "knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	fakedynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	pointer "knative.dev/pkg/ptr"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

func TestContentModeFromString(t *testing.T) {
	type args struct {
		mode string
	}
	tests := []struct {
		name string
		args args
		want contract.ContentMode
	}{
		{
			name: eventing.ModeBinary,
			args: args{
				mode: eventing.ModeBinary,
			},
			want: contract.ContentMode_BINARY,
		},
		{
			name: eventing.ModeStructured,
			args: args{
				mode: eventing.ModeStructured,
			},
			want: contract.ContentMode_STRUCTURED,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ContentModeFromString(tt.args.mode); got != tt.want {
				t.Errorf("ContentModeFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBackoffPolicyFromString(t *testing.T) {
	linerar := eventingduck.BackoffPolicyLinear
	exponential := eventingduck.BackoffPolicyExponential
	wrong := eventingduck.BackoffPolicyType("default")
	tests := []struct {
		name          string
		backoffPolicy *eventingduck.BackoffPolicyType
		want          contract.BackoffPolicy
	}{
		{
			name:          "nil",
			backoffPolicy: nil,
			want:          contract.BackoffPolicy_Exponential,
		},
		{
			name:          "exponential",
			backoffPolicy: &exponential,
			want:          contract.BackoffPolicy_Exponential,
		},
		{
			name:          "linear",
			backoffPolicy: &linerar,
			want:          contract.BackoffPolicy_Linear,
		},
		{
			name:          "default",
			backoffPolicy: &wrong,
			want:          contract.BackoffPolicy_Exponential,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BackoffPolicyFromString(tt.backoffPolicy); got != tt.want {
				t.Errorf("BackoffPolicyFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDurationMillisFromISO8601String(t *testing.T) {

	tests := []struct {
		name         string
		backoffDelay *string
		defaultDelay uint64
		want         uint64
		wantError    bool
	}{
		{
			name:         "happy case",
			backoffDelay: pointer.String("PT2S"),
			defaultDelay: 1000,
			want:         2000,
		},
		{
			name:         "happy case fractional",
			backoffDelay: pointer.String("PT0.2S"),
			defaultDelay: 1000,
			want:         200,
		},
		{
			name:         "happy case fractional - 2 decimals",
			backoffDelay: pointer.String("PT0.25S"),
			defaultDelay: 1000,
			want:         200, // The library we use don't support more than one decimal, it should be 250 though.
		},
		{
			name:         "happy case fractional minutes",
			backoffDelay: pointer.String("PT1M0.2S"),
			defaultDelay: 1000,
			want:         1*60*1000 + 200,
		},
		{
			name:         "milliseconds",
			backoffDelay: pointer.String("PT0.001S"),
			defaultDelay: 1000,
			want:         0,
		},
		{
			name:         "microseconds",
			backoffDelay: pointer.String("PT0.000001S"),
			defaultDelay: 1000,
			want:         0,
		},
		{
			name:         "65 seconds",
			backoffDelay: pointer.String("PT65S"),
			defaultDelay: 1000,
			want:         1 * 65 * 1000,
		},
		{
			name:         "default",
			defaultDelay: 1000,
			want:         1000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DurationMillisFromISO8601String(tt.backoffDelay, tt.defaultDelay)
			if (err != nil) != tt.wantError {
				t.Errorf("wantError = %v got %v", tt.wantError, err)
			}
			if got != tt.want {
				t.Errorf("DurationMillisFromISO8601String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIncrementContractGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		generation uint64
		expected   uint64
	}{
		{
			name:       "min",
			generation: 0,
			expected:   1,
		},
		{
			name:       "max",
			generation: math.MaxUint64 - 2,
			expected:   0,
		},
		{
			name:       "random",
			generation: 42,
			expected:   43,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ct := &contract.Contract{
				Generation: tt.generation,
			}
			IncrementContractGeneration(ct)

			if ct.Generation != tt.expected {
				t.Errorf("Got %d expected %d", ct.Generation, tt.expected)
			}
		})
	}
}

func TestEgressConfigFromDelivery(t *testing.T) {
	ctx := context.Background()
	ctx, _ = fakedynamicclient.With(ctx, runtime.NewScheme())
	ctx = addressable.WithDuck(ctx)

	var (
		url = &apis.URL{
			Scheme: "http",
			Host:   "localhost",
			Path:   "/path",
		}

		exponential = eventingduck.BackoffPolicyExponential
	)

	tests := []struct {
		name                  string
		ctx                   context.Context
		resolver              *resolver.URIResolver
		parent                metav1.Object
		delivery              *eventingduck.DeliverySpec
		defaultBackoffDelayMs uint64
		want                  *contract.EgressConfig
		wantErr               bool
	}{
		{
			name:                  "nil delivery",
			ctx:                   ctx,
			resolver:              resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:                &eventing.KafkaSink{},
			delivery:              nil,
			defaultBackoffDelayMs: 0,
			want:                  nil,
			wantErr:               false,
		},
		{
			name:                  "nil retry and nil dls",
			ctx:                   ctx,
			resolver:              resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:                &eventing.KafkaSink{},
			delivery:              &eventingduck.DeliverySpec{},
			defaultBackoffDelayMs: 0,
			want:                  nil,
			wantErr:               false,
		},
		{
			name:     "full delivery",
			ctx:      ctx,
			resolver: resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				DeadLetterSink: &duckv1.Destination{URI: url},
				Retry:          pointer.Int32(3),
				BackoffPolicy:  &exponential,
				BackoffDelay:   pointer.String("PT1S"),
				Timeout:        pointer.String("PT2S"),
			},
			defaultBackoffDelayMs: 0,
			want: &contract.EgressConfig{
				DeadLetter:    url.String(),
				Retry:         3,
				BackoffPolicy: contract.BackoffPolicy_Exponential,
				BackoffDelay:  uint64(time.Second.Milliseconds()),
				Timeout:       uint64(time.Second.Milliseconds() * 2),
			},
			wantErr: false,
		},
		{
			name:     "only dls",
			ctx:      ctx,
			resolver: resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				DeadLetterSink: &duckv1.Destination{URI: url},
			},
			defaultBackoffDelayMs: 0,
			want: &contract.EgressConfig{
				DeadLetter: url.String(),
			},
			wantErr: false,
		},
		{
			name:     "only timeout",
			ctx:      ctx,
			resolver: resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				Timeout: pointer.String("PT2S"),
			},
			defaultBackoffDelayMs: 0,
			want: &contract.EgressConfig{
				Timeout: uint64(time.Second.Milliseconds() * 2),
			},
			wantErr: false,
		},
		{
			name:     "only retry - use default backoff delay",
			ctx:      ctx,
			resolver: resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0)),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				Retry:         pointer.Int32(3),
				BackoffPolicy: &exponential,
			},
			defaultBackoffDelayMs: 100,
			want: &contract.EgressConfig{
				Retry:         3,
				BackoffPolicy: contract.BackoffPolicy_Exponential,
				BackoffDelay:  100,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EgressConfigFromDelivery(tt.ctx, tt.resolver, tt.parent, tt.delivery, tt.defaultBackoffDelayMs)
			if (err != nil) != tt.wantErr {
				t.Errorf("EgressConfigFromDelivery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EgressConfigFromDelivery() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeEgressConfig(t *testing.T) {

	tt := []struct {
		name     string
		e0       *contract.EgressConfig
		e1       *contract.EgressConfig
		expected *contract.EgressConfig
	}{
		{
			name: "e0 nil",
			e1: &contract.EgressConfig{
				Retry: 42,
			},
			expected: &contract.EgressConfig{
				Retry: 42,
			},
		},
		{
			name: "e1 nil",
			e0: &contract.EgressConfig{
				Retry: 42,
			},
			expected: &contract.EgressConfig{
				Retry: 42,
			},
		},
		{
			name: "e0 retry priority",
			e0: &contract.EgressConfig{
				Retry: 42,
			},
			e1: &contract.EgressConfig{
				Retry: 43,
			},
			expected: &contract.EgressConfig{
				Retry: 42,
			},
		},
		{
			name: "e0 dead letter priority",
			e0: &contract.EgressConfig{
				DeadLetter: "e0",
			},
			e1: &contract.EgressConfig{
				Retry:      43,
				DeadLetter: "e1",
			},
			expected: &contract.EgressConfig{
				DeadLetter: "e0",
				Retry:      43,
			},
		},
		{
			name: "e0 timeout priority",
			e0: &contract.EgressConfig{
				DeadLetter: "e0",
				Timeout:    100,
			},
			e1: &contract.EgressConfig{
				Retry:      43,
				DeadLetter: "e1",
				Timeout:    101,
			},
			expected: &contract.EgressConfig{
				DeadLetter: "e0",
				Retry:      43,
				Timeout:    100,
			},
		},
		{
			name: "e0 backoff delay priority",
			e0: &contract.EgressConfig{
				DeadLetter:   "e0",
				Timeout:      100,
				BackoffDelay: 4001,
			},
			e1: &contract.EgressConfig{
				Retry:        43,
				DeadLetter:   "e1",
				Timeout:      101,
				BackoffDelay: 4000,
			},
			expected: &contract.EgressConfig{
				DeadLetter:   "e0",
				Retry:        43,
				Timeout:      100,
				BackoffDelay: 4001,
			},
		},
		{
			name: "e0 backoff policy priority",
			e0: &contract.EgressConfig{
				DeadLetter:    "e0",
				Timeout:       100,
				BackoffDelay:  4001,
				BackoffPolicy: 0,
			},
			e1: &contract.EgressConfig{
				Retry:         43,
				DeadLetter:    "e1",
				Timeout:       101,
				BackoffDelay:  4000,
				BackoffPolicy: 1,
			},
			expected: &contract.EgressConfig{
				DeadLetter:    "e0",
				Retry:         43,
				Timeout:       100,
				BackoffDelay:  4001,
				BackoffPolicy: 0,
			},
		},
		{
			name: "e0 retry priority (all config)",
			e0: &contract.EgressConfig{
				Retry:         42,
				DeadLetter:    "e0",
				Timeout:       100,
				BackoffDelay:  4001,
				BackoffPolicy: 0,
			},
			e1: &contract.EgressConfig{
				Retry:         43,
				DeadLetter:    "e1",
				Timeout:       101,
				BackoffDelay:  4000,
				BackoffPolicy: 1,
			},
			expected: &contract.EgressConfig{
				DeadLetter:    "e0",
				Retry:         42,
				Timeout:       100,
				BackoffDelay:  4001,
				BackoffPolicy: 0,
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			opts := []cmp.Option{
				cmpopts.IgnoreTypes(
					protoimpl.MessageState{},
					protoimpl.SizeCache(0),
					protoimpl.UnknownFields{},
				),
			}
			if diff := cmp.Diff(tc.expected, MergeEgressConfig(tc.e0, tc.e1), opts...); diff != "" {
				t.Errorf("(-want, +got) %s", diff)
			}
		})
	}
}

func TestContractEventPoliciesEventPolicies(t *testing.T) {

	tests := []struct {
		name                     string
		applyingPolicies         []string
		existingEventPolicies    []*eventingv1alpha1.EventPolicy
		namespace                string
		defaultAuthorizationMode feature.Flag
		expected                 []*contract.EventPolicy
		oidcDisabled             bool
	}{
		{
			name: "Exact match",
			applyingPolicies: []string{
				"policy-1",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-1",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						exactTokenMatcher("from-1"),
					},
				},
			},
		}, {
			name: "Prefix match",
			applyingPolicies: []string{
				"policy-1",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-*",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						prefixTokenMatcher("from-"),
					},
				},
			},
		}, {
			name: "Multiple policies",
			applyingPolicies: []string{
				"policy-1",
				"policy-2",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-1",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				}, {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-2",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-2-*",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						exactTokenMatcher("from-1"),
					},
				}, {
					TokenMatchers: []*contract.TokenMatcher{
						prefixTokenMatcher("from-2-"),
					},
				},
			},
		}, {
			name: "Multiple policies with filters",
			applyingPolicies: []string{
				"policy-1",
				"policy-2",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Spec: eventingv1alpha1.EventPolicySpec{
						Filters: []eventingv1.SubscriptionsAPIFilter{
							{
								CESQL: "true",
							},
						},
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-1",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				}, {
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-2",
						Namespace: "my-ns",
					},
					Spec: eventingv1alpha1.EventPolicySpec{
						Filters: []eventingv1.SubscriptionsAPIFilter{
							{
								CESQL: "false",
							},
						},
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-2-*",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionTrue,
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						exactTokenMatcher("from-1"),
					},
					Filters: []*contract.DialectedFilter{
						{
							Filter: &contract.DialectedFilter_Cesql{
								Cesql: &contract.CESQL{
									Expression: "true",
								},
							},
						},
					},
				}, {
					TokenMatchers: []*contract.TokenMatcher{
						prefixTokenMatcher("from-2-"),
					},
					Filters: []*contract.DialectedFilter{
						{
							Filter: &contract.DialectedFilter_Cesql{
								Cesql: &contract.CESQL{
									Expression: "false",
								},
							},
						},
					},
				},
			},
		}, {
			name:                     "No applying policies - allow-same-namespace default mode",
			applyingPolicies:         []string{},
			existingEventPolicies:    []*eventingv1alpha1.EventPolicy{},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationAllowSameNamespace,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						prefixTokenMatcher("system:serviceaccount:my-ns:"),
					},
				},
			},
		}, {
			name:                     "No applying policies - allow-all default mode",
			applyingPolicies:         []string{},
			existingEventPolicies:    []*eventingv1alpha1.EventPolicy{},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationAllowAll,
			expected: []*contract.EventPolicy{
				{
					TokenMatchers: []*contract.TokenMatcher{
						prefixTokenMatcher(""),
					},
				},
			},
		}, {
			name:                     "No applying policies - deny-all default mode",
			applyingPolicies:         []string{},
			existingEventPolicies:    []*eventingv1alpha1.EventPolicy{},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected:                 []*contract.EventPolicy{},
		}, {
			name: "Applying policy not ready",
			applyingPolicies: []string{
				"policy-1",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-*",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionFalse,
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationDenyAll,
			expected:                 []*contract.EventPolicy{},
		}, {
			name:         "No policy when OIDC is disabled",
			oidcDisabled: true,
			applyingPolicies: []string{
				"policy-1",
			},
			existingEventPolicies: []*eventingv1alpha1.EventPolicy{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "policy-1",
						Namespace: "my-ns",
					},
					Status: eventingv1alpha1.EventPolicyStatus{
						From: []string{
							"from-1",
						},
						Status: duckv1.Status{
							Conditions: duckv1.Conditions{
								{
									Type:   eventingv1alpha1.EventPolicyConditionReady,
									Status: corev1.ConditionFalse, // is false, as OIDC is disabled
								},
							},
						},
					},
				},
			},
			namespace:                "my-ns",
			defaultAuthorizationMode: feature.AuthorizationAllowSameNamespace,
			expected:                 []*contract.EventPolicy{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			features := feature.Flags{
				feature.AuthorizationDefaultMode: tt.defaultAuthorizationMode,
				feature.OIDCAuthentication:       feature.Enabled,
			}

			if tt.oidcDisabled {
				features[feature.OIDCAuthentication] = feature.Disabled
			}

			applyingPolicies := []*eventingv1alpha1.EventPolicy{}
			for _, applyingPolicyName := range tt.applyingPolicies {
				for _, existingPolicy := range tt.existingEventPolicies {
					if applyingPolicyName == existingPolicy.Name {
						applyingPolicies = append(applyingPolicies, existingPolicy)
					}
				}
			}

			got := ContractEventPoliciesFromEventPolicies(applyingPolicies, tt.namespace, features)
			expectedJSON, err := protojson.Marshal(&contract.Ingress{
				EventPolicies: tt.expected,
			})
			if err != nil {
				t.Fatal(err)
			}
			gotJSON, err := protojson.Marshal(&contract.Ingress{
				EventPolicies: got,
			})
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(expectedJSON, gotJSON); diff != "" {
				t.Errorf("(-want, +got) %s", diff)
			}
		})
	}
}

func exactTokenMatcher(sub string) *contract.TokenMatcher {
	return &contract.TokenMatcher{
		Matcher: &contract.TokenMatcher_Exact{
			Exact: &contract.Exact{
				Attributes: map[string]string{
					"sub": sub,
				},
			},
		},
	}
}

func prefixTokenMatcher(sub string) *contract.TokenMatcher {
	return &contract.TokenMatcher{
		Matcher: &contract.TokenMatcher_Prefix{
			Prefix: &contract.Prefix{
				Attributes: map[string]string{
					"sub": sub,
				},
			},
		},
	}
}

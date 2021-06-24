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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	fakedynamicclient "knative.dev/pkg/injection/clients/dynamicclient/fake"
	"knative.dev/pkg/resolver"

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
			backoffDelay: pointer.StringPtr("PT2S"),
			defaultDelay: 1000,
			want:         2000,
		},
		{
			name:         "happy case fractional",
			backoffDelay: pointer.StringPtr("PT0.2S"),
			defaultDelay: 1000,
			want:         200,
		},
		{
			name:         "happy case fractional - 2 decimals",
			backoffDelay: pointer.StringPtr("PT0.25S"),
			defaultDelay: 1000,
			want:         200, // The library we use don't support more than one decimal, it should be 250 though.
		},
		{
			name:         "happy case fractional minutes",
			backoffDelay: pointer.StringPtr("PT1M0.2S"),
			defaultDelay: 1000,
			want:         1*60*1000 + 200,
		},
		{
			name:         "milliseconds",
			backoffDelay: pointer.StringPtr("PT0.001S"),
			defaultDelay: 1000,
			want:         0,
		},
		{
			name:         "microseconds",
			backoffDelay: pointer.StringPtr("PT0.000001S"),
			defaultDelay: 1000,
			want:         0,
		},
		{
			name:         "65 seconds",
			backoffDelay: pointer.StringPtr("PT65S"),
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
			resolver:              resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
			parent:                &eventing.KafkaSink{},
			delivery:              nil,
			defaultBackoffDelayMs: 0,
			want:                  nil,
			wantErr:               false,
		},
		{
			name:                  "nil retry and nil dls",
			ctx:                   ctx,
			resolver:              resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
			parent:                &eventing.KafkaSink{},
			delivery:              &eventingduck.DeliverySpec{},
			defaultBackoffDelayMs: 0,
			want:                  nil,
			wantErr:               false,
		},
		{
			name:     "full delivery",
			ctx:      ctx,
			resolver: resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				DeadLetterSink: &duckv1.Destination{URI: url},
				Retry:          pointer.Int32Ptr(3),
				BackoffPolicy:  &exponential,
				BackoffDelay:   pointer.StringPtr("PT1S"),
				Timeout:        pointer.StringPtr("PT2S"),
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
			resolver: resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
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
			resolver: resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				Timeout: pointer.StringPtr("PT2S"),
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
			resolver: resolver.NewURIResolver(ctx, func(name types.NamespacedName) {}),
			parent:   &eventing.KafkaSink{},
			delivery: &eventingduck.DeliverySpec{
				Retry:         pointer.Int32Ptr(3),
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

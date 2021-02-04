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
	"math"
	"testing"

	"k8s.io/utils/pointer"
	duck "knative.dev/eventing/pkg/apis/duck/v1"

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
	linerar := duck.BackoffPolicyLinear
	exponential := duck.BackoffPolicyExponential
	wrong := duck.BackoffPolicyType("default")
	tests := []struct {
		name          string
		backoffPolicy *duck.BackoffPolicyType
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

func TestBackoffDelayFromString(t *testing.T) {

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
			got, err := BackoffDelayFromISO8601String(tt.backoffDelay, tt.defaultDelay)
			if (err != nil) != tt.wantError {
				t.Errorf("wantError = %v got %v", tt.wantError, err)
			}
			if got != tt.want {
				t.Errorf("BackoffDelayFromISO8601String() = %v, want %v", got, tt.want)
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

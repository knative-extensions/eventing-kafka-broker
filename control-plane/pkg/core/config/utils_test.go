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
		defaultDelay string
		want         string
	}{
		{
			name:         "happy case",
			backoffDelay: pointer.StringPtr("PT2S"),
			defaultDelay: "PT1S",
			want:         "PT2S",
		},
		{
			name:         "default",
			defaultDelay: "PT1S",
			want:         "PT1S",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BackoffDelayFromString(tt.backoffDelay, tt.defaultDelay); got != tt.want {
				t.Errorf("BackoffDelayFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

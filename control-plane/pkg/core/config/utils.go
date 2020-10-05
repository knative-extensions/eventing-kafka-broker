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
	"fmt"

	duck "knative.dev/eventing/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

// ContentModeFromString returns the ContentMode from the given string.
func ContentModeFromString(mode string) contract.ContentMode {
	switch mode {
	case eventing.ModeBinary:
		return contract.ContentMode_BINARY
	case eventing.ModeStructured:
		return contract.ContentMode_STRUCTURED
	default:
		panic(fmt.Errorf(
			"unknown content mode: %s - allowed: %v",
			mode,
			[]string{eventing.ModeStructured, eventing.ModeBinary},
		))
	}
}

// BackoffPolicyFromString returns the BackoffPolicy from the given string.
//
// Default value is contract.BackoffPolicy_Exponential.
func BackoffPolicyFromString(backoffPolicy *duck.BackoffPolicyType) contract.BackoffPolicy {
	if backoffPolicy == nil {
		return contract.BackoffPolicy_Exponential
	}

	bp := *backoffPolicy
	switch bp {
	case duck.BackoffPolicyLinear:
		return contract.BackoffPolicy_Linear
	case duck.BackoffPolicyExponential: // The linter complains for missing case in switch
		return contract.BackoffPolicy_Exponential
	default:
		return contract.BackoffPolicy_Exponential
	}
}

// BackoffDelayFromString returns the BackoffDelay from the given string.
//
// Default value is the specified defaultDelay.
func BackoffDelayFromString(backoffDelay *string, defaultDelay string) string {
	if backoffDelay == nil {
		return defaultDelay
	}

	return *backoffDelay
}

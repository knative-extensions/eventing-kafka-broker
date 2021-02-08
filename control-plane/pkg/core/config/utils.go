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
	"fmt"
	"math"

	"github.com/rickb777/date/period"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/resolver"

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

func EgressConfigFromDelivery(
	ctx context.Context,
	resolver *resolver.URIResolver,
	parent metav1.Object,
	delivery *duck.DeliverySpec,
	defaultBackoffDelayMs uint64,
) (*contract.EgressConfig, error) {

	if delivery == nil || (delivery.DeadLetterSink == nil && delivery.Retry == nil) {
		return nil, nil
	}

	egressConfig := &contract.EgressConfig{}

	if delivery.DeadLetterSink != nil {

		deadLetterSinkURL, err := resolver.URIFromDestinationV1(ctx, *delivery.DeadLetterSink, parent)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve broker.Spec.Deliver.DeadLetterSink: %w", err)
		}

		egressConfig.DeadLetter = deadLetterSinkURL.String()
	}

	if delivery.Retry != nil {
		egressConfig.Retry = uint32(*delivery.Retry)
		var err error
		delay, err := BackoffDelayFromISO8601String(delivery.BackoffDelay, defaultBackoffDelayMs)
		if err != nil {
			return nil, fmt.Errorf("failed to parse backoff delay: %w", err)
		}
		egressConfig.BackoffDelay = delay
		egressConfig.BackoffPolicy = BackoffPolicyFromString(delivery.BackoffPolicy)
	}

	return egressConfig, nil
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

// BackoffDelayFromISO8601String returns the BackoffDelay from the given string.
//
// Default value is the specified defaultDelay.
func BackoffDelayFromISO8601String(backoffDelay *string, defaultDelay uint64) (uint64, error) {
	if backoffDelay == nil {
		return defaultDelay, nil
	}

	d, err := period.Parse(*backoffDelay, false)
	if err != nil {
		return 0, fmt.Errorf("failed to parse backoffDelay: %w", err)
	}

	ms, _ := d.Duration()

	return uint64(math.Abs(float64(ms.Milliseconds()))), nil
}

// Increment contract.Contract.Generation.
func IncrementContractGeneration(ct *contract.Contract) {
	ct.Generation = (ct.Generation + 1) % (math.MaxUint64 - 1)
}

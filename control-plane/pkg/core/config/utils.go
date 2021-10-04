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

	if delivery == nil || (delivery.DeadLetterSink == nil && delivery.Retry == nil && delivery.Timeout == nil) {
		return nil, nil
	}

	egressConfig := &contract.EgressConfig{}

	if delivery.DeadLetterSink != nil {
		destination := *delivery.DeadLetterSink // Do not update object Spec, so copy destination.
		if destination.Ref != nil && destination.Ref.Namespace == "" {
			destination.Ref.Namespace = parent.GetNamespace()
		}
		deadLetterSinkURL, err := resolver.URIFromDestinationV1(ctx, *delivery.DeadLetterSink, parent)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve Spec.Delivery.DeadLetterSink: %w", err)
		}
		egressConfig.DeadLetter = deadLetterSinkURL.String()
	}

	if delivery.Retry != nil {
		egressConfig.Retry = uint32(*delivery.Retry)
		var err error
		delay, err := DurationMillisFromISO8601String(delivery.BackoffDelay, defaultBackoffDelayMs)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Spec.Delivery.BackoffDelay: %w", err)
		}
		egressConfig.BackoffDelay = delay
		egressConfig.BackoffPolicy = BackoffPolicyFromString(delivery.BackoffPolicy)
	}

	if delivery.Timeout != nil {
		var err error
		timeout, err := DurationMillisFromISO8601String(
			delivery.Timeout,
			0, /* 0 means absent, allowing the data plane to default it */
		)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Spec.Delivery.Timeout: %w", err)
		}
		egressConfig.Timeout = timeout
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

// DurationMillisFromISO8601String returns the duration in milliseconds from the given string.
//
// Default value is the specified defaultDelay.
func DurationMillisFromISO8601String(durationStr *string, defaultDurationMillis uint64) (uint64, error) {
	if durationStr == nil {
		return defaultDurationMillis, nil
	}

	d, err := period.Parse(*durationStr, false)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration string: %w", err)
	}

	ms, _ := d.Duration()

	return uint64(math.Abs(float64(ms.Milliseconds()))), nil
}

// Increment contract.Contract.Generation.
func IncrementContractGeneration(ct *contract.Contract) {
	ct.Generation = (ct.Generation + 1) % (math.MaxUint64 - 1)
}

// MergeEgressConfig merges the 2 given egress configs into one egress config prioritizing e0 values.
func MergeEgressConfig(e0, e1 *contract.EgressConfig) *contract.EgressConfig {
	if e0 == nil {
		return e1
	}
	if e1 == nil {
		return e0
	}
	return &contract.EgressConfig{
		DeadLetter:    mergeString(e0.GetDeadLetter(), e1.GetDeadLetter()),
		Retry:         mergeUint32(e0.GetRetry(), e1.GetRetry()),
		BackoffPolicy: e0.GetBackoffPolicy(),
		BackoffDelay:  mergeUint64(e0.GetBackoffDelay(), e1.GetBackoffDelay()),
		Timeout:       mergeUint64(e0.GetTimeout(), e1.GetTimeout()),
	}
}

func mergeUint64(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	return a
}

func mergeUint32(a, b uint32) uint32 {
	if a == 0 {
		return b
	}
	return a
}

func mergeString(a, b string) string {
	if a == "" {
		return b
	}
	return a
}

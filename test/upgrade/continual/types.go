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

package continual

import (
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
	"knative.dev/eventing/test/upgrade/prober/wathola/event"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"

	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

const (
	defaultRetryCount    = 12
	defaultBackoffPolicy = eventingduckv1.BackoffPolicyExponential
	defaultBackoffDelay  = "PT1S"
)

func defaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		RetryCount:    defaultRetryCount,
		BackoffPolicy: defaultBackoffPolicy,
		BackoffDelay:  defaultBackoffDelay,
	}
}

func defaultReplicationOptions() *ReplicationOptions {
	return &ReplicationOptions{
		NumPartitions:     6,
		ReplicationFactor: 3,
	}
}

// TestOptions holds options for EventingKafka continual tests.
type TestOptions struct {
	prober.ContinualVerificationOptions
	SUTs map[string]sut.SystemUnderTest
}

type Triggers struct {
	Prefix string
	sut.Triggers
}

type Broker struct {
	Name  string
	Class string
	*ReplicationOptions
	*RetryOptions
}

type Sink struct {
	Name string
	Spec eventing.KafkaSinkSpec
}

type Source struct {
	Name string
	Spec sources.KafkaSourceSpec
}

var eventTypes = []string{
	event.Step{}.Type(),
	event.Finished{}.Type(),
}

// ReplicationOptions hold options for replication.
type ReplicationOptions struct {
	NumPartitions     int
	ReplicationFactor int
}

// RetryOptions holds options for retries.
type RetryOptions struct {
	RetryCount    int
	BackoffPolicy eventingduckv1.BackoffPolicyType
	BackoffDelay  string
}

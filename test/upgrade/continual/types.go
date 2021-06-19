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
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
)

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

// KafkaCluster represents Kafka cluster endpoint.
type KafkaCluster struct {
	BootstrapServers []string
	Name             string
	Namespace        string
}

// TestOptions holds options for EventingKafka continual tests.
type TestOptions struct {
	prober.ContinualVerificationOptions
	SUTs map[string]sut.SystemUnderTest
}

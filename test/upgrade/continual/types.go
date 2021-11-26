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
	eventingkafkaupgrade "knative.dev/eventing-kafka/test/upgrade/continual"
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
	"knative.dev/eventing/test/upgrade/prober/wathola/event"
)

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
	Name string
	*eventingkafkaupgrade.ReplicationOptions
	*eventingkafkaupgrade.RetryOptions
}

var eventTypes = []string{
	event.Step{}.Type(),
	event.Finished{}.Type(),
}

//go:build e2e
// +build e2e

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

package conformance

import (
	"testing"

	conformance "knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func brokerCreator(class string) func(client *testlib.Client, name string) {
	return func(client *testlib.Client, name string) {
		broker := client.CreateBrokerOrFail(name,
			resources.WithBrokerClassForBroker(class),
		)

		client.WaitForResourceReadyOrFail(broker.Name, testlib.BrokerTypeMeta)
	}
}

func TestBrokerControlPlane(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {
		conformance.BrokerV1ControlPlaneTest(t, brokerCreator(kafka.BrokerClass))
	})
}

func TestNamespacedBrokerControlPlane(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {
		conformance.BrokerV1ControlPlaneTest(t, brokerCreator(kafka.NamespacedBrokerClass))
	})
}

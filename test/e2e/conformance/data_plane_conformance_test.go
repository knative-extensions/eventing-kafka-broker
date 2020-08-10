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

	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	conformance "knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestBrokerIngressV1Beta1(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		broker := createBroker(client)

		conformance.BrokerV1Beta1IngressDataPlaneTestHelper(t, client, broker)
	})
}

func TestBrokerConsumerV1Beta1(t *testing.T) {
	// TODO re-enable this test
	t.Skip("This scenario is already covered by TestEventTransformationForTrigger*")

	pkgtesting.RunMultiple(t, func(t *testing.T) {

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		broker := createBroker(client)

		conformance.BrokerV1Beta1ConsumerDataPlaneTestHelper(t, client, broker)
	})
}

func createBroker(client *testlib.Client) *eventing.Broker {

	broker := client.CreateBrokerV1Beta1OrFail("broker",
		resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
	)

	client.WaitForResourceReadyOrFail(broker.Name, testlib.BrokerTypeMeta)
	return broker
}

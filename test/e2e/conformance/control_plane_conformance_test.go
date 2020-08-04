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

	"knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

func brokerCreator(client *testlib.Client, name string) {

	broker := client.CreateBrokerV1Beta1OrFail(name,
		resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
	)

	client.WaitForResourceReadyOrFail(broker.Name, testlib.BrokerTypeMeta)
}

func TestBrokerV1Beta1ControlPlane(t *testing.T) {
	helpers.BrokerV1Beta1ControlPlaneTest(t, brokerCreator)
}

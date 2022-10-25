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

	eventing "knative.dev/eventing/pkg/apis/eventing/v1"

	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg"
	brokertest "knative.dev/eventing-kafka-broker/test/pkg/broker"
	conformance "knative.dev/eventing/test/conformance/helpers"
	testlib "knative.dev/eventing/test/lib"
)

func brokerCreator(client *testlib.Client, name string) {
	brokertest.CreatorWithBrokerOptions(
		client,
		"v1",
		brokertest.WithBrokerClassFromEnvVar,
		func(b *eventing.Broker) {
			b.Name = name
		},
	)

	client.WaitForResourceReadyOrFail(name, testlib.BrokerTypeMeta)
}

func TestBrokerControlPlane(t *testing.T) {
	pkgtesting.RunMultiple(t, func(t *testing.T) {
		conformance.BrokerV1ControlPlaneTest(t, brokerCreator)
	})
}

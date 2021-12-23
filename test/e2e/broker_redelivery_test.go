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

package e2e

import (
	"context"
	"testing"

	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/e2e/helpers"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	brokerName = "broker"
)

func TestBrokerRedeliveryBrokerV1BackoffLinear(t *testing.T) {
	t.Skip()

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduck.BackoffPolicyLinear

			client.CreateBrokerOrFail(brokerName,
				resources.WithBrokerClassForBroker(kafka.BrokerClass),
				resources.WithDeliveryForBroker(&eventingduck.DeliverySpec{
					Retry:         &numRetries,
					BackoffPolicy: &backoff,
					BackoffDelay:  pointer.StringPtr("PT0.2S"),
				}),
			)

			return brokerName
		})
	})
}

func TestBrokerRedeliveryBrokerV1BackoffExponential(t *testing.T) {
	t.Skip()

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduck.BackoffPolicyExponential

			client.CreateBrokerOrFail(brokerName,
				resources.WithBrokerClassForBroker(kafka.BrokerClass),
				resources.WithDeliveryForBroker(&eventingduck.DeliverySpec{
					Retry:         &numRetries,
					BackoffPolicy: &backoff,
					BackoffDelay:  pointer.StringPtr("PT0.2S"),
				}),
			)

			return brokerName
		})
	})
}

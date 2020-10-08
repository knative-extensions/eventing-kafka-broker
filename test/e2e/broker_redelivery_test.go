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
	eventingduckv1beta1 "knative.dev/eventing/pkg/apis/duck/v1beta1"
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

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduck.BackoffPolicyLinear

			client.CreateBrokerV1OrFail(brokerName,
				resources.WithBrokerClassForBrokerV1(kafka.BrokerClass),
				resources.WithDeliveryForBrokerV1(&eventingduck.DeliverySpec{
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

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduck.BackoffPolicyExponential

			client.CreateBrokerV1OrFail(brokerName,
				resources.WithBrokerClassForBrokerV1(kafka.BrokerClass),
				resources.WithDeliveryForBrokerV1(&eventingduck.DeliverySpec{
					Retry:         &numRetries,
					BackoffPolicy: &backoff,
					BackoffDelay:  pointer.StringPtr("PT0.2S"),
				}),
			)

			return brokerName
		})
	})
}

func TestBrokerRedeliveryBrokerV1Beta1BackoffLinear(t *testing.T) {

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduckv1beta1.BackoffPolicyLinear

			client.CreateBrokerV1Beta1OrFail(brokerName,
				resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
				resources.WithDeliveryForBrokerV1Beta1(&eventingduckv1beta1.DeliverySpec{
					Retry:         &numRetries,
					BackoffPolicy: &backoff,
					BackoffDelay:  pointer.StringPtr("PT0.2S"),
				}),
			)

			return brokerName
		})
	})
}

func TestBrokerRedeliveryBrokerV1Beta1BackoffExponential(t *testing.T) {

	kafkatesting.RunMultiple(t, func(t *testing.T) {

		helpers.BrokerRedelivery(context.Background(), t, func(client *testlib.Client, numRetries int32) string {

			backoff := eventingduckv1beta1.BackoffPolicyExponential

			client.CreateBrokerV1Beta1OrFail(brokerName,
				resources.WithBrokerClassForBrokerV1Beta1(kafka.BrokerClass),
				resources.WithDeliveryForBrokerV1Beta1(&eventingduckv1beta1.DeliverySpec{
					Retry:         &numRetries,
					BackoffPolicy: &backoff,
					BackoffDelay:  pointer.StringPtr("PT0.2S"),
				}),
			)

			return brokerName
		})
	})
}

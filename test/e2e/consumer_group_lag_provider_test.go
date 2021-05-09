// +build e2e

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

package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	testlib "knative.dev/eventing/test/lib"

	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestConsumerGroupLagProvider(t *testing.T) {
	t.Skip("Only useful for debugging the consumer group lag provider control-plane/pkg/kafka/consumer_group_lag.go")

	const (
		consumerGroupLagJobName = "consumer-group-lag-provider-test"
	)

	client := testlib.Setup(t, false)
	defer testlib.TearDown(client)

	ctx := context.Background()

	err := kafkatest.VerifyConsumerGroupLag(
		client.Kube,
		client.Tracker,
		types.NamespacedName{
			Namespace: client.Namespace,
			Name:      consumerGroupLagJobName,
		},
	)

	pkgtesting.LogJobOutput(t, ctx, client.Kube, client.Namespace, consumerGroupLagJobName)

	require.Nil(t, err)
}

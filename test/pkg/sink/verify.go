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

package sink

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	testlib "knative.dev/eventing/test/lib"

	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func Verify(t *testing.T, client *testlib.Client, mode, topic string, ids []string) {

	err := kafkatest.VerifyMessagesInTopic(
		client.Kube,
		client.Tracker,
		types.NamespacedName{
			Namespace: client.Namespace,
			Name:      names.SimpleNameGenerator.GenerateName("verify-messages"),
		},
		&kafkatest.ConsumerConfig{
			BootstrapServers: testingpkg.BootstrapServersPlaintext,
			Topic:            topic,
			IDS:              strings.Join(ids, ","),
			ContentMode:      mode,
		},
	)
	assert.Nil(t, err, "failed to verify messages in topic: %v - (see pod logs)", err)
}

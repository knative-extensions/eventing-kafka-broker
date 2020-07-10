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

package broker_test // different package name due to import cycles. (broker -> testing -> broker)

import (
	"testing"

	"github.com/Shopify/sarama"
	"gotest.tools/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/broker"
	reconcilertesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
)

func TestCreateTopicTopicAlreadyExists(t *testing.T) {

	b := &eventing.Broker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bname",
			Namespace: "bnamespace",
		},
	}
	topic := broker.Topic(b)
	errMsg := "topic already exists"

	r := broker.Reconciler{
		KafkaClusterAdmin: reconcilertesting.MockKafkaClusterAdmin{
			ExpectedTopicName:   topic,
			ExpectedTopicDetail: sarama.TopicDetail{},
			ErrorOnCreateTopic: &sarama.TopicError{
				Err:    sarama.ErrTopicAlreadyExists,
				ErrMsg: &errMsg,
			},
			T: t,
		},
	}

	topicRet, err := r.CreateTopic(b)

	assert.Equal(t, topicRet, topic, "expected topic %s go %s", topic, topicRet)
	assert.NilError(t, err, "expected nil error on topic already exists")
}

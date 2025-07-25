/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package helpers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/storage/names"
	"knative.dev/eventing-kafka-broker/test/pkg/kafka"

	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
	contribtestlib "knative.dev/eventing-kafka-broker/test/lib"
	contribresources "knative.dev/eventing-kafka-broker/test/lib/resources"
)

const (
	KafkaBootstrapUrlPlain   = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	KafkaClusterName         = "my-cluster"
	KafkaClusterNamespace    = "kafka"
	verifyCommittedOffsetJob = "verify-committed"
)

// SourceTestScope returns true if we should proceed with given
// test case.
type SourceTestScope func(auth, testCase, version string) bool

// AssureKafkaSourceConsumesMsgNoEvent assures that KafkaSource reads messages
// that were not cloud events.
func AssureKafkaSourceConsumesMsgNoEvent(t *testing.T) {
	tests := map[string]struct {
		messageKey     string
		messageCount   int
		messageHeaders map[string]string
		transactional  bool
		messagePayload string
		matcherGen     func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher
		extensions     map[string]string
	}{
		"no_event": {
			messageKey:   "0",
			messageCount: 1,
			messageHeaders: map[string]string{
				"content-type": "application/json",
			},
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasDataContentType("application/json"),
					HasData([]byte(`{"value":5}`)),
					HasExtension("key", "0"),
				)
			},
		},
		"no_event_no_content_type": {
			messageKey:     "0",
			messageCount:   1,
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasData([]byte(`{"value":5}`)),
					HasExtension("key", "0"),
				)
			},
		},
		"no_event_content_type_or_key": {
			messageCount:   1,
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasData([]byte(`{"value":5}`)),
				)
			},
		},
		"no_event_with_text_plain_body": {
			messageKey:   "0",
			messageCount: 1,
			messageHeaders: map[string]string{
				"content-type": "text/plain",
			},
			messagePayload: "simple 10",
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasDataContentType("text/plain"),
					HasData([]byte("simple 10")),
					HasExtension("key", "0"),
				)
			},
		},
		"no_event_with_transactional_id": {
			messageKey:    "0",
			messageCount:  10,
			transactional: true,
			messageHeaders: map[string]string{
				"content-type": "application/json",
			},
			messagePayload: `{"value":5}`,
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSource(cloudEventsSourceName),
					HasType(cloudEventsEventType),
					HasDataContentType("application/json"),
					HasData([]byte(`{"value":5}`)),
					HasExtension("key", "0"),
				)
			},
		},
	}

	for name, test := range tests {
		test := test
		name := name
		for _, version := range []string{"v1"} {
			testName := name + "-" + version
			t.Run(testName, func(t *testing.T) {
				testKafkaSource(t, name, version, test.messageKey, test.messageCount, test.messageHeaders, test.transactional, test.messagePayload, test.matcherGen, KafkaBootstrapUrlPlain, test.extensions)
			})
		}
	}
}

func testKafkaSource(t *testing.T,
	name string, version string, messageKey string, messageCount int, messageHeaders map[string]string, transactional bool, messagePayload string,
	matcherGen func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher,
	bootStrapServer string, extensions map[string]string) {

	name = fmt.Sprintf("%s-%s", name, version)

	var (
		kafkaTopicName     = uuid.New().String()
		consumerGroup      = uuid.New().String()
		recordEventPodName = "e2e-kafka-r-" + strings.ReplaceAll(name, "_", "-")
		kafkaSourceName    = "e2e-kafka-source-" + strings.ReplaceAll(name, "_", "-")
	)

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	MustCreateTopic(client, KafkaClusterName, KafkaClusterNamespace, kafkaTopicName, 10)
	if len(recordEventPodName) > 63 {
		recordEventPodName = recordEventPodName[:63]
	}
	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, recordEventPodName)

	var (
		cloudEventsSourceName string
		cloudEventsEventType  string
		transactionalId       string
		expectedLag           uint64
	)

	t.Logf("Creating KafkaSource %s", version)
	switch version {
	case "v1":
		contribtestlib.CreateKafkaSourceOrFail(client, contribresources.KafkaSource(
			bootStrapServer,
			kafkaTopicName,
			sources.Ordered,
			resources.ServiceRef(recordEventPodName),
			contribresources.WithName(kafkaSourceName),
			contribresources.WithConsumerGroup(consumerGroup),
			contribresources.WithExtensions(extensions),
		))
		cloudEventsSourceName = sources.KafkaEventSource(client.Namespace, kafkaSourceName, kafkaTopicName)
		cloudEventsEventType = sources.KafkaEventType
	default:
		t.Fatalf("Unknown KafkaSource version %s", version)
	}

	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	if transactional {
		transactionalId = kafkaTopicName
	}

	for range messageCount {
		MustPublishKafkaMessage(client, KafkaBootstrapUrlPlain, kafkaTopicName, messageKey, messageHeaders, transactionalId, messagePayload)
	}

	eventTracker.AssertExact(messageCount, recordevents.MatchEvent(matcherGen(cloudEventsSourceName, cloudEventsEventType)))

	// With transactions, the last event is a transaction control record, which will not be consumed, so the expected lag is 1
	if transactional {
		expectedLag = 1
	}

	err := kafka.VerifyLagEquals(client.Kube, client.Tracker, types.NamespacedName{
		Namespace: client.Namespace,
		Name:      names.SimpleNameGenerator.GenerateName(verifyCommittedOffsetJob + "-" + kafkaTopicName),
	}, &kafka.AdminConfig{
		BootstrapServers: bootStrapServer,
		Topic:            kafkaTopicName,
		Group:            consumerGroup,
	}, expectedLag)
	require.Nil(t, err, "Failed to verify committed offset")
}

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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/cloudevents/sdk-go/v2/test"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/stretchr/testify/require"
	"knative.dev/eventing/pkg/utils"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
)

func testKafkaSourceUpdate(t *testing.T, name string, test updateTest) {

	messagePayload := []byte(`{"value":5}`)
	matcherGen := func(cloudEventsSourceName, originalOrUpdate string) EventMatcher {
		return AllOf(
			HasSource(cloudEventsSourceName),
			HasData(messagePayload),
			HasExtension("kafkaheaderceoriginalorupdate", originalOrUpdate))
	}

	originalMessage := message{
		payload: messagePayload,
		headers: map[string]string{
			"ce_originalorupdate": "original",
		},
		key: "0",
	}
	updateMessage := message{
		payload: messagePayload,
		headers: map[string]string{
			"ce_originalorupdate": "update",
		},
		key: "0",
	}

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	t.Logf("Creating topic: %s\n", defaultKafkaSource.topicName+name)
	MustCreateTopic(client, KafkaClusterName, KafkaClusterNamespace, defaultKafkaSource.topicName+name, 10)

	t.Logf("Copying secrets: %s\n", defaultKafkaSource.topicName+name)
	_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaTLSSecret, client.Namespace, "default")
	if err != nil {
		t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
	}

	t.Logf("Creating default eventrecorder pod: %s\n", defaultKafkaSource.sinkName)
	originalEventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, defaultKafkaSource.sinkName)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	kafkaSourceName := "e2e-kafka-source-" + name

	t.Logf("Creating kafkasource: %s\n", kafkaSourceName)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
		defaultKafkaSource.auth.bootStrapServer,
		defaultKafkaSource.topicName+name,
		resources.ServiceRef(defaultKafkaSource.sinkName),
		contribresources.WithNameV1Beta1(kafkaSourceName),
		withAuthEnablementV1Beta1(defaultKafkaSource.auth),
	))
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	t.Logf("Send update event to kafkatopic")
	MustPublishKafkaMessage(client, KafkaBootstrapUrlPlain,
		defaultKafkaSource.topicName+name,
		originalMessage.key, originalMessage.headers, string(originalMessage.payload))
	eventSourceName := sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, defaultKafkaSource.topicName+name)
	originalEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "original")))
	t.Logf("Properly received original event for %s\n", eventSourceName)

	ksObj := waitForKafkaSourceReconcilerToReconcileSource(t, client, kafkaSourceName)

	if test.topicName != defaultKafkaSource.topicName {
		MustCreateTopic(client, KafkaClusterName, KafkaClusterNamespace, test.topicName+name, 10)
		ksObj.Spec.Topics = []string{test.topicName + name}
		eventSourceName = sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, test.topicName+name)
	}

	var newSinkEventTracker *recordevents.EventInfoStore
	if test.sinkName != defaultKafkaSource.sinkName {
		kSinkRef := resources.ServiceKRef(test.sinkName)
		ksObj.Spec.Sink.Ref = kSinkRef

		newSinkEventTracker, _ = recordevents.StartEventRecordOrFail(context.Background(), client, test.sinkName)
	}
	if test.auth.bootStrapServer != defaultKafkaSource.auth.bootStrapServer {
		ksObj.Spec.KafkaAuthSpec.BootstrapServers = []string{test.auth.bootStrapServer}
		ksObj.Spec.KafkaAuthSpec.Net.TLS.Enable = test.auth.TLSEnabled
		ksObj.Spec.KafkaAuthSpec.Net.SASL.Enable = test.auth.SASLEnabled
	}

	contribtestlib.UpdateKafkaSourceV1Beta1OrFail(client, ksObj)
	waitForKafkaSourceReconcilerToReconcileSource(t, client, kafkaSourceName)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	t.Logf("Send update event to kafkatopic")
	MustPublishKafkaMessage(client, KafkaBootstrapUrlPlain,
		test.topicName+name,
		updateMessage.key, updateMessage.headers, string(updateMessage.payload))

	if test.sinkName != defaultKafkaSource.sinkName {
		newSinkEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "update")))
	} else {
		originalEventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(eventSourceName, "update")))
	}

}

func waitForKafkaSourceReconcilerToReconcileSource(t *testing.T, client *testlib.Client, kafkaSourceName string) *sourcesv1beta1.KafkaSource {
	var ksObj *sourcesv1beta1.KafkaSource
	err := wait.Poll(10*time.Second, 4*time.Minute, func() (done bool, err error) {
		ksObj = contribtestlib.GetKafkaSourceV1Beta1OrFail(client, kafkaSourceName)
		if ksObj == nil {
			t.Fatalf("Unabled to Get kafkasource: %s/%s\n", client.Namespace, kafkaSourceName)
		}
		return ksObj.Status.IsReady() && ksObj.Status.ObservedGeneration == ksObj.Generation, nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for KafkaSource reconciler to reconcile KafkaSource: %v", ksObj)
	}
	return ksObj
}

type message struct {
	payload []byte
	headers map[string]string
	key     string
}

type updateTest struct {
	auth      authSetup
	topicName string
	sinkName  string
}

var (
	defaultKafkaSource = updateTest{
		auth: authSetup{
			bootStrapServer: kafkaBootstrapUrlTLS,
			SASLEnabled:     false,
			TLSEnabled:      true,
		},
		topicName: "initial-topic",
		sinkName:  "default-event-recorder",
	}
)

func TestKafkaSourceClaims(t *testing.T) {

	topic := defaultKafkaSource.topicName + "-test-claims"
	sink := defaultKafkaSource.sinkName

	messageHeaders := map[string]string{
		"content-type": "application/cloudevents+json",
	}
	messagePayload := mustJsonMarshal(t, map[string]interface{}{
		"specversion":     "1.0",
		"type":            "com.github.pull.create",
		"source":          "https://github.com/cloudevents/spec/pull",
		"subject":         "123",
		"id":              "A234-1234-1234",
		"time":            "2018-04-05T17:31:00Z",
		"datacontenttype": "application/json",
		"data": map[string]string{
			"hello": "Francesco",
		},
		"comexampleextension1": "value",
		"comexampleothervalue": 5,
	})

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	t.Logf("Creating topic: %s\n", topic)
	MustCreateTopic(client, KafkaClusterName, KafkaClusterNamespace, topic, 10)

	t.Logf("Creating default eventrecorder pod: %s\n", sink)
	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, sink)
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	kafkaSourceName := "e2e-kafka-source-test-claims"

	t.Logf("Creating kafkasource: %s\n", kafkaSourceName)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
		KafkaBootstrapUrlPlain,
		topic,
		resources.ServiceRef(sink),
		contribresources.WithNameV1Beta1(kafkaSourceName),
	))
	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	t.Logf("Send update event to kafkatopic")
	MustPublishKafkaMessage(
		client,
		KafkaBootstrapUrlPlain,
		topic,
		"0",
		messageHeaders,
		messagePayload,
	)

	eventTracker.AssertExact(1, recordevents.MatchEvent(test.HasType("com.github.pull.create")))

	ksObj := contribtestlib.GetKafkaSourceV1Beta1OrFail(client, kafkaSourceName)
	if ksObj == nil {
		t.Fatalf("Unabled to Get kafkasource: %s/%s\n", client.Namespace, kafkaSourceName)
	}

	require.NotEmpty(t, ksObj.Status.Claims)
	t.Logf("Claims value: %s", ksObj.Status.Claims)
}

func TestKafkaSourceUpdate(t *testing.T) {

	testCases := map[string]updateTest{
		"no-change": defaultKafkaSource,
		"change-sink": {
			auth:      defaultKafkaSource.auth,
			topicName: defaultKafkaSource.topicName,
			sinkName:  "update-event-recorder",
		},
		// "change-topic": {
		// 	auth:      defaultKafkaSource.auth,
		// 	topicName: "update-topic",
		// 	sinkName:  defaultKafkaSource.sinkName,
		// },
		"change-bootstrap-server": {
			auth: authSetup{
				bootStrapServer: KafkaBootstrapUrlPlain,
				TLSEnabled:      false,
				SASLEnabled:     false,
			},
			topicName: defaultKafkaSource.topicName,
			sinkName:  defaultKafkaSource.sinkName,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			testKafkaSourceUpdate(t, name, tc)
		})
	}
}

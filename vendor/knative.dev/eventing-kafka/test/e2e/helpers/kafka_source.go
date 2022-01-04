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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	sourcesv1beta1 "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"

	"knative.dev/eventing/pkg/utils"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"

	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
)

const (
	KafkaBootstrapUrlPlain = "my-cluster-kafka-bootstrap.kafka.svc:9092"
	kafkaBootstrapUrlTLS   = "my-cluster-kafka-bootstrap.kafka.svc:9093"
	kafkaBootstrapUrlSASL  = "my-cluster-kafka-bootstrap.kafka.svc:9094"

	KafkaClusterName      = "my-cluster"
	KafkaClusterNamespace = "kafka"

	kafkaSASLSecret = "strimzi-sasl-secret"
	kafkaTLSSecret  = "strimzi-tls-secret"
)

// SourceTestScope returns true if we should proceed with given
// test case.
type SourceTestScope func(auth, testCase, version string) bool

// AssureKafkaSourceIsOperational assures that KafkaSource works as intended.
func AssureKafkaSourceIsOperational(t *testing.T, scope SourceTestScope) {
	eventTime, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")

	auths := map[string]struct {
		auth authSetup
	}{
		"plain": {
			auth: authSetup{
				bootStrapServer: KafkaBootstrapUrlPlain,
				SASLEnabled:     false,
				TLSEnabled:      false,
			},
		},
		"s512": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlSASL,
				SASLEnabled:     true,
				TLSEnabled:      false,
			},
		},
		"tls": {
			auth: authSetup{
				bootStrapServer: kafkaBootstrapUrlTLS,
				SASLEnabled:     false,
				TLSEnabled:      true,
			},
		},
	}
	tests := map[string]struct {
		messageKey     string
		messageHeaders map[string]string
		messagePayload string
		matcherGen     func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher
		extensions     map[string]string
	}{
		"no_event": {
			messageKey: "0",
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
			messageKey: "0",
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
		"structured": {
			messageHeaders: map[string]string{
				"content-type": "application/cloudevents+json",
			},
			messagePayload: mustJsonMarshal(t, map[string]interface{}{
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
			}),
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSpecVersion(cloudevents.VersionV1),
					HasType("com.github.pull.create"),
					HasSource("https://github.com/cloudevents/spec/pull"),
					HasSubject("123"),
					HasId("A234-1234-1234"),
					HasTime(eventTime),
					HasDataContentType("application/json"),
					HasData([]byte(`{"hello":"Francesco"}`)),
					HasExtension("comexampleextension1", "value"),
					HasExtension("comexampleothervalue", "5"),
				)
			},
		},
		"binary": {
			messageHeaders: map[string]string{
				"ce_specversion":          "1.0",
				"ce_type":                 "com.github.pull.create",
				"ce_source":               "https://github.com/cloudevents/spec/pull",
				"ce_subject":              "123",
				"ce_id":                   "A234-1234-1234",
				"ce_time":                 "2018-04-05T17:31:00Z",
				"content-type":            "application/json",
				"ce_comexampleextension1": "value",
				"ce_comexampleothervalue": "5",
			},
			messagePayload: mustJsonMarshal(t, map[string]string{
				"hello": "Francesco",
			}),
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSpecVersion(cloudevents.VersionV1),
					HasType("com.github.pull.create"),
					HasSource("https://github.com/cloudevents/spec/pull"),
					HasSubject("123"),
					HasId("A234-1234-1234"),
					HasTime(eventTime),
					HasDataContentType("application/json"),
					HasData([]byte(`{"hello":"Francesco"}`)),
					HasExtension("comexampleextension1", "value"),
					HasExtension("comexampleothervalue", "5"),
				)
			},
		},
		"with_extensions": {
			messageHeaders: map[string]string{
				"content-type": "application/cloudevents+json",
			},
			messagePayload: mustJsonMarshal(t, map[string]interface{}{
				"specversion": "1.0",
				"type":        "com.github.pull.create",
				"source":      "https://github.com/cloudevents/spec/pull",
				"id":          "A234-1234-1234",
			}),
			extensions: map[string]string{
				"comexampleextension1": "value",
				"comexampleothervalue": "5",
			},
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSpecVersion(cloudevents.VersionV1),
					HasType("com.github.pull.create"),
					HasSource("https://github.com/cloudevents/spec/pull"),
					HasExtension("comexampleextension1", "value"),
					HasExtension("comexampleothervalue", "5"),
				)
			},
		},
		"with_overrides": {
			messageHeaders: map[string]string{
				"content-type": "application/cloudevents+json",
			},
			messagePayload: mustJsonMarshal(t, map[string]interface{}{
				"specversion": "1.0",
				"type":        "com.github.pull.create",
				"source":      "https://github.com/cloudevents/spec/pull",
				"id":          "A234-1234-1234",
			}),
			extensions: map[string]string{
				"comexampleothervalue": "5",
			},
			matcherGen: func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher {
				return AllOf(
					HasSpecVersion(cloudevents.VersionV1),
					HasSource("https://github.com/cloudevents/spec/pull"),
					HasExtension("comexampleothervalue", "5"),
				)
			},
		},
	}
	for authName, auth := range auths {
		for testcase, test := range tests {
			test := test
			name := testcase + "_" + authName
			for _, version := range []string{"v1beta1"} {
				testName := name + "-" + version
				if !scope(authName, testcase, version) {
					t.Log("Skipping the test case, because it's out of configured scope: ", testName)
					continue
				}
				t.Run(testName, func(t *testing.T) {
					testKafkaSource(t, name, version, test.messageKey, test.messageHeaders, test.messagePayload, test.matcherGen, auth.auth, test.extensions)
				})
			}
		}
	}
}

type authSetup struct {
	bootStrapServer string
	SASLEnabled     bool
	TLSEnabled      bool
}

func withAuthEnablementV1Beta1(auth authSetup) contribresources.KafkaSourceV1Beta1Option {
	// We test with sasl512 and enable tls with it, so check tls first
	if auth.TLSEnabled {
		return func(ks *sourcesv1beta1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "ca.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Cert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.crt",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Key.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaTLSSecret,
				},
				Key: "user.key",
			}
		}
	}
	if auth.SASLEnabled {
		return func(ks *sourcesv1beta1.KafkaSource) {
			ks.Spec.KafkaAuthSpec.Net.SASL.Enable = true
			ks.Spec.KafkaAuthSpec.Net.SASL.User.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "user",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Password.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "password",
			}
			ks.Spec.KafkaAuthSpec.Net.SASL.Type.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "saslType",
			}
			ks.Spec.KafkaAuthSpec.Net.TLS.Enable = true
			ks.Spec.KafkaAuthSpec.Net.TLS.CACert.SecretKeyRef = &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: kafkaSASLSecret,
				},
				Key: "ca.crt",
			}
		}
	}
	return func(ks *sourcesv1beta1.KafkaSource) {}
}

func testKafkaSource(t *testing.T,
	name string, version string, messageKey string, messageHeaders map[string]string, messagePayload string,
	matcherGen func(cloudEventsSourceName, cloudEventsEventType string) EventMatcher,
	auth authSetup, extensions map[string]string) {

	name = fmt.Sprintf("%s-%s", name, version)

	var (
		kafkaTopicName     = uuid.New().String()
		consumerGroup      = uuid.New().String()
		recordEventPodName = "e2e-kafka-r-" + strings.ReplaceAll(name, "_", "-")
		kafkaSourceName    = "e2e-kafka-source-" + strings.ReplaceAll(name, "_", "-")
	)

	client := testlib.Setup(t, true)
	defer testlib.TearDown(client)

	if auth.SASLEnabled {
		_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaSASLSecret, client.Namespace, "default")
		if err != nil {
			t.Fatalf("could not copy SASL secret(%s): %v", kafkaSASLSecret, err)
		}
	}
	if auth.TLSEnabled {
		_, err := utils.CopySecret(client.Kube.CoreV1(), "knative-eventing", kafkaTLSSecret, client.Namespace, "default")
		if err != nil {
			t.Fatalf("could not copy secret(%s): %v", kafkaTLSSecret, err)
		}
	}
	MustCreateTopic(client, KafkaClusterName, KafkaClusterNamespace, kafkaTopicName, 10)
	if len(recordEventPodName) > 63 {
		recordEventPodName = recordEventPodName[:63]
	}
	eventTracker, _ := recordevents.StartEventRecordOrFail(context.Background(), client, recordEventPodName)

	var (
		cloudEventsSourceName string
		cloudEventsEventType  string
	)

	t.Logf("Creating KafkaSource %s", version)
	switch version {
	case "v1beta1":
		contribtestlib.CreateKafkaSourceV1Beta1OrFail(client, contribresources.KafkaSourceV1Beta1(
			auth.bootStrapServer,
			kafkaTopicName,
			resources.ServiceRef(recordEventPodName),
			contribresources.WithNameV1Beta1(kafkaSourceName),
			contribresources.WithConsumerGroupV1Beta1(consumerGroup),
			withAuthEnablementV1Beta1(auth),
			contribresources.WithExtensionsV1Beta1(extensions),
		))
		cloudEventsSourceName = sourcesv1beta1.KafkaEventSource(client.Namespace, kafkaSourceName, kafkaTopicName)
		cloudEventsEventType = sourcesv1beta1.KafkaEventType
	default:
		t.Fatalf("Unknown KafkaSource version %s", version)
	}

	client.WaitForAllTestResourcesReadyOrFail(context.Background())

	MustPublishKafkaMessage(client, KafkaBootstrapUrlPlain, kafkaTopicName, messageKey, messageHeaders, messagePayload)

	eventTracker.AssertExact(1, recordevents.MatchEvent(matcherGen(cloudEventsSourceName, cloudEventsEventType)))
}

func mustJsonMarshal(t *testing.T, val interface{}) string {
	data, err := json.Marshal(val)
	if err != nil {
		t.Errorf("unexpected error, %v", err)
	}
	return string(data)
}

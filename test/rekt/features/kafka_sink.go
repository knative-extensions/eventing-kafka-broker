/*
 * Copyright 2023 The Knative Authors
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

package features

import (
	"fmt"
	"time"

	"github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/resources/job"

	cetest "github.com/cloudevents/sdk-go/v2/test"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
)

const (
	kafkaConsumerImage = "ko://knative.dev/eventing-kafka-broker/test/test_images/kafka-consumer"
)

/*
+---------+     +--------+     +---------+   +---------------------------+
|  Broker +---->+ Trigger+---->+KafkaSink+-->+kafka consumer (test image)|
+----+----+     +--------+     +----+----+   +---------------------------+

	|                              ^
	|          +--------+          |
	+--------->+ Trigger+----------+
	           +--------+
*/
func BrokerWithTriggersAndKafkaSink(env environment.Environment) *feature.Feature {

	brokerName := feature.MakeRandomK8sName("broker")
	trigger1Name := feature.MakeRandomK8sName("trigger-1")
	trigger2Name := feature.MakeRandomK8sName("trigger-2")
	sink := feature.MakeRandomK8sName("sink")
	verifyMessagesJobName := feature.MakeRandomK8sName("verify-messages-job")

	trigger1FilterType := "trigger-1"
	trigger2FilterType := "trigger-2"

	eventIdTrigger1 := uuid.New().String()
	eventTrigger1 := test.MinEvent()
	eventTrigger1.SetID(eventIdTrigger1)
	eventTrigger1.SetType(trigger1FilterType)

	eventIdTrigger2 := uuid.New().String()
	eventTrigger2 := test.MinEvent()
	eventTrigger2.SetID(eventIdTrigger2)
	eventTrigger2.SetType(trigger2FilterType)

	f := feature.NewFeatureNamed("Trigger with KafkaSink")
	topic, err := apisconfig.DefaultFeaturesConfig().ExecuteBrokersTopicTemplate(metav1.ObjectMeta{Name: brokerName, Namespace: env.Namespace()})
	if err != nil {
		panic("failed to create broker topic name")
	}

	f.Setup("Install broker", broker.Install(brokerName, broker.WithEnvConfig()...))
	f.Setup("Broker is ready", broker.IsReady(brokerName))
	f.Setup("Topic is ready", kafkatopic.IsReady(topic))

	f.Setup("Install kafkasink", kafkasink.Install(sink, topic, testpkg.BootstrapServersPlaintextArr,
		kafkasink.WithNumPartitions(10),
		kafkasink.WithReplicationFactor(1)))
	f.Setup("KafkaSink is ready", kafkasink.IsReady(sink))

	f.Setup("Create trigger 1", trigger.Install(trigger1Name, brokerName,
		trigger.WithSubscriber(kafkasink.AsKReference(sink, env.Namespace()), ""),
		trigger.WithFilter(map[string]string{
			"type": trigger1FilterType,
		})))
	f.Setup("Trigger 1 is ready", trigger.IsReady(trigger1Name))

	f.Setup("Create trigger 2", trigger.Install(trigger2Name, brokerName,
		trigger.WithSubscriber(kafkasink.AsKReference(sink, env.Namespace()), ""),
		trigger.WithFilter(map[string]string{
			"type": trigger2FilterType,
		})))
	f.Setup("Trigger 2 is ready", trigger.IsReady(trigger2Name))

	f.Requirement("Send event for trigger 1", eventshub.Install(feature.MakeRandomK8sName("sender-1"),
		eventshub.InputEvent(eventTrigger1),
		eventshub.StartSenderToResource(broker.GVR(), brokerName)))
	f.Requirement("Send event for trigger 2", eventshub.Install(feature.MakeRandomK8sName("sender-2"),
		eventshub.InputEvent(eventTrigger2),
		eventshub.StartSenderToResource(broker.GVR(), brokerName)))

	f.Requirement("Install verify-messages job", job.Install(verifyMessagesJobName, kafkaConsumerImage,
		job.WithEnvs(map[string]string{
			"BOOTSTRAP_SERVERS": testpkg.BootstrapServersPlaintext,
			"TOPIC":             topic,
			"IDS":               fmt.Sprintf("%s,%s", eventIdTrigger1, eventIdTrigger2),
			"CONTENT_MODE":      "structured",
		}),
		job.WithRestartPolicy(corev1.RestartPolicyNever),
		job.WithBackoffLimit(2),
		job.WithImagePullPolicy(corev1.PullIfNotPresent)))
	f.Assert("Verify-messages job succeeded", job.IsSucceeded(verifyMessagesJobName))

	return f
}

func SetupKafkaTopicWithEvents(count int, topic string) *feature.Feature {

	f := feature.NewFeatureNamed(fmt.Sprintf("setup Kafka topic with %d events", count))

	ksink := feature.MakeRandomK8sName("ksink")
	source := feature.MakeRandomK8sName("source-to-ksink")

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))
	f.Setup("install kafkasink", kafkasink.Install(ksink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("KafkaSink is ready", kafkasink.IsReady(ksink))

	f.Requirement("install source for ksink", eventshub.Install(source,
		eventshub.StartSenderToResource(kafkasink.GVR(), ksink),
		eventshub.InputEvent(cetest.FullEvent()),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(count, time.Millisecond)))

	return f
}

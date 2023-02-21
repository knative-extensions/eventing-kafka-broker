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

	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/feature"

	cetest "github.com/cloudevents/sdk-go/v2/test"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
)

func SetupKafkaTopicWithEvents(count int, topic string) *feature.Feature {

	f := feature.NewFeatureNamed(fmt.Sprintf("setup Kafka topic with %d events", count))

	ksink := feature.MakeRandomK8sName("ksink")
	source := feature.MakeRandomK8sName("source-to-ksink")

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))
	f.Setup("install kafkasink", kafkasink.Install(ksink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("KafkaSink is ready", kafkasink.IsReady(ksink))

	f.Requirement("install source for ksink", eventshub.Install(
		source,
		eventshub.StartSenderToResource(kafkasink.GVR(), ksink),
		eventshub.InputEvent(cetest.FullEvent()),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(2, time.Millisecond),
	))

	return f
}

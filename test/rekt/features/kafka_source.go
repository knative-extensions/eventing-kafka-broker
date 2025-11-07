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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"github.com/google/uuid"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	"knative.dev/eventing/test/rekt/features/featureflags"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/network"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"

	"knative.dev/eventing/test/rekt/features/source"

	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	kafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	sourcesclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

const (
	SASLSecretName = "strimzi-sasl-secret"
	TLSSecretName  = "strimzi-tls-secret"
	SASLMech       = "sasl"
	TLSMech        = "tls"
	PlainMech      = "plain"
)



func SetupAndCleanupKafkaSources(prefix string, n int) *feature.Feature {
	f := SetupKafkaSources(prefix, n)
	f.Teardown("cleanup resources", f.DeleteResources)
	return f
}

func SetupKafkaSources(prefix string, n int) *feature.Feature {
	sink := "sink"
	f := feature.NewFeatureNamed("KafkaSources")

	f.Setup("install a sink", service.Install(sink,
		service.WithSelectors(map[string]string{"app": "rekt"})))

	for i := 0; i < n; i++ {
		topicName := feature.MakeRandomK8sName("topic") // A k8s name is also a valid topic name.
		name := fmt.Sprintf("%s%d", prefix, i)

		f.Setup("install kafka topic", kafkatopic.Install(topicName))
		f.Setup(fmt.Sprintf("install kafkasource %s", name), kafkasource.Install(
			name,
			kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
			kafkasource.WithTopics([]string{topicName}),
			kafkasource.WithSink(service.AsDestinationRef(sink)),
		))

		f.Assert(fmt.Sprintf("kafkasource %s is ready", name), kafkasource.IsReady(name))
	}

	return f
}

func KafkaSourcesAreNotPresentInContractConfigMaps(prefix string) *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSources are not present in Contract configmaps")

	f.Assert("deleted KafkaSources are not present in Contract CMs", deletedKafkaSourcesAreNotPresentInContractConfigMaps(prefix))

	return f
}

func deletedKafkaSourcesAreNotPresentInContractConfigMaps(prefix string) feature.StepFn {
	return func(ctx context.Context, t feature.T) {

		namespace := environment.FromContext(ctx).Namespace()

		kss, err := kafkaclient.Get(ctx).SourcesV1().
			KafkaSources(namespace).
			List(ctx, metav1.ListOptions{
				Limit: 2000,
			})
		if err != nil {
			t.Fatal("Failed to list KafkaSources")
		}

		systemNamespaceCMs, err := kubeclient.Get(ctx).CoreV1().
			ConfigMaps(knative.KnativeNamespaceFromContext(ctx)).
			List(ctx, metav1.ListOptions{
				Limit: 100,
			})
		if err != nil {
			t.Fatal(err)
		}
		for _, cm := range systemNamespaceCMs.Items {
			if !strings.HasPrefix(cm.Name, "kafka-source-dispatcher") {
				continue
			}

			ct, err := base.GetDataPlaneConfigMapData(zap.NewNop(), &cm, base.Json)
			if err != nil {
				t.Fatal(err)
			}

			for _, r := range ct.Resources {
				if r.Reference.Namespace != namespace {
					continue
				}
				if !strings.HasPrefix(r.Reference.Name, prefix) {
					continue
				}

				found := false
				for _, s := range kss.Items {
					if r.Reference.Namespace == s.Namespace && r.Reference.Name == s.Name {
						found = true
					}
				}
				if !found {
					t.Errorf("%v", ResourceError{
						Resource: r,
						Sources:  kss,
					}.Error())
				}
			}
		}
	}
}

type ResourceError struct {
	Resource *contract.Resource       `json:"resource"`
	Sources  *sources.KafkaSourceList `json:"sources"`
}

func (r ResourceError) Error() string {
	bytes, _ := json.Marshal(r)
	return string(bytes)
}

func ScaleKafkaSource() *feature.Feature {
	f := feature.NewFeatureNamed("scale KafkaSource")

	replicas := int32(3)
	source := feature.MakeRandomK8sName("kafkasource")
	topicName := feature.MakeRandomK8sName("scale-topic")
	sink := feature.MakeRandomK8sName("sink")

	f.Setup("install a sink", service.Install(sink,
		service.WithSelectors(map[string]string{"app": "rekt"})))
	f.Setup("install kafka topic", kafkatopic.Install(topicName))
	f.Setup("scale kafkasource", kafkasource.Install(source,
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithSink(service.AsDestinationRef(sink)),
		kafkasource.WithAnnotations(map[string]string{
			// Disable autoscaling for this KafkaSource since we want to have the expected replicas
			// in the status reflected without the autoscaler intervention.
			"autoscaling.eventing.knative.dev/class": "disabled",
		}),
		kafkasource.WithConsumers(replicas),
	))

	f.Requirement("kafkasource is ready", kafkasource.IsReady(source))

	f.Assert("kafkasource is scaled", kafkasource.VerifyScale(source, replicas))

	return f
}

func KafkaSourceInitialOffsetEarliest(count int, topic string) *feature.Feature {

	f := feature.NewFeatureNamed("KafkaSource initial offset earliest")

	sink := feature.MakeRandomK8sName("sink")
	source := feature.MakeRandomK8sName("source")

	f.Setup("install sink", eventshub.Install(sink, eventshub.StartReceiver))

	f.Setup("install kafkasource", kafkasource.Install(
		source,
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithInitialOffset(sources.OffsetEarliest),
		kafkasource.WithSink(service.AsDestinationRef(sink)),
	))
	f.Setup("KafkaSource is ready", kafkasource.IsReady(source))

	f.Requirement("consumergroup has earliest offset set", compareConsumerGroup(
		source,
		func(group *internalscg.ConsumerGroup) error {
			got := group.Spec.Template.Spec.Delivery.InitialOffset
			if got != sources.OffsetEarliest {
				return fmt.Errorf("expected consumergroup to have offset %s set, got %s", sources.OffsetEarliest, got)
			}
			return nil
		},
	))

	for i := 1; i <= count; i++ {
		f.Assert(fmt.Sprintf("received event with sequence %d", i),
			assert.OnStore(sink).
				MatchEvent(cetest.HasExtension("sequence", fmt.Sprintf("%d", i))).
				AtLeast(1),
		)
	}

	return f
}

func compareConsumerGroup(source string, cmp func(*internalscg.ConsumerGroup) error) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		ns := environment.FromContext(ctx).Namespace()

		ks, err := sourcesclient.Get(ctx).
			SourcesV1beta1().
			KafkaSources(ns).
			Get(ctx, source, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		InternalsClient := consumergroupclient.Get(ctx)
		cg, err := InternalsClient.InternalV1alpha1().
			ConsumerGroups(ns).
			Get(ctx, string(ks.UID), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if err := cmp(cg); err != nil {
			t.Error(err)
		}
	}
}

type kafkaSourceConfig struct {
	sourceName string
	authMech   string
	topic      string
	opts       []manifest.CfgFn
}

type kafkaSinkConfig struct {
	sinkName string
	opts     []manifest.CfgFn
}

func KafkaSourceFeatureSetup(f *feature.Feature,
	kafkaSourceCfg kafkaSourceConfig,
	kafkaSinkCfg kafkaSinkConfig) (string, string) {

	if kafkaSourceCfg.topic == "" {
		kafkaSourceCfg.topic = feature.MakeRandomK8sName("topic")
	}

	if kafkaSinkCfg.sinkName == "" {
		kafkaSinkCfg.sinkName = feature.MakeRandomK8sName("kafkaSink")
	}

	if kafkaSourceCfg.sourceName == "" {
		kafkaSourceCfg.sourceName = feature.MakeRandomK8sName("kafkaSource")
	}

	receiver := feature.MakeRandomK8sName("eventshub-receiver")

	secretName := feature.MakeRandomK8sName("secret")

	f.Setup("install kafka topic", kafkatopic.Install(kafkaSourceCfg.topic))
	f.Setup("topic is ready", kafkatopic.IsReady(kafkaSourceCfg.topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install kafkasink", kafkasink.Install(kafkaSinkCfg.sinkName, kafkaSourceCfg.topic,
		testpkg.BootstrapServersPlaintextArr,
		kafkaSinkCfg.opts...))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSinkCfg.sinkName))

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsDestinationRef(receiver)),
		kafkasource.WithTopics([]string{kafkaSourceCfg.topic}),
	}
	kafkaSourceOpts = append(kafkaSourceOpts, kafkaSourceCfg.opts...)

	switch kafkaSourceCfg.authMech {
	case TLSMech:
		f.Setup("Create TLS secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), TLSSecretName, secretName))
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslArr),
			kafkasource.WithTLSCACert(secretName, "ca.crt"),
			kafkasource.WithTLSCert(secretName, "user.crt"),
			kafkasource.WithTLSKey(secretName, "user.key"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(secretName, "ca.crt"),
		)
	case SASLMech:
		f.Setup("Create SASL secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), SASLSecretName, secretName))
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslSaslScramArr),
			kafkasource.WithSASLEnabled(),
			kafkasource.WithSASLUser(secretName, "user"),
			kafkasource.WithSASLPassword(secretName, "password"),
			kafkasource.WithSASLType(secretName, "sasl.mechanism"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(secretName, "ca.crt"),
		)
	default:
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr))
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSourceCfg.sourceName, kafkaSourceOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSourceCfg.sourceName))

	return kafkaSinkCfg.sinkName, receiver
}

func KafkaSourceFeatureAssert(f *feature.Feature, kafkaSink, receiver string, customizeFunc CustomizeEventFunc) {
	sender := feature.MakeRandomK8sName("eventshub-sender")
	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
	}

	senderOpts, matcher := customizeFunc()

	options = append(options, senderOpts...)

	f.Requirement("install eventshub sender", eventshub.Install(sender, options...))

	f.Assert("eventshub receiver gets event", matchEvent(receiver, matcher))
}

func matchEvent(sink string, matcher cetest.EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		assert.OnStore(sink).MatchEvent(matcher).AtLeast(1)(ctx, t)
	}
}

// CustomizeEventFunc creates a pair of eventshub options that customize the event
// and corresponding event matcher that will match the respective event.
type CustomizeEventFunc func() ([]eventshub.EventsHubOption, cetest.EventMatcher)

func KafkaSourceBinaryEventCustomizeFunc() CustomizeEventFunc {
	return func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputHeader("ce-specversion", cloudevents.VersionV1),
			eventshub.InputHeader("ce-type", "com.github.pull.create"),
			eventshub.InputHeader("ce-source", "github.com/cloudevents/spec/pull"),
			eventshub.InputHeader("ce-subject", "123"),
			eventshub.InputHeader("ce-id", id),
			eventshub.InputHeader("content-type", "application/json"),
			eventshub.InputHeader("ce-comexampleextension1", "value"),
			eventshub.InputHeader("ce-comexampleothervalue", "5"),
			eventshub.InputBody(marshalJSON(map[string]string{
				"hello": "Francesco",
			})),
		}
		matcher := cetest.AllOf(
			cetest.HasSpecVersion(cloudevents.VersionV1),
			cetest.HasType("com.github.pull.create"),
			cetest.HasSource("github.com/cloudevents/spec/pull"),
			cetest.HasSubject("123"),
			cetest.HasId(id),
			cetest.HasDataContentType("application/json"),
			cetest.HasData([]byte(`{"hello":"Francesco"}`)),
			cetest.HasExtension("comexampleextension1", "value"),
			cetest.HasExtension("comexampleothervalue", "5"),
		)
		return senderOptions, matcher
	}
}

func KafkaSourceBinaryEvent() *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSourceBinaryEvent")

	kafkaSink, receiver := KafkaSourceBinaryEventFeatureSetup(f)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, KafkaSourceBinaryEventCustomizeFunc())

	return f
}

func KafkaSourceBinaryEventFeatureSetup(f *feature.Feature) (string, string) {
	return KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech: PlainMech,
		},
		kafkaSinkConfig{},
	)
}

func KafkaSourceBinaryEventWithExtensions() *feature.Feature {
	customizeFunc := func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputHeader("ce-specversion", cloudevents.VersionV1),
			eventshub.InputHeader("ce-type", "com.github.pull.create"),
			eventshub.InputHeader("ce-source", "github.com/cloudevents/spec/pull"),
			eventshub.InputHeader("ce-subject", "123"),
			eventshub.InputHeader("ce-id", id),
			eventshub.InputHeader("content-type", "application/json"),
		}
		matcher := cetest.AllOf(
			cetest.HasSpecVersion(cloudevents.VersionV1),
			cetest.HasType("com.github.pull.create"),
			cetest.HasSource("github.com/cloudevents/spec/pull"),
			cetest.HasSubject("123"),
			cetest.HasId(id),
			cetest.HasDataContentType("application/json"),
			cetest.HasExtension("comexampleextension1", "value"),
			cetest.HasExtension("comexampleothervalue", "5"),
		)
		return senderOptions, matcher
	}

	f := feature.NewFeatureNamed("KafkaSourceBinaryEventWithExtensions")

	kafkaSink, receiver := KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech: PlainMech,
			opts: []manifest.CfgFn{
				kafkasource.WithExtensions(map[string]string{
					"comexampleextension1": "value",
					"comexampleothervalue": "5",
				})},
		},
		kafkaSinkConfig{},
	)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, customizeFunc)

	return f
}

func KafkaSourceStructuredEventCustomizeFunc() CustomizeEventFunc {
	return func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		eventTime, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputHeader("content-type", "application/cloudevents+json"),
			eventshub.InputBody(marshalJSON(map[string]interface{}{
				"specversion":     cloudevents.VersionV1,
				"type":            "com.github.pull.create",
				"source":          "https://github.com/cloudevents/spec/pull",
				"subject":         "123",
				"id":              id,
				"time":            "2018-04-05T17:31:00Z",
				"datacontenttype": "application/json",
				"data": map[string]string{
					"hello": "Francesco",
				},
				"comexampleextension1": "value",
				"comexampleothervalue": 5,
			})),
		}
		matcher := cetest.AllOf(
			cetest.HasSpecVersion(cloudevents.VersionV1),
			cetest.HasType("com.github.pull.create"),
			cetest.HasSource("https://github.com/cloudevents/spec/pull"),
			cetest.HasSubject("123"),
			cetest.HasId(id),
			cetest.HasTime(eventTime),
			cetest.HasDataContentType("application/json"),
			cetest.HasData([]byte(`{"hello":"Francesco"}`)),
			cetest.HasExtension("comexampleextension1", "value"),
			cetest.HasExtension("comexampleothervalue", "5"),
		)
		return senderOptions, matcher
	}
}

func KafkaSourceStructuredEvent() *feature.Feature {
	f := feature.NewFeatureNamed("KafkaSourceStructuredEvent")

	kafkaSink, receiver := KafkaSourceBinaryEventFeatureSetup(f)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, KafkaSourceStructuredEventCustomizeFunc())

	return f
}

func KafkaSourceStructuredEventFeatureSetup(f *feature.Feature) (string, string) {
	return KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech: PlainMech,
		},
		kafkaSinkConfig{},
	)
}

func KafkaSourceWithExtensions() *feature.Feature {
	customizeFunc := func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputHeader("content-type", "application/cloudevents+json"),
			eventshub.InputBody(marshalJSON(map[string]interface{}{
				"specversion": cloudevents.VersionV1,
				"type":        "com.github.pull.create",
				"source":      "https://github.com/cloudevents/spec/pull",
				"id":          id,
			})),
		}
		matcher := cetest.AllOf(
			cetest.HasSpecVersion(cloudevents.VersionV1),
			cetest.HasId(id),
			cetest.HasType("com.github.pull.create"),
			cetest.HasSource("https://github.com/cloudevents/spec/pull"),
			cetest.HasExtension("comexampleextension1", "value"),
			cetest.HasExtension("comexampleothervalue", "5"),
		)
		return senderOptions, matcher
	}

	f := feature.NewFeatureNamed("KafkaSourceWithExtensions")

	kafkaSink, receiver := KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech: PlainMech,
			opts: []manifest.CfgFn{
				kafkasource.WithExtensions(map[string]string{
					"comexampleextension1": "value",
					"comexampleothervalue": "5",
				})},
		},
		kafkaSinkConfig{
			opts: []manifest.CfgFn{kafkasink.WithContentMode("structured")},
		},
	)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, customizeFunc)

	return f
}

func KafkaSourceTLS(kafkaSource, kafkaSink, topic string) *feature.Feature {
	customizeFunc := func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		e := cetest.FullEvent()
		e.SetID(id)
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputEvent(e),
		}
		matcher := cetest.AllOf(
			cetest.HasData(e.Data()),
			cetest.HasId(id),
		)
		return senderOptions, matcher
	}

	f := feature.NewFeatureNamed("KafkaSourceTLS")

	kafkaSink, receiver := KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech:   TLSMech,
			topic:      topic,
			sourceName: kafkaSource,
			opts:       []manifest.CfgFn{kafkasource.WithOrdering(string(sources.Ordered))},
		},
		kafkaSinkConfig{
			sinkName: kafkaSink,
		},
	)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, customizeFunc)

	return f
}

func KafkaSourceTLSSink() *feature.Feature {

	kafkaSource := feature.MakeRandomK8sName("kafkaSource")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	topic := feature.MakeRandomK8sName("tls-sink-topic")
	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")
	event := cetest.FullEvent()
	event.SetID(uuid.NewString())

	f := feature.NewFeature()

	f.Prerequisite("should not run when Istio is enabled", featureflags.IstioDisabled())

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install KafkaSink", kafkasink.Install(kafkaSink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSink))

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiverTLS))

	f.Setup("install kafka source", func(ctx context.Context, t feature.T) {
		d := service.AsDestinationRef(receiver)
		d.CACerts = eventshub.GetCaCerts(ctx)
		kafkasource.Install(kafkaSource,
			kafkasource.WithTopics([]string{topic}),
			kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
			kafkasource.WithSink(d),
		)(ctx, t)
	})
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	f.Requirement("install eventshub sender", eventshub.Install(sender,
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.InputEvent(event),
	))

	f.Stable("KafkaSource as event source").
		Must("delivers events on sink with ref",
			assert.OnStore(receiver).
				MatchReceivedEvent(cetest.HasId(event.ID())).
				AtLeast(1)).
		Must("Set sinkURI to HTTPS endpoint", source.ExpectHTTPSSink(kafkasource.GVR(), kafkaSource)).
		Must("Set sinkCACerts to non empty CA certs", source.ExpectCACerts(kafkasource.GVR(), kafkaSource))

	return f
}

func KafkaSourceTLSSinkTrustBundle() *feature.Feature {

	kafkaSource := feature.MakeRandomK8sName("kafkaSource")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	topic := feature.MakeRandomK8sName("tls-sink-topic")
	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")
	event := cetest.FullEvent()
	event.SetID(uuid.NewString())

	f := feature.NewFeature()

	f.Prerequisite("should not run when Istio is enabled", featureflags.IstioDisabled())

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install KafkaSink", kafkasink.Install(kafkaSink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSink))

	f.Setup("install eventshub receiver", eventshub.Install(receiver,
		eventshub.StartReceiverTLS,
		eventshub.IssuerRef(eventingtlstesting.IssuerKind, eventingtlstesting.IssuerName),
	))

	f.Setup("install kafka source", func(ctx context.Context, t feature.T) {
		d := &duckv1.Destination{
			URI: &apis.URL{
				Scheme: "https", // Force using https
				Host:   network.GetServiceHostname(receiver, environment.FromContext(ctx).Namespace()),
			},
			CACerts: nil, // CA certs are in the trust-bundle
		}
		kafkasource.Install(kafkaSource,
			kafkasource.WithTopics([]string{topic}),
			kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
			kafkasource.WithSink(d),
		)(ctx, t)
	})
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	f.Requirement("install eventshub sender", eventshub.Install(sender,
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.InputEvent(event),
	))

	f.Stable("KafkaSource as event source").
		Must("delivers events on sink with ref",
			assert.OnStore(receiver).
				MatchReceivedEvent(cetest.HasId(event.ID())).
				AtLeast(1)).
		Must("Set sinkURI to HTTPS endpoint", source.ExpectHTTPSSink(kafkasource.GVR(), kafkaSource))

	return f
}

func KafkaSourceSASL() *feature.Feature {
	customizeFunc := func() ([]eventshub.EventsHubOption, cetest.EventMatcher) {
		id := feature.MakeRandomK8sName("id")
		e := cetest.FullEvent()
		e.SetID(id)
		senderOptions := []eventshub.EventsHubOption{
			eventshub.InputEvent(e),
		}
		matcher := cetest.AllOf(
			cetest.HasData(e.Data()),
			cetest.HasId(id),
		)
		return senderOptions, matcher
	}

	f := feature.NewFeatureNamed("KafkaSourceSASL")

	kafkaSink, receiver := KafkaSourceFeatureSetup(f,
		kafkaSourceConfig{
			authMech: SASLMech,
		},
		kafkaSinkConfig{},
	)
	KafkaSourceFeatureAssert(f, kafkaSink, receiver, customizeFunc)

	return f
}

func marshalJSON(val interface{}) string {
	data, _ := json.Marshal(val)
	return string(data)
}

func KafkaSourceWithEventAfterUpdate(kafkaSource, kafkaSink, topic string) *feature.Feature {

	f := feature.NewFeatureNamed("KafkaSourceWithEventAfterUpdate")

	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	kafkaSourceUpdateOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsDestinationRef(receiver)),
		// Keep the original topic.
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithOrdering(string(sources.Unordered)),
		kafkasource.WithTLSDisabled(),
		kafkasource.WithSASLDisabled(),
	}

	f.Setup("update kafka source", kafkasource.Install(kafkaSource, kafkaSourceUpdateOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	e := cetest.FullEvent()

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.InputEvent(e),
	}

	f.Requirement("install eventshub sender for kafka sink", eventshub.Install(sender, options...))

	matcher := cetest.AllOf(
		cetest.HasData(e.Data()),
		cetest.HasTime(e.Time()),
	)
	f.Assert("sink receives event", matchEvent(receiver, matcher))

	return f
}

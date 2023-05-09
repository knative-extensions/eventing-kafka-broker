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
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/featuressteps"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	sourcesv1beta1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	kafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	sourcesclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

const (
	saslSecretName = "strimzi-sasl-secret"
	tlsSecretName  = "strimzi-tls-secret"
	SASLMech       = "sasl"
	TLSMech        = "tls"
	PlainMech      = "plain"
)

type kafkaSourceUpdate struct {
	auth authSetup
	sink string
}

type authSetup struct {
	bootstrapServers []string
	SASLEnabled      bool
	TLSEnabled       bool
}

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
			kafkasource.WithSink(&duckv1.KReference{Kind: "Service", Name: sink, APIVersion: "v1"}, ""),
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

		kss, err := kafkaclient.Get(ctx).SourcesV1beta1().
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
		kafkasource.WithSink(service.AsKReference(sink), ""),
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
		kafkasource.WithSink(service.AsKReference(sink), ""),
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
	sourceName   string
	authMech     string
	ceExtensions map[string]string
}

type kafkaSinkConfig struct {
	sinkName string
	opts     []manifest.CfgFn
}

func KafkaSourceFeature(name string,
	kafkaSourceCfg kafkaSourceConfig,
	kafkaSinkCfg kafkaSinkConfig,
	senderOpts []eventshub.EventsHubOption,
	matcher cetest.EventMatcher) *feature.Feature {

	f := feature.NewFeatureNamed(name)

	topic := feature.MakeRandomK8sName("topic")
	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install kafkasink", kafkasink.Install(kafkaSinkCfg.sinkName, topic,
		testpkg.BootstrapServersPlaintextArr,
		kafkaSinkCfg.opts...))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSinkCfg.sinkName))

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsKReference(receiver), ""),
		kafkasource.WithTopics([]string{topic}),
	}
	if len(kafkaSourceCfg.ceExtensions) != 0 {
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithExtensions(kafkaSourceCfg.ceExtensions))
	}

	switch kafkaSourceCfg.authMech {
	case TLSMech:
		f.Setup("Create TLS secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), tlsSecretName))
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslArr),
			kafkasource.WithTLSCACert(tlsSecretName, "ca.crt"),
			kafkasource.WithTLSCert(tlsSecretName, "user.crt"),
			kafkasource.WithTLSKey(tlsSecretName, "user.key"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(tlsSecretName, "ca.crt"),
		)
	case SASLMech:
		f.Setup("Create SASL secret", featuressteps.CopySecretInTestNamespace(system.Namespace(), saslSecretName))
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersSslSaslScramArr),
			kafkasource.WithSASLEnabled(),
			kafkasource.WithSASLUser(saslSecretName, "user"),
			kafkasource.WithSASLPassword(saslSecretName, "password"),
			kafkasource.WithSASLType(saslSecretName, "saslType"),
			kafkasource.WithTLSEnabled(),
			kafkasource.WithTLSCACert(saslSecretName, "ca.crt"),
		)
	default:
		kafkaSourceOpts = append(kafkaSourceOpts, kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr))
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSourceCfg.sourceName, kafkaSourceOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSourceCfg.sourceName, environment.DefaultPollInterval, 5*time.Minute))

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSinkCfg.sinkName),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, time.Millisecond),
	}
	options = append(options, senderOpts...)
	f.Requirement("install eventshub sender", eventshub.Install(sender, options...))

	f.Assert("eventshub receiver gets event", matchEvent(receiver, matcher))

	return f
}

func matchEvent(sink string, matcher EventMatcher) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		assert.OnStore(sink).MatchEvent(matcher).Exact(1)(ctx, t)
	}
}

func KafkaSourceBinaryEvent() *feature.Feature {
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("ce-specversion", cloudevents.VersionV1),
		eventshub.InputHeader("ce-type", "com.github.pull.create"),
		eventshub.InputHeader("ce-source", "github.com/cloudevents/spec/pull"),
		eventshub.InputHeader("ce-subject", "123"),
		eventshub.InputHeader("ce-id", "A234-1234-1234"),
		eventshub.InputHeader("content-type", "application/json"),
		eventshub.InputHeader("ce-comexampleextension1", "value"),
		eventshub.InputHeader("ce-comexampleothervalue", "5"),
		eventshub.InputBody(marshalJSON(map[string]string{
			"hello": "Francesco",
		})),
	}
	matcher := AllOf(
		HasSpecVersion(cloudevents.VersionV1),
		HasType("com.github.pull.create"),
		HasSource("github.com/cloudevents/spec/pull"),
		HasSubject("123"),
		HasId("A234-1234-1234"),
		HasDataContentType("application/json"),
		HasData([]byte(`{"hello":"Francesco"}`)),
		HasExtension("comexampleextension1", "value"),
		HasExtension("comexampleothervalue", "5"),
	)

	return KafkaSourceFeature("KafkaSourceBinaryEvent",
		kafkaSourceConfig{
			authMech:   PlainMech,
			sourceName: feature.MakeRandomK8sName("kafkaSource"),
		},
		kafkaSinkConfig{
			sinkName: feature.MakeRandomK8sName("kafkaSink"),
		},
		senderOptions,
		matcher,
	)
}

func KafkaSourceStructuredEvent() *feature.Feature {
	eventTime, _ := cetypes.ParseTime("2018-04-05T17:31:00Z")
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("content-type", "application/cloudevents+json"),
		eventshub.InputBody(marshalJSON(map[string]interface{}{
			"specversion":     cloudevents.VersionV1,
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
		})),
	}
	matcher := AllOf(
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

	return KafkaSourceFeature("KafkaSourceStructuredEvent",
		kafkaSourceConfig{
			authMech:   PlainMech,
			sourceName: feature.MakeRandomK8sName("kafkaSource"),
		},
		kafkaSinkConfig{
			sinkName: feature.MakeRandomK8sName("kafkaSink"),
			opts:     []manifest.CfgFn{kafkasink.WithContentMode("structured")},
		},
		senderOptions,
		matcher,
	)
}

func KafkaSourceWithExtensions() *feature.Feature {
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputHeader("content-type", "application/cloudevents+json"),
		eventshub.InputBody(marshalJSON(map[string]interface{}{
			"specversion": cloudevents.VersionV1,
			"type":        "com.github.pull.create",
			"source":      "https://github.com/cloudevents/spec/pull",
			"id":          "A234-1234-1234",
		})),
	}
	kafkaSourceExtensions := map[string]string{
		"comexampleextension1": "value",
		"comexampleothervalue": "5",
	}
	matcher := AllOf(
		HasSpecVersion(cloudevents.VersionV1),
		HasType("com.github.pull.create"),
		HasSource("https://github.com/cloudevents/spec/pull"),
		HasExtension("comexampleextension1", "value"),
		HasExtension("comexampleothervalue", "5"),
	)

	return KafkaSourceFeature("KafkaSourceWithExtensions",
		kafkaSourceConfig{
			authMech:     PlainMech,
			sourceName:   feature.MakeRandomK8sName("kafkaSource"),
			ceExtensions: kafkaSourceExtensions,
		},
		kafkaSinkConfig{
			sinkName: feature.MakeRandomK8sName("kafkaSink"),
			opts:     []manifest.CfgFn{kafkasink.WithContentMode("structured")},
		},
		senderOptions,
		matcher,
	)
}

func KafkaSourceTLS(kafkaSource, kafkaSink string) *feature.Feature {
	e := cetest.FullEvent()
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputEvent(e),
	}
	matcher := HasData(e.Data())

	return KafkaSourceFeature("KafkaSourceTLS",
		kafkaSourceConfig{
			authMech:   TLSMech,
			sourceName: feature.MakeRandomK8sName("kafkaSource"),
		},
		kafkaSinkConfig{
			sinkName: feature.MakeRandomK8sName("kafkaSink"),
		},
		senderOptions,
		matcher,
	)
}

func KafkaSourceSASL() *feature.Feature {
	e := cetest.FullEvent()
	senderOptions := []eventshub.EventsHubOption{
		eventshub.InputEvent(e),
	}
	matcher := HasData(e.Data())

	return KafkaSourceFeature("KafkaSourceSASL",
		kafkaSourceConfig{
			authMech:   SASLMech,
			sourceName: feature.MakeRandomK8sName("kafkaSource"),
		},
		kafkaSinkConfig{
			sinkName: feature.MakeRandomK8sName("kafkaSink"),
		},
		senderOptions,
		matcher,
	)
}

func marshalJSON(val interface{}) string {
	data, _ := json.Marshal(val)
	return string(data)
}

func KafkaSourceWithEventAfterUpdate(kafkaSource string, kafkaSink string) *feature.Feature {

	f := feature.NewFeatureNamed("KafkaSourceWithEventAfterUpdate")

	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	update := kafkaSourceUpdate{
		sink: receiver,
		auth: authSetup{
			bootstrapServers: testingpkg.BootstrapServersPlaintextArr,
			TLSEnabled:       false,
			SASLEnabled:      false,
		},
	}
	f.Setup("update kafka source", ModifyKafkaSource(kafkaSource, update))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	e := cetest.FullEvent()

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.AddSequence,
		eventshub.SendMultipleEvents(1, time.Millisecond),
		eventshub.InputEvent(e),
	}

	f.Requirement("install eventshub sender for kafka sink", eventshub.Install(sender, options...))

	matcher := AllOf(
		HasData(e.Data()),
		HasTime(e.Time()),
	)
	f.Assert("sink receives event", matchEvent(receiver, matcher))

	return f
}

func ModifyKafkaSource(kafkaSourceName string, update kafkaSourceUpdate) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		ksObj, err := waitForKafkaSourceToBeReconciled(ctx, kafkaSourceName)
		if err != nil {
			t.Fatalf("Failed waiting for KafkaSource to be reconciled: %v", ksObj)
		}

		// Update KafkaSource spec
		ksObj.Spec.Sink.Ref = service.AsKReference(update.sink)
		ksObj.Spec.KafkaAuthSpec.BootstrapServers = update.auth.bootstrapServers
		ksObj.Spec.KafkaAuthSpec.Net.TLS.Enable = update.auth.TLSEnabled
		ksObj.Spec.KafkaAuthSpec.Net.SASL.Enable = update.auth.SASLEnabled

		err = updateKafkaSource(ctx, ksObj)
		if err != nil {
			t.Fatalf("Failed to update v1beta1 KafkaSource %q: %v", ksObj.Name, err)
		}

		_, err = waitForKafkaSourceToBeReconciled(ctx, kafkaSourceName)
		if err != nil {
			t.Fatalf("Failed waiting for KafkaSource to be reconciled: %v", ksObj)
		}
	}
}

func waitForKafkaSourceToBeReconciled(ctx context.Context, kafkaSourceName string) (*sourcesv1beta1.KafkaSource, error) {
	var ksObj *sourcesv1beta1.KafkaSource
	interval, timeout := environment.PollTimingsFromContext(ctx)
	err := wait.Poll(interval, timeout, func() (done bool, err error) {
		ns := environment.FromContext(ctx).Namespace()
		ksObj, err = kafkaclient.Get(ctx).SourcesV1beta1().
			KafkaSources(ns).
			Get(ctx, kafkaSourceName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return ksObj.Status.IsReady() && ksObj.Status.ObservedGeneration == ksObj.Generation, nil
	})
	return ksObj, err
}

func updateKafkaSource(ctx context.Context, ksObj *sourcesv1beta1.KafkaSource) error {
	return retryWebhookErrors(func(i int) error {
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			ns := environment.FromContext(ctx).Namespace()
			latestKafkaSource, err := kafkaclient.Get(ctx).SourcesV1beta1().
				KafkaSources(ns).
				Get(ctx, ksObj.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			ksObj.Spec.DeepCopyInto(&latestKafkaSource.Spec)
			_, err = kafkaclient.Get(ctx).SourcesV1beta1().
				KafkaSources(ns).Update(context.Background(), latestKafkaSource, metav1.UpdateOptions{})
			return err
		})
	})
}

func isWebhookError(err error) bool {
	str := err.Error()
	// Example error:
	// Internal error occurred: failed calling webhook "defaulting.webhook.kafka.eventing.knative.dev": Post "https://kafka-webhook-eventing.knative-eventing.svc:443/defaulting?timeout=2s": EOF
	return strings.Contains(str, "webhook") &&
		strings.Contains(str, "https") &&
		strings.Contains(str, "EOF")
}

func retryWebhookErrors(updater func(int) error) error {
	attempts := 0
	return retry.OnError(retry.DefaultRetry, isWebhookError, func() error {
		err := updater(attempts)
		attempts++
		return err
	})
}

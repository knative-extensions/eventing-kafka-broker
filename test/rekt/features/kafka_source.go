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

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/knative"
	"knative.dev/reconciler-test/resources/svc"

	internalscg "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
	kafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	sourcesclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

func SetupAndCleanupKafkaSources(prefix string, n int) *feature.Feature {
	f := SetupKafkaSources(prefix, n)
	f.Teardown("cleanup resources", f.DeleteResources)
	return f
}

func SetupKafkaSources(prefix string, n int) *feature.Feature {
	sink := "sink"
	f := feature.NewFeatureNamed("KafkaSources")

	f.Setup("install a sink", svc.Install(sink, "app", "rekt"))

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

	f.Setup("install a sink", svc.Install(sink, "app", "rekt"))
	f.Setup("install kafka topic", kafkatopic.Install(topicName))
	f.Setup("scale kafkasource", kafkasource.Install(source,
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
		kafkasource.WithTopics([]string{topicName}),
		kafkasource.WithSink(svc.AsKReference(sink), ""),
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
		kafkasource.WithSink(svc.AsKReference(sink), ""),
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

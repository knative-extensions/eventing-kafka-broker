/*
 * Copyright 2022 The Knative Authors
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

package continual

import (
	"fmt"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	bindings "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	eventingkafkaupgrade "knative.dev/eventing-kafka/test/upgrade/continual"
	"knative.dev/eventing/test/upgrade/prober/sut"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	pkgupgrade "knative.dev/pkg/test/upgrade"

	eventingkafkatestlib "knative.dev/eventing-kafka/test/lib"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	kafkaSinkConfigTemplate = "test/upgrade/continual/kafka-sink-source-config.toml"

	defaultTopic = "kafka-sink-source-continual-test-topic"
)

type KafkaSinkSourceTestOptions struct {
	*eventingkafkaupgrade.TestOptions
	*Sink
	*Source
}

func (o *KafkaSinkSourceTestOptions) setDefaults() {
	if o.TestOptions == nil {
		o.TestOptions = &eventingkafkaupgrade.TestOptions{}
	}
	if o.Sink == nil {
		rf := int16(3)
		o.Sink = &Sink{
			Name: "sink-kafka-sink-source-continual-test",
			Spec: eventing.KafkaSinkSpec{
				Topic:             defaultTopic,
				NumPartitions:     pointer.Int32(10),
				ReplicationFactor: &rf,
				BootstrapServers:  []string{testingpkg.BootstrapServersPlaintext},
			},
		}
	}
	if o.Source == nil {
		o.Source = &Source{
			Name: "source-kafka-sink-source-continual-test",
			Spec: sources.KafkaSourceSpec{
				Topics:        []string{defaultTopic},
				ConsumerGroup: "source-kafka-sink-source-continual-test-consumer-group",
				InitialOffset: sources.OffsetEarliest,
				KafkaAuthSpec: bindings.KafkaAuthSpec{
					BootstrapServers: []string{testingpkg.BootstrapServersPlaintext},
				},
			},
		}
	}
}

func (o *KafkaSinkSourceTestOptions) validate() error {
	if len(o.Source.Spec.Topics) != 1 || o.Sink.Spec.Topic != o.Source.Spec.Topics[0] {
		return fmt.Errorf("expected sink and source topic to be the same topic, source topics %v, sink topic %v", o.Source.Spec.Topics, o.Sink.Spec.Topic)
	}
	return nil
}

type kafkaSinkSourceSut struct {
	Sink
	Source
}

func (k kafkaSinkSourceSut) Deploy(ctx sut.Context, destination duckv1.Destination) interface{} {
	k.deploySink(ctx)
	k.deploySource(ctx, destination)
	url := k.fetchUrl(ctx)
	return url
}

func (k kafkaSinkSourceSut) deploySink(ctx sut.Context) {
	clientSet, err := eventingv1alpha1clientset.NewForConfig(ctx.Config)
	require.Nil(ctx.T, err)

	s := types.NamespacedName{
		Namespace: ctx.Client.Namespace,
		Name:      k.Sink.Name,
	}
	if _, err = sink.CreatorV1Alpha1(clientSet, ctx.Tracker, k.Sink.Spec)(s); err != nil {
		ctx.T.Fatalf("failed to create KafkaSink %+v: %v", s, err)
	}
}

func (k kafkaSinkSourceSut) deploySource(ctx sut.Context, destination duckv1.Destination) {
	ks := &sources.KafkaSource{
		TypeMeta: metav1.TypeMeta{Kind: "KafkaSource", APIVersion: sources.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.Source.Name,
			Namespace: ctx.Client.Namespace,
		},
		Spec: *k.Source.Spec.DeepCopy(),
	}
	ks.Spec.Sink = destination
	eventingkafkatestlib.CreateKafkaSourceV1Beta1OrFail(ctx.Client, ks)
}

func (k kafkaSinkSourceSut) fetchUrl(ctx sut.Context) *apis.URL {
	tm := &metav1.TypeMeta{
		Kind:       "KafkaSink",
		APIVersion: eventing.SchemeGroupVersion.String(),
	}
	ctx.Client.WaitForResourceReadyOrFail(k.Sink.Name, tm)
	address, err := ctx.Client.GetAddressableURI(k.Sink.Name, tm)
	require.Nil(ctx.T, err, "Failed to get address from sink %s", k.Sink.Name)

	url, _ := apis.ParseURL(address)
	return url
}

func SinkSourceTest(opts KafkaSinkSourceTestOptions) pkgupgrade.BackgroundOperation {
	opts.setDefaults()
	if err := opts.validate(); err != nil {
		panic(err)
	}
	return continualVerification(
		"KafkaSinkSourceContinualTest",
		opts.TestOptions,
		&kafkaSinkSourceSut{Sink: *opts.Sink, Source: *opts.Source},
		kafkaSinkConfigTemplate,
	)
}

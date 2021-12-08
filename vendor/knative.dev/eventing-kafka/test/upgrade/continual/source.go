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

package continual

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/test/e2e/helpers"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	contribresources "knative.dev/eventing-kafka/test/lib/resources"
	"knative.dev/eventing/test/upgrade/prober"
	"knative.dev/eventing/test/upgrade/prober/sut"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	pkgTest "knative.dev/pkg/test"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

const (
	defaultKafkaBootstrapPort    = 9092
	defaultKafkaClusterName      = "my-cluster"
	defaultKafkaClusterNamespace = "kafka"
	sourceConfigTemplatePath     = "test/upgrade/continual/source-config.toml"
)

// SourceTestOptions holds test options for KafkaSource tests.
type SourceTestOptions struct {
	*TestOptions
	*KafkaCluster
}

// SourceTest tests source operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are propagated
// well.
func SourceTest(opts SourceTestOptions) pkgupgrade.BackgroundOperation {
	opts = opts.withDefaults()
	return continualVerification(
		"SourceContinualTests",
		opts.TestOptions,
		&kafkaSourceSut{KafkaCluster: opts.KafkaCluster},
		sourceConfigTemplatePath,
	)
}

func (o SourceTestOptions) withDefaults() SourceTestOptions {
	sto := o
	if sto.TestOptions == nil {
		sto.TestOptions = &TestOptions{}
	}
	if sto.KafkaCluster == nil {
		sto.KafkaCluster = &KafkaCluster{}
	}
	c := sto.KafkaCluster.withDefaults()
	sto.KafkaCluster = &c
	sto.Configurators = append([]prober.Configurator{
		func(config *prober.Config) error {
			config.Wathola.ImageResolver = kafkaSourceSenderImageResolver
			return nil
		},
	}, sto.Configurators...)
	return sto
}

func (c KafkaCluster) withDefaults() KafkaCluster {
	kc := c
	if kc.Name == "" {
		kc.Name = defaultKafkaClusterName
	}
	if kc.Namespace == "" {
		kc.Namespace = defaultKafkaClusterNamespace
	}
	if len(kc.BootstrapServers) == 0 {
		kc.BootstrapServers = []string{
			fmt.Sprintf("%s-kafka-bootstrap.%s.svc:%d",
				kc.Name,
				kc.Namespace,
				defaultKafkaBootstrapPort,
			),
		}
	}
	return kc
}

func kafkaSourceSenderImageResolver(component string) string {
	if component == "wathola-sender" {
		// replacing the original image with modified one from this repo
		component = "wathola-kafka-sender"
	}
	return pkgTest.ImagePath(component)
}

type kafkaSourceSut struct {
	*KafkaCluster
}

func (k kafkaSourceSut) Deploy(ctx sut.Context, destination duckv1.Destination) interface{} {
	topicName := uuid.NewString()
	c := k.KafkaCluster
	helpers.MustCreateTopic(ctx.Client, c.Name, c.Namespace,
		topicName, 6)
	contribtestlib.CreateKafkaSourceV1Beta1OrFail(ctx.Client, contribresources.KafkaSourceV1Beta1(
		c.serversLine(),
		topicName,
		toObjectReference(destination),
	))
	return kafkaTopicEndpoint{
		BootstrapServers: k.KafkaCluster.serversLine(),
		TopicName:        topicName,
	}
}

func (c KafkaCluster) serversLine() string {
	return strings.Join(c.BootstrapServers, ",")
}

func toObjectReference(destination duckv1.Destination) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: destination.Ref.APIVersion,
		Kind:       destination.Ref.Kind,
		Namespace:  destination.Ref.Namespace,
		Name:       destination.Ref.Name,
	}
}

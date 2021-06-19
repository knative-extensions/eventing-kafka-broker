/*
 * Copyright 2021 The Knative Authors
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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/test/upgrade/prober/sut"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/system"
	pkgupgrade "knative.dev/pkg/test/upgrade"
)

const (
	kafkaBrokerConfigTemplatePath = "test/upgrade/continual/kafka-broker-config.toml"

	defaultRetryCount    = 12
	defaultBackoffPolicy = eventingduckv1.BackoffPolicyExponential
	defaultBackoffDelay  = "PT1S"
)

// KafkaBrokerTestOptions holds test options for Kafka Broker tests.
type KafkaBrokerTestOptions struct {
	*TestOptions
	*ReplicationOptions
	*RetryOptions
}

func (o *KafkaBrokerTestOptions) setDefaults() {
	if o.TestOptions == nil {
		o.TestOptions = &TestOptions{}
	}
	if o.RetryOptions == nil {
		o.RetryOptions = defaultRetryOptions()
	}
	if o.ReplicationOptions == nil {
		o.ReplicationOptions = defaultReplicationOptions()
	}
}

func defaultRetryOptions() *RetryOptions {
	return &RetryOptions{
		RetryCount:    defaultRetryCount,
		BackoffPolicy: defaultBackoffPolicy,
		BackoffDelay:  defaultBackoffDelay,
	}
}

func defaultReplicationOptions() *ReplicationOptions {
	return &ReplicationOptions{
		NumPartitions:     6,
		ReplicationFactor: 3,
	}
}

// BrokerTest tests a broker operation in continual manner during the
// whole upgrade and downgrade process asserting that all event are
// propagated well.
func BrokerTest(opts KafkaBrokerTestOptions) pkgupgrade.BackgroundOperation {
	opts.setDefaults()
	return continualVerification(
		"KafkaBrokerContinualTest",
		opts.TestOptions,
		&kafkaBrokerSut{
			ReplicationOptions: opts.ReplicationOptions,
			RetryOptions:       opts.RetryOptions,
			defaultSut:         nil,
		},
		kafkaBrokerConfigTemplatePath,
	)
}

type kafkaBrokerSut struct {
	*ReplicationOptions
	*RetryOptions
	defaultSut sut.SystemUnderTest
}

func (b kafkaBrokerSut) Deploy(ctx sut.Context, destination duckv1.Destination) interface{} {
	b.setKafkaBrokerAsDefaultForBroker(ctx)

	b.defaultSut = sut.NewDefault()
	return b.defaultSut.Deploy(ctx, destination)
}

func (b *kafkaBrokerSut) setKafkaBrokerAsDefaultForBroker(ctx sut.Context) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: system.Namespace(),
			Name:      "config-br-defaults",
		},
		Data: map[string]string{
			"default-br-config": `
clusterDefault:
  brokerClass: Kafka
  apiVersion: v1
  kind: ConfigMap
  name: kafka-broker-config
  namespace: knative-eventing
`,
		},
	}
	cm, err := ctx.Client.Kube.CoreV1().ConfigMaps(system.Namespace()).Update(ctx.Ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		ctx.T.Fatal(err)
	}
	ctx.Log.Info("Updated config-br-defaults in ns knative-eventing to eq: ", cm.Data)
}

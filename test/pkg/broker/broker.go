/*
 * Copyright 2020 The Knative Authors
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

package broker

import (
	"context"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingtestlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
)

const BrokerClassEnvVarKey = "BROKER_CLASS"

func GetKafkaClassFromEnv() (string, error) {
	val := ""
	exists := false

	if val, exists = os.LookupEnv(BrokerClassEnvVarKey); !exists {
		return "", fmt.Errorf("unable to determine KafkaBroker class. Specify '%s' env var", BrokerClassEnvVarKey)
	}

	if val != kafka.BrokerClass && val != kafka.NamespacedBrokerClass {
		return "", fmt.Errorf("KafkaBroker class '%s' is unknown. Specify '%s' env var", val, BrokerClassEnvVarKey)
	}

	return val, nil
}

func WithBrokerClassFromEnvVar(b *eventingv1.Broker) {
	class, err := GetKafkaClassFromEnv()
	if err != nil {
		panic(fmt.Sprintf("error getting KafkaBroker class from env '%v'", err))
	}
	resources.WithBrokerClassForBroker(class)(b)
}

// Creator creates a Broker with the class that is specified in an environment variable.
func Creator(client *eventingtestlib.Client, version string) string {
	return CreatorWithBrokerOptions(client, version, WithBrokerClassFromEnvVar)
}

// CreatorForClass creates a Broker with the given class.
func CreatorForClass(class string) func(*eventingtestlib.Client, string) string {
	return func(client *eventingtestlib.Client, version string) string {
		return CreatorWithBrokerOptions(client, version, resources.WithBrokerClassForBroker(class))
	}
}

type ConfigOptions func(data map[string]string)

func CreatorWithBrokerOptions(client *eventingtestlib.Client, version string, brokerOptions ...resources.BrokerOption) string {
	return CreatorWithOptions(client, version, brokerOptions, []ConfigOptions{})
}

func CreatorWithOptions(client *eventingtestlib.Client, version string, brokerOptions []resources.BrokerOption, configOptions []ConfigOptions) string {
	name := "broker"

	version = strings.ToLower(version)

	switch version {
	case "v1":
		namespace := client.Namespace
		cmName := "kafka-broker-config"
		// Create Broker's own ConfigMap to prevent using defaults.
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: namespace,
			},
			Data: map[string]string{
				kafka.BootstrapServersConfigMapKey:              testingpkg.BootstrapServersPlaintext,
				kafka.DefaultTopicNumPartitionConfigMapKey:      fmt.Sprintf("%d", testingpkg.NumPartitions),
				kafka.DefaultTopicReplicationFactorConfigMapKey: fmt.Sprintf("%d", testingpkg.ReplicationFactor),
			},
		}
		for _, co := range configOptions {
			co(cm.Data)
		}

		cm, err := client.Kube.CoreV1().ConfigMaps(namespace).Create(context.Background(), cm, metav1.CreateOptions{})
		if err != nil && !apierrors.IsAlreadyExists(err) {
			client.T.Fatalf("Failed to create ConfigMap %s/%s: %v", namespace, cm.GetName(), err)
		}

		brokerOptions = append(brokerOptions,
			resources.WithConfigForBroker(&duckv1.KReference{
				Kind:       "ConfigMap",
				Namespace:  namespace,
				Name:       cmName,
				APIVersion: "v1",
			}),
		)

		client.CreateBrokerOrFail(
			name,
			brokerOptions...,
		)
	default:
		panic(fmt.Sprintf("Unsupported version of Broker: %q", version))
	}

	return name
}

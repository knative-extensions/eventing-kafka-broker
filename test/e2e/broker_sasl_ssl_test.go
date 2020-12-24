// +build e2e

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

package e2e

import (
	"context"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/util/retry"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/lib/sender"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	kafkaNamespace     = "kafka"
	tlsUserSecretName  = "my-tls-user"
	saslUserSecretName = "my-sasl-user"
	caSecretName       = "my-cluster-cluster-ca-cert"

	kafkaListenerPlaintext     = "my-cluster-kafka-bootstrap.kafka:9092"
	kafkaListenerSSL           = "my-cluster-kafka-bootstrap.kafka:9093"
	kafkaListenerSASLPlaintext = "my-cluster-kafka-bootstrap.kafka:9094"
	kafkaListenerSSLSASLSCRAM  = "my-cluster-kafka-bootstrap.kafka:9095"
)

type SecretProvider func(name string, client *testlib.Client) map[string][]byte

type ConfigProvider func(secretName string, client *testlib.Client) map[string]string

func BrokerAuthBecomeReady(t *testing.T, secretProvider SecretProvider, configProvider ConfigProvider) {

	pkgtesting.RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		const (
			brokerName  = "broker"
			triggerName = "trigger"
			subscriber  = "subscriber"
			configMap   = "config-broker"
			secretName  = "broker-auth"

			eventType   = "type1"
			eventSource = "source1"
			eventBody   = `{"msg":"e2e-auth-body"}`
			senderName  = "sender"
		)

		client := testlib.Setup(t, true)
		defer testlib.TearDown(client)

		client.CreateConfigMapOrFail(
			configMap,
			client.Namespace,
			configProvider(secretName, client),
		)

		client.CreateBrokerV1OrFail(
			brokerName,
			resources.WithBrokerClassForBrokerV1(kafka.BrokerClass),
			resources.WithConfigForBrokerV1(&duckv1.KReference{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Namespace:  client.Namespace,
				Name:       configMap,
			}),
		)

		// secret doesn't exist, so broker won't become ready.
		time.Sleep(time.Second * 30)
		br, err := client.Eventing.EventingV1().Brokers(client.Namespace).Get(ctx, brokerName, metav1.GetOptions{})
		assert.Nil(t, err)
		assert.False(t, br.Status.IsReady(), "secret %s/%s doesn't exist, so broker must no be ready", client.Namespace, secretName)

		secretData := secretProvider(secretName, client)

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: client.Namespace,
				Name:      secretName,
			},
			Data: secretData,
		}

		secret, err = client.Kube.CoreV1().Secrets(client.Namespace).Create(ctx, secret, metav1.CreateOptions{})
		assert.Nil(t, err)

		// Trigger a reconciliation by updating the referenced ConfigMap in broker.spec.config.
		err = retry.RetryOnConflict(retry.DefaultBackoff, func() error {
			config, err := client.Kube.CoreV1().ConfigMaps(client.Namespace).Get(ctx, configMap, metav1.GetOptions{})
			if err != nil {
				return nil
			}

			if config.Labels == nil {
				config.Labels = make(map[string]string, 1)
			}
			config.Labels["test.eventing.knative.dev/updated"] = names.SimpleNameGenerator.GenerateName("now")

			config, err = client.Kube.CoreV1().ConfigMaps(client.Namespace).Update(ctx, config, metav1.UpdateOptions{})
			return err
		})
		assert.Nil(t, err)

		client.WaitForResourceReadyOrFail(brokerName, testlib.BrokerTypeMeta)

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber, recordevents.AddTracing())

		client.CreateTriggerV1OrFail(
			triggerName,
			resources.WithBrokerV1(brokerName),
			resources.WithSubscriberServiceRefForTriggerV1(subscriber),
		)

		client.WaitForAllTestResourcesReadyOrFail(ctx)

		id := uuid.New().String()
		eventToSend := cloudevents.NewEvent()
		eventToSend.SetID(id)
		eventToSend.SetType(eventType)
		eventToSend.SetSource(eventSource)
		err = eventToSend.SetData(cloudevents.ApplicationJSON, []byte(eventBody))
		assert.Nil(t, err)

		client.SendEventToAddressable(
			ctx,
			senderName,
			brokerName,
			testlib.BrokerTypeMeta,
			eventToSend,
			sender.EnableTracing(),
		)

		eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
			HasId(id),
			HasSource(eventSource),
			HasType(eventType),
			HasData([]byte(eventBody)),
		))
	})
}

func TestBrokerAuthBecomeReadyPlaintext(t *testing.T) {

	BrokerAuthBecomeReady(
		t,
		func(name string, client *testlib.Client) map[string][]byte {
			return map[string][]byte{
				"protocol": []byte("PLAINTEXT"),
			}
		},
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                kafkaListenerPlaintext,
				"auth.secret.name":                 secretName,
			}
		},
	)
}

func TestBrokerAuthBecomeReadySsl(t *testing.T) {

	BrokerAuthBecomeReady(
		t,
		func(name string, client *testlib.Client) map[string][]byte {
			caSecret, err := client.Kube.CoreV1().Secrets(kafkaNamespace).Get(context.Background(), caSecretName, metav1.GetOptions{})
			assert.Nil(t, err)

			tlsUserSecret, err := client.Kube.CoreV1().Secrets(kafkaNamespace).Get(context.Background(), tlsUserSecretName, metav1.GetOptions{})
			assert.Nil(t, err)

			return map[string][]byte{
				"protocol": []byte("SSL"),
				"ca.crt":   caSecret.Data["ca.crt"],
				"user.crt": tlsUserSecret.Data["user.crt"],
				"user.key": tlsUserSecret.Data["user.key"],
			}
		},
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                kafkaListenerSSL,
				"auth.secret.name":                 secretName,
			}
		},
	)
}

func TestBrokerAuthBecomeReadySaslPlaintextScram512(t *testing.T) {

	BrokerAuthBecomeReady(
		t,
		func(name string, client *testlib.Client) map[string][]byte {

			saslUserSecret, err := client.Kube.CoreV1().Secrets(kafkaNamespace).Get(context.Background(), saslUserSecretName, metav1.GetOptions{})
			assert.Nil(t, err)

			return map[string][]byte{
				"protocol":       []byte("SASL_PLAINTEXT"),
				"sasl.mechanism": []byte("SCRAM-SHA-512"),
				"user":           []byte(saslUserSecretName),
				"password":       saslUserSecret.Data["password"],
			}
		},
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                kafkaListenerSASLPlaintext,
				"auth.secret.name":                 secretName,
			}
		},
	)
}

func TestBrokerAuthBecomeReadySslSaslScram512(t *testing.T) {

	BrokerAuthBecomeReady(
		t,
		func(name string, client *testlib.Client) map[string][]byte {
			caSecret, err := client.Kube.CoreV1().Secrets(kafkaNamespace).Get(context.Background(), caSecretName, metav1.GetOptions{})
			assert.Nil(t, err)

			saslUserSecret, err := client.Kube.CoreV1().Secrets(kafkaNamespace).Get(context.Background(), saslUserSecretName, metav1.GetOptions{})
			assert.Nil(t, err)

			return map[string][]byte{
				"protocol":       []byte("SASL_SSL"),
				"sasl.mechanism": []byte("SCRAM-SHA-512"),
				"ca.crt":         caSecret.Data["ca.crt"],
				"user":           []byte(saslUserSecretName),
				"password":       saslUserSecret.Data["password"],
			}
		},
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                kafkaListenerSSLSASLSCRAM,
				"auth.secret.name":                 secretName,
			}
		},
	)
}

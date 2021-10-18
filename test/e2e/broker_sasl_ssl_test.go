//go:build e2e
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
	. "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func brokerAuth(t *testing.T, secretProvider SecretProvider, configProvider ConfigProvider) {

	RunMultiple(t, func(t *testing.T) {

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

		client.CreateBrokerOrFail(
			brokerName,
			resources.WithBrokerClassForBroker(kafka.BrokerClass),
			resources.WithConfigForBroker(&duckv1.KReference{
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
		assert.False(t, br.IsReady(), "secret %s/%s doesn't exist, so broker must no be ready", client.Namespace, secretName)

		secretData := secretProvider(t, client)

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: client.Namespace,
				Name:      secretName,
			},
			Data: secretData,
		}

		_, err = client.Kube.CoreV1().Secrets(client.Namespace).Create(ctx, secret, metav1.CreateOptions{})
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

			_, err = client.Kube.CoreV1().ConfigMaps(client.Namespace).Update(ctx, config, metav1.UpdateOptions{})
			return err
		})
		assert.Nil(t, err)

		client.WaitForResourceReadyOrFail(brokerName, testlib.BrokerTypeMeta)

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, subscriber, recordevents.AddTracing())

		client.CreateTriggerOrFail(
			triggerName,
			resources.WithBroker(brokerName),
			resources.WithSubscriberServiceRefForTrigger(subscriber),
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

func TestBrokerAuthPlaintext(t *testing.T) {

	brokerAuth(
		t,
		Plaintext,
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                BootstrapServersPlaintext,
				"auth.secret.ref.name":             secretName,
			}
		},
	)
}

func TestBrokerAuthSsl(t *testing.T) {

	brokerAuth(
		t,
		Ssl,
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                BootstrapServersSsl,
				"auth.secret.ref.name":             secretName,
			}
		},
	)
}

func TestBrokerAuthSaslPlaintextScram512(t *testing.T) {

	brokerAuth(
		t,
		SaslPlaintextScram512,
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                BootstrapServersSaslPlaintext,
				"auth.secret.ref.name":             secretName,
			}
		},
	)
}

func TestBrokerAuthSslSaslScram512(t *testing.T) {

	brokerAuth(
		t,
		SslSaslScram512,
		func(secretName string, client *testlib.Client) map[string]string {
			return map[string]string{
				"default.topic.replication.factor": "2",
				"default.topic.partitions":         "2",
				"bootstrap.servers":                BootstrapServersSslSaslScram,
				"auth.secret.ref.name":             secretName,
			}
		},
	)
}

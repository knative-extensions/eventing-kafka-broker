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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
	. "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	sinkSecretName = "secret-test"
)

func TestKafkaSinkV1Alpha1DefaultContentMode(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, nil, func(kss *eventingv1alpha1.KafkaSinkSpec) error {
		kss.ContentMode = pointer.StringPtr("")
		return nil
	})
}

func TestKafkaSinkV1Alpha1StructuredContentMode(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, nil)
}

func TestKafkaSinkV1Alpha1BinaryContentMode(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeBinary, nil)
}

func TestKafkaSinkV1Alpha1AuthPlaintext(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, Plaintext, withBootstrap(BootstrapServersPlaintextArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSsl(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, Ssl, withBootstrap(BootstrapServersSslArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSaslPlaintextScram512(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, SaslPlaintextScram512, withBootstrap(BootstrapServersSaslPlaintextArr), withSecret)
}

func TestKafkaSinkV1Alpha1AuthSslSaslScram512(t *testing.T) {
	t.Skip("for now")
	testKafkaSink(t, eventingv1alpha1.ModeStructured, SslSaslScram512, withBootstrap(BootstrapServersSslSaslScramArr), withSecret)
}

func testKafkaSink(t *testing.T, mode string, sp SecretProvider, opts ...func(kss *eventingv1alpha1.KafkaSinkSpec) error) {
	RunMultiple(t, func(t *testing.T) {

		ctx := context.Background()

		const (
			kafkaSinkName = "kafka-sink"
		)

		client := testlib.Setup(t, false)
		defer testlib.TearDown(client)

		clientSet, err := eventingv1alpha1clientset.NewForConfig(client.Config)
		require.Nil(t, err)

		// Create a KafkaSink with the following spec.

		kss := eventingv1alpha1.KafkaSinkSpec{
			Topic:             "kafka-sink-" + client.Namespace,
			NumPartitions:     pointer.Int32Ptr(10),
			ReplicationFactor: func(rf int16) *int16 { return &rf }(1),
			BootstrapServers:  BootstrapServersPlaintextArr,
			ContentMode:       pointer.StringPtr(mode),
		}
		for _, opt := range opts {
			require.Nil(t, opt(&kss))
		}

		t.Log(kss)

		if sp != nil {
			secretData := sp(t, client)
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: client.Namespace,
					Name:      sinkSecretName,
				},
				Data: secretData,
			}
			secret, err = client.Kube.CoreV1().Secrets(client.Namespace).Create(ctx, secret, metav1.CreateOptions{})
			require.Nil(t, err)
			client.Tracker.Add(corev1.GroupName, "v1", "Secret", secret.Namespace, secret.Name)
		}

		createFunc := sink.CreatorV1Alpha1(clientSet, kss)

		kafkaSink, err := createFunc(types.NamespacedName{
			Namespace: client.Namespace,
			Name:      kafkaSinkName,
		})
		require.Nil(t, err)

		ks, err := clientSet.KafkaSinks(client.Namespace).Get(ctx, kafkaSinkName, metav1.GetOptions{})
		require.Nil(t, err)
		client.Tracker.AddObj(ks)

		client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

		// Send events to the KafkaSink.
		ids := addressable.Send(t, kafkaSink)

		// Read events from the topic.
		sink.Verify(t, client, mode, kss.Topic, ids)
	})
}

func withSecret(kss *eventingv1alpha1.KafkaSinkSpec) error {
	kss.Auth = &eventingv1alpha1.Auth{
		Secret: &eventingv1alpha1.Secret{
			Ref: &eventingv1alpha1.SecretReference{
				Name: sinkSecretName,
			},
		},
	}
	return nil
}

func withBootstrap(bs []string) func(kss *eventingv1alpha1.KafkaSinkSpec) error {
	return func(kss *eventingv1alpha1.KafkaSinkSpec) error {
		kss.BootstrapServers = bs
		return nil
	}
}

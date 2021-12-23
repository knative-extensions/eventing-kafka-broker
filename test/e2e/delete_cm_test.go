//go:build deletecm
// +build deletecm

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

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/pkg/system"

	eventingv1alpha1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
	eventingv1alpha1clientset "knative.dev/eventing-kafka-broker/control-plane/pkg/client/clientset/versioned/typed/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/test/pkg/addressable"
	"knative.dev/eventing-kafka-broker/test/pkg/sink"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

func TestDeleteSinkConfigMap(t *testing.T) {
	t.Skip()
	const (
		configMapName = "kafka-sink-sinks"
	)

	ctx := context.Background()

	client := testlib.Setup(t, false)
	defer testlib.TearDown(client)

	clientSet, err := eventingv1alpha1clientset.NewForConfig(client.Config)
	assert.Nil(t, err)

	// Create a KafkaSink with the following spec.

	kss := eventingv1alpha1.KafkaSinkSpec{
		Topic:             "kafka-sink-" + client.Namespace,
		NumPartitions:     pointer.Int32Ptr(10),
		ReplicationFactor: func(rf int16) *int16 { return &rf }(1),
		BootstrapServers:  testingpkg.BootstrapServersPlaintextArr,
	}

	createFunc := sink.CreatorV1Alpha1(clientSet, kss)

	kafkaSink, err := createFunc(types.NamespacedName{
		Namespace: client.Namespace,
		Name:      "kafka-sink",
	})
	assert.Nil(t, err)

	client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

	// Send events to the KafkaSink.
	ids := addressable.Send(t, kafkaSink)

	// Read events from the topic.
	sink.Verify(t, client, eventingv1alpha1.ModeStructured, kss.Topic, ids)

	t.Log("Deleting ConfigMap", configMapName)

	err = client.Kube.CoreV1().ConfigMaps(system.Namespace()).Delete(ctx, configMapName, metav1.DeleteOptions{})
	assert.Nil(t, err)

	err = wait.Poll(time.Second, 10*time.Second, func() (done bool, err error) {

		_, err = client.Kube.CoreV1().ConfigMaps(system.Namespace()).Get(ctx, configMapName, metav1.GetOptions{})

		return err == nil, nil
	})
	assert.Nil(t, err)

	client.WaitForResourceReadyOrFail(kafkaSink.Name, &kafkaSink.TypeMeta)

	// Send events to the KafkaSink.
	ids = addressable.Send(t, kafkaSink)

	// Read events from the topic.
	sink.Verify(t, client, eventingv1alpha1.ModeStructured, kss.Topic, ids)
}

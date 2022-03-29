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

package consumergroup

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	_ "knative.dev/eventing-kafka/pkg/client/injection/informers/sources/v1beta1/kafkasource/fake"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/apps/v1/statefulset/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/node/fake"
	_ "knative.dev/pkg/client/injection/kube/informers/core/v1/pod/fake"
	reconcilertesting "knative.dev/pkg/reconciler/testing"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumer/fake"
	_ "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/informers/eventing/v1alpha1/consumergroup/fake"
)

const (
	RefreshPeriod = "100"
	PodCapacity   = "20"
	// ConfigKafkaSchedulerName is the name of the ConfigMap to configure the scheduler.
	ConfigKafkaSchedulerName = "config-kafka-scheduler"
	// ConfigKafkaDeSchedulerName is the name of the ConfigMap to configure the descheduler.
	ConfigKafkaDeSchedulerName = "config-kafka-descheduler"
)

func TestNewController(t *testing.T) {
	ctx, _ := reconcilertesting.SetupFakeContext(t)

	t.Setenv("SYSTEM_NAMESPACE", systemNamespace)

	ctx, _ = kubeclient.With(ctx,
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ConfigKafkaSchedulerName,
				Namespace: systemNamespace,
			},
			Data: map[string]string{
				"predicates": `
			     [
			       {"Name": "PodFitsResources"},
			       {"Name": "NoMaxResourceCount", "Args": "{\"NumPartitions\": 100}"},
			       {"Name": "EvenPodSpread", "Args": "{\"MaxSkew\": 2}"}
			     ]`,
				"priorities": `
                [
                  {"Name": "AvailabilityZonePriority", "Weight": 10, "Args":  "{\"MaxSkew\": 2}"},
                  {"Name": "LowestOrdinalPriority", "Weight": 2}
                ]`,
			},
		},
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      ConfigKafkaDeSchedulerName,
				Namespace: systemNamespace,
			},
			Data: map[string]string{
				"predicates": `[]`,
				"priorities": `
                 [
                    {"Name": "RemoveWithEvenPodSpreadPriority", "Weight": 10, "Args": "{\"MaxSkew\": 2}"},
                    {"Name": "RemoveWithAvailabilityZonePriority", "Weight": 10, "Args":  "{\"MaxSkew\": 2}"},
                    {"Name": "RemoveWithHighestOrdinalPriority", "Weight": 2}
                 ]`,
			},
		},
	)

	t.Setenv("AUTOSCALER_REFRESH_PERIOD", RefreshPeriod)
	t.Setenv("POD_CAPACITY", PodCapacity)
	t.Setenv("SCHEDULER_CONFIG", ConfigKafkaSchedulerName)
	t.Setenv("DESCHEDULER_CONFIG", ConfigKafkaDeSchedulerName)
	controller := NewController(ctx)
	if controller == nil {
		t.Error("failed to create controller: <nil>")
	}
}

func TestEnqueueConsumerFromConsumerGroup(t *testing.T) {

	capture := types.NamespacedName{}

	f := enqueueConsumerGroupFromConsumer(func(name types.NamespacedName) {
		// Make sure this is called only once.
		require.Empty(t, capture.Namespace)
		require.Empty(t, capture.Name)

		capture = name
	})

	ns := "ns"
	or := metav1.OwnerReference{
		APIVersion: kafkainternals.SchemeGroupVersion.String(),
		Kind:       kafkainternals.ConsumerGroupGroupVersionKind.Kind,
		Name:       "hello",
	}

	f(&kafkainternals.Consumer{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			OwnerReferences: []metav1.OwnerReference{or},
		},
	})

	require.Equal(t, or.Name, capture.Name)
	require.Equal(t, ns, capture.Namespace)
}

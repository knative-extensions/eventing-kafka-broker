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

package featuressteps

import (
	"context"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/propagator"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/e2e_new/resources/configmap/broker"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"

	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"

	kubeclient "knative.dev/pkg/client/injection/kube/client"

	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/resources/svc"
)

func compose(steps ...feature.StepFn) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		for _, s := range steps {
			s(ctx, t)
		}
	}
}

func BrokerSmokeTest(brokerName, triggerName string) feature.StepFn {

	sink := feature.MakeRandomK8sName("sink")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	eventMatchers := []cetest.EventMatcher{
		cetest.HasId(event.ID()),
		cetest.HasSource(event.Source()),
		cetest.HasType(event.Type()),
		cetest.HasSubject(event.Subject()),
	}

	backoffPolicy := eventingduck.BackoffPolicyLinear

	configName := feature.MakeRandomK8sName("config")

	return compose(
		eventshub.Install(sink, eventshub.StartReceiver),
		brokerconfigmap.Install(
			configName,
			brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersPlaintext),
			brokerconfigmap.WithNumPartitions(1),
			brokerconfigmap.WithReplicationFactor(1),
		),
		broker.Install(brokerName, append([]manifest.CfgFn{broker.WithConfig(configName)}, broker.WithEnvConfig()...)...),
		broker.IsReady(brokerName),
		trigger.Install(
			triggerName,
			brokerName,
			trigger.WithRetry(3, &backoffPolicy, pointer.String("PT1S")),
			trigger.WithSubscriber(svc.AsKReference(sink), ""),
		),
		trigger.IsReady(triggerName),
		eventshub.Install(
			feature.MakeRandomK8sName("source"),
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.AddSequence,
			eventshub.InputEvent(event),
		),
		assert.OnStore(sink).MatchEvent(eventMatchers...).Exact(1),
	)
}

func DeleteResources(f *feature.Feature) feature.StepFn {
	return compose(f.DeleteResources)
}

func DeleteConfigMap(name string) feature.StepFn {
	foreground := metav1.DeletePropagationForeground
	return func(ctx context.Context, t feature.T) {
		ns := environment.FromContext(ctx).Namespace()
		err := kubeclient.Get(ctx).CoreV1().
			ConfigMaps(ns).
			Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foreground})
		if err != nil {
			t.Fatalf("Failed to delete ConfigMap %s/%s: %w\n", ns, name, err)
		}
	}
}

func DeleteBroker(name string) feature.StepFn {
	foreground := metav1.DeletePropagationForeground
	return func(ctx context.Context, t feature.T) {
		ns := environment.FromContext(ctx).Namespace()
		err := eventingclient.Get(ctx).EventingV1().
			Brokers(ns).
			Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &foreground})
		if err != nil {
			t.Fatalf("Failed to delete Broker %s/%s: %w\n", ns, name, err)
		}

		interval, timeout := environment.PollTimingsFromContext(ctx)

		err = wait.PollImmediate(interval, timeout, func() (bool, error) {
			br, err := eventingclient.Get(ctx).
				EventingV1().
				Brokers(ns).
				Get(ctx, name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				t.Logf("Failed to get broker %s: %w", name, err)
				return false, err
			}

			b, _ := json.MarshalIndent(br, "", " ")
			t.Logf("Broker %s still present\n%s\n", name, b)

			return false, nil
		})
		require.Nil(t, err)
	}
}

func AddAdditionalResourcesToPropagationConfigMap(cmName string, additionalResources ...unstructured.Unstructured) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		cm, err := kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(ctx, cmName, metav1.GetOptions{})
		if err != nil {
			t.Errorf("Failed to get ConfigMap %s/%s: %w", system.Namespace(), cmName, err)
		}

		resources, err := propagator.UnmarshalTemplate(cm)
		if err != nil {
			t.Fatal("Failed to unmarshal resources from ConfigMap %s/%s: %w\n%s", system.Namespace(), cmName, err, cm.Data["resources"])
		}

		resources.Resources = append(resources.Resources, additionalResources...)

		value, err := propagator.Marshal(resources)
		if err != nil {
			t.Fatal(err)
		}

		cm = &corev1.ConfigMap{
			TypeMeta:   cm.TypeMeta,
			ObjectMeta: cm.ObjectMeta,
			Data: map[string]string{
				"resources": value,
			},
		}

		_, err = kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Update(ctx, cm, metav1.UpdateOptions{})
		if err != nil {
			t.Fatal("Failed to update ConfigMap %s/%s: %w", cm.GetNamespace(), cm.GetName(), err)
		}
	}
}

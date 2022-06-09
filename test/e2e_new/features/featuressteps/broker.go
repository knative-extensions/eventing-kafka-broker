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
	"fmt"
	"strings"
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	"knative.dev/eventing/test/rekt/resources/broker"
	"knative.dev/eventing/test/rekt/resources/trigger"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/injection/clients/dynamicclient"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
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

	return compose(
		eventshub.Install(sink, eventshub.StartReceiver),
		broker.Install(brokerName, broker.WithEnvConfig()...),
		broker.IsReady(brokerName),
		trigger.Install(
			triggerName,
			brokerName,
			trigger.WithRetry(3, &backoffPolicy, pointer.StringPtr("PT1S")),
			trigger.WithSubscriber(svc.AsKReference(sink), ""),
		),
		trigger.IsReady(triggerName),
		eventshub.Install(
			feature.MakeRandomK8sName("source"),
			eventshub.StartSenderToResource(broker.GVR(), brokerName),
			eventshub.AddSequence,
			eventshub.AddTracing,
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

func DeleteKnativeResources(f *feature.Feature) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		refs := f.References()
		eg := errgroup.Group{}

		for _, ref := range refs {
			if strings.Contains(ref.APIVersion, "knative.dev") {
				dc := dynamicclient.Get(ctx)
				gvr, _ := meta.UnsafeGuessKindToResource(ref.GroupVersionKind())
				if err := dc.Resource(gvr).Namespace(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{}); err != nil {
					t.Errorf("failed to delete resource %+v: %v", ref, err)
				}

				eg.Go(func() error {
					var lastState *unstructured.Unstructured
					err := wait.Poll(time.Second, time.Minute, func() (done bool, err error) {
						lastState, err = dc.Resource(gvr).Namespace(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
						if apierrors.IsNotFound(err) {
							return true, nil
						}
						return false, nil
					})
					if err != nil {
						b, _ := json.MarshalIndent(lastState.UnstructuredContent(), "", " ")
						return fmt.Errorf("failed to wait for resource deletion %+v: %v, last state: \n%s\n", ref, err, string(b))
					}
					return nil
				})
			}
		}

		if err := eg.Wait(); err != nil {
			t.Error(err)
		}
	}
}

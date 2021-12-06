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

package helpers

import (
	"context"
	"fmt"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	. "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	channelsv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	contribtestlib "knative.dev/eventing-kafka/test/lib"
	testlib "knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/recordevents"
	"knative.dev/eventing/test/lib/resources"
)

const (
	kafkaChannelName    = "kafka-sub-ready-channel"
	kafkaSub0           = "kafka-sub-0"
	kafkaSub1           = "kafka-sub-1"
	recordEventsPodName = "e2e-channel-sub-ready-recordevents-pod"
	eventSenderName     = "e2e-channel-event-sender-pod"
)

func scaleDispatcherDeployment(ctx context.Context, t *testing.T, desiredReplicas int32, client *testlib.Client) {
	dispatcherDeployment, err := client.Kube.AppsV1().Deployments("knative-eventing").Get(ctx, "kafka-ch-dispatcher", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unable to get kafka-ch-dispatcher deployment: %v", err)
	}
	if *dispatcherDeployment.Spec.Replicas != desiredReplicas {
		desired := dispatcherDeployment.DeepCopy()
		*desired.Spec.Replicas = desiredReplicas
		_, err := client.Kube.AppsV1().Deployments("knative-eventing").Update(ctx, desired, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Unable to update kafka-ch-dispatcher deployment to %d replica: %v", desiredReplicas, err)
		}
		// we actively do NOT wait for deployments to be ready so we can check state below
	}
}

func readyDispatcherPodsCheck(ctx context.Context, t *testing.T, client *testlib.Client) int32 {
	dispatcherDeployment, err := client.Kube.AppsV1().Deployments("knative-eventing").Get(ctx, "kafka-ch-dispatcher", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unable to get kafka-ch-dispatcher deployment: %v", err)
	}
	return dispatcherDeployment.Status.ReadyReplicas
}

func createKafkaChannel(client *testlib.Client, kafkaChannelMeta metav1.TypeMeta, kafkaChannelName string) {
	kafkaChannelV1Beta1 := &channelsv1beta1.KafkaChannel{
		ObjectMeta: metav1.ObjectMeta{
			Name: kafkaChannelName,
		},
		Spec: channelsv1beta1.KafkaChannelSpec{
			NumPartitions: 3,
		},
	}
	contribtestlib.CreateKafkaChannelV1Beta1OrFail(client, kafkaChannelV1Beta1)
}

func ChannelSubscriptionScaleReadyHelper(
	ctx context.Context,
	t *testing.T,
	channelTestRunner testlib.ComponentsTestRunner,
	options ...testlib.SetupClientOption) {

	eventSource := fmt.Sprintf("http://%s.svc/", eventSenderName)

	channelTestRunner.RunTests(t, testlib.FeatureBasic, func(st *testing.T, kafkaChannelMeta metav1.TypeMeta) {
		client := testlib.Setup(st, true, options...)
		defer testlib.TearDown(client)

		scaleDispatcherDeployment(ctx, st, 1, client)
		createKafkaChannel(client, kafkaChannelMeta, kafkaChannelName)
		client.WaitForResourceReadyOrFail(kafkaChannelName, &kafkaChannelMeta)

		eventTracker, _ := recordevents.StartEventRecordOrFail(ctx, client, recordEventsPodName)
		client.CreateSubscriptionOrFail(
			kafkaSub0,
			kafkaChannelName,
			&kafkaChannelMeta,
			resources.WithSubscriberForSubscription(recordEventsPodName),
		)
		client.WaitForAllTestResourcesReadyOrFail(ctx)

		scaleDispatcherDeployment(ctx, st, 4, client)
		client.WaitForResourceReadyOrFail(kafkaSub0, testlib.SubscriptionTypeMeta) //this should still be ready

		client.CreateSubscriptionOrFail(
			kafkaSub1,
			kafkaChannelName,
			&kafkaChannelMeta,
			resources.WithSubscriberForSubscription(recordEventsPodName),
		)
		for readyDispatcherPodsCheck(ctx, st, client) < 3 {
			subObj, err := client.Eventing.MessagingV1().Subscriptions(client.Namespace).Get(ctx, kafkaSub1, metav1.GetOptions{})
			if err != nil {
				st.Fatalf("Could not get v1 subscription object %q: %v", subObj.Name, err)
			}
			if subObj.Status.IsReady() {
				st.Fatalf("Subscription: %s, marked ready before dispatcher pods ready", subObj.Name)
			}
		}
		client.WaitForResourceReadyOrFail(kafkaSub1, testlib.SubscriptionTypeMeta)
		// send CloudEvent to the first channel
		event := cloudevents.NewEvent()
		event.SetID("test")
		event.SetSource(eventSource)
		event.SetType(testlib.DefaultEventType)

		body := fmt.Sprintf(`{"msg":"TestSingleEvent %s"}`, uuid.New().String())
		if err := event.SetData(cloudevents.ApplicationJSON, []byte(body)); err != nil {
			st.Fatalf("Cannot set the payload of the event: %s", err.Error())
		}

		client.SendEventToAddressable(ctx, eventSenderName, kafkaChannelName, &kafkaChannelMeta, event)
		// verify the logger service receives the event
		eventTracker.AssertAtLeast(1, recordevents.MatchEvent(
			HasSource(eventSource),
			HasData([]byte(body)),
		))
	})
}

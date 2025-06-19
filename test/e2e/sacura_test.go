//go:build sacura
// +build sacura

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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
	messaging "knative.dev/eventing/pkg/apis/messaging/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	testlib "knative.dev/eventing/test/lib"
	pointer "knative.dev/pkg/ptr"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"

	pkgtest "knative.dev/eventing-kafka-broker/test/pkg"
	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	app                            = "sacura"
	sacuraVerifyCommittedOffsetJob = "verify-committed-offset"

	sacuraChannelName      = "channel"
	sacuraTriggerName      = "trigger"
	sacuraSubscriptionName = "subscription"
	sacuraSourceName       = "source"

	pollTimeout  = 40 * time.Minute
	pollInterval = 10 * time.Second
)

type SacuraTestConfig struct {
	// Namespace is the test namespace.
	Namespace string

	ConsumerResourceGVR schema.GroupVersionResource

	// BrokerTopic is the expected Broker topic.
	// It's used to verify the committed offset.
	BrokerTopic *string

	// ChannelTopic is the expected Channel topic.
	// It's used to verify the committed offset.
	ChannelTopic *string

	// SourceTopic is the Source topic.
	// It's used to verify the committed offset.
	SourceTopic *string
}

func TestSacuraSinkSourceJob(t *testing.T) {
	runSacuraTest(t, SacuraTestConfig{
		Namespace:           "sacura-sink-source",
		ConsumerResourceGVR: sources.SchemeGroupVersion.WithResource("kafkasources"),
		SourceTopic:         pointer.String("sacura-sink-source-topic"),
	})
}

func TestSacuraBrokerJob(t *testing.T) {
	runSacuraTest(t, SacuraTestConfig{
		Namespace:           "sacura",
		ConsumerResourceGVR: eventing.SchemeGroupVersion.WithResource("triggers"),
		BrokerTopic:         pointer.String("knative-broker-sacura-sink-source-broker"),
	})
}

type Event struct {
	GVR   schema.GroupVersionResource
	Event watch.Event
}

func runSacuraTest(t *testing.T, config SacuraTestConfig) {

	c := testlib.Setup(t, false)
	defer testlib.TearDown(c)

	ctx := context.Background()

	out := make(chan Event)

	watchContext, watchCancel := context.WithCancel(ctx)

	watchUserFacingResource := watchResource(t, watchContext, c.Dynamic, config.Namespace, config.ConsumerResourceGVR, out)
	t.Cleanup(watchUserFacingResource.Stop)

	watchConsumerGroups := watchResource(t, watchContext, c.Dynamic, config.Namespace, kafkainternals.SchemeGroupVersion.WithResource("consumergroups"), out)
	t.Cleanup(watchConsumerGroups.Stop)

	watchConsumer := watchResource(t, watchContext, c.Dynamic, config.Namespace, kafkainternals.SchemeGroupVersion.WithResource("consumers"), out)
	t.Cleanup(watchConsumer.Stop)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for e := range out {
			bytes, _ := json.MarshalIndent(e, "", "  ")
			t.Logf("Resource %q changed:\n%s\n\n", e.GVR.String(), string(bytes))
		}
	}()

	jobPollError := wait.Poll(pollInterval, pollTimeout, func() (done bool, err error) {
		job, err := c.Kube.BatchV1().Jobs(config.Namespace).Get(ctx, app, metav1.GetOptions{})
		assert.Nil(t, err)

		return isJobSucceeded(job)
	})

	watchCancel()
	close(out)
	wg.Wait()

	pkgtesting.LogJobOutput(t, ctx, c.Kube, config.Namespace, app)

	if jobPollError != nil {
		t.Fatal(jobPollError)
	}

	topics := map[*string]func(t *testing.T) string{
		config.BrokerTopic:  getKafkaTriggerConsumerGroup(ctx, c.Eventing, config.Namespace),
		config.ChannelTopic: getKafkaSubscriptionConsumerGroup(ctx, c.Dynamic, config.Namespace),
		config.SourceTopic:  getKafkaSourceConsumerGroup(ctx, c.Dynamic, config.Namespace),
	}

	t.Run("verify committed offset", func(t *testing.T) {
		for topic, groupFunc := range topics {
			if topic == nil {
				continue
			}

			t.Run(*topic, func(t *testing.T) {
				t.Log(strings.Repeat("-", 30))
				t.Log("Verify committed offset")
				t.Log(strings.Repeat("-", 30))

				consumerGroup := groupFunc(t)

				err := kafkatest.VerifyCommittedOffset(
					c.Kube,
					c.Tracker,
					types.NamespacedName{
						Namespace: config.Namespace,
						Name:      names.SimpleNameGenerator.GenerateName(sacuraVerifyCommittedOffsetJob + "-" + *topic),
					},
					&kafkatest.AdminConfig{
						BootstrapServers: pkgtest.BootstrapServersPlaintext,
						Topic:            *topic,
						Group:            consumerGroup,
					},
				)
				require.Nil(t, err, "Failed to verify committed offset")
			})
		}
	})
}

func isJobSucceeded(job *batchv1.Job) (bool, error) {
	if job.Status.Failed > 0 {
		return false, fmt.Errorf("job %s/%s failed", job.Namespace, job.Name)
	}

	return job.Status.Succeeded > 0, nil
}

func getKafkaTriggerConsumerGroup(ctx context.Context, c *eventingclientset.Clientset, ns string) func(t *testing.T) string {
	return func(t *testing.T) string {
		trigger, err := c.EventingV1().Triggers(ns).Get(ctx, sacuraTriggerName, metav1.GetOptions{})
		require.Nil(t, err, "Failed to get trigger %s/%s: %v", ns, sacuraTriggerName)
		groupId, ok := trigger.Status.Annotations[kafka.GroupIdAnnotation]
		require.True(t, ok, "Group ID annotation not set on trigger")
		return groupId
	}
}

func getKafkaSourceConsumerGroup(ctx context.Context, c dynamic.Interface, ns string) func(t *testing.T) string {
	return func(t *testing.T) string {
		gvr := sources.SchemeGroupVersion.WithResource("kafkasources")
		ksUnstr, err := c.Resource(gvr).Namespace(ns).Get(ctx, sacuraSourceName, metav1.GetOptions{})
		require.Nilf(t, err, "%+v %s/%s", gvr, ns, sacuraSourceName)

		ks := sources.KafkaSource{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ksUnstr.UnstructuredContent(), &ks)
		require.Nil(t, err)
		require.NotEmpty(t, ks.Spec.ConsumerGroup)

		return ks.Spec.ConsumerGroup
	}
}

func getKafkaSubscriptionConsumerGroup(ctx context.Context, c dynamic.Interface, ns string) func(t *testing.T) string {
	return func(t *testing.T) string {
		gvr := messaging.SchemeGroupVersion.WithResource("subscriptions")
		subUnstr, err := c.Resource(gvr).Namespace(ns).Get(ctx, sacuraSubscriptionName, metav1.GetOptions{})
		require.Nil(t, err)

		sub := messaging.Subscription{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(subUnstr.UnstructuredContent(), &sub)
		require.Nil(t, err)

		return fmt.Sprintf("kafka.%s.%s.%s", c, sacuraChannelName, string(sub.UID))
	}
}

func watchResource(t *testing.T, ctx context.Context, dynamic dynamic.Interface, ns string, gvr schema.GroupVersionResource, out chan<- Event) watch.Interface {

	w, err := dynamic.Resource(gvr).
		Namespace(ns).
		Watch(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal("Failed to watch resource", gvr, err)
	}

	go func() {
		for e := range w.ResultChan() {
			select {
			case <-ctx.Done():
				return
			default:
				out <- Event{
					GVR:   gvr,
					Event: e,
				}
			}
		}
	}()

	return w
}

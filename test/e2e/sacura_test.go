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
	"fmt"
	"strings"
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
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/utils/pointer"
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	messaging "knative.dev/eventing/pkg/apis/messaging/v1"
	eventingclientset "knative.dev/eventing/pkg/client/clientset/versioned"
	testlib "knative.dev/eventing/test/lib"

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
		Namespace:    "sacura-sink-source",
		SourceTopic:  pointer.StringPtr("sacura-sink-source-topic"),
	})
}

func TestSacuraBrokerJob(t *testing.T) {
	runSacuraTest(t, SacuraTestConfig{
		Namespace:    "sacura",
		BrokerTopic:  pointer.StringPtr("knative-broker-sacura-sink-source-broker"),
	})
}

func runSacuraTest(t *testing.T, config SacuraTestConfig) {

	c := testlib.Setup(t, false)
	defer testlib.TearDown(c)

	ctx := context.Background()

	jobPollError := wait.Poll(pollInterval, pollTimeout, func() (done bool, err error) {
		job, err := c.Kube.BatchV1().Jobs(config.Namespace).Get(ctx, app, metav1.GetOptions{})
		assert.Nil(t, err)

		return isJobSucceeded(job)
	})

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
						BootstrapServers: pkgtesting.BootstrapServersPlaintext,
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
		return string(trigger.UID)
	}
}

func getKafkaSourceConsumerGroup(ctx context.Context, c dynamic.Interface, ns string) func(t *testing.T) string {
	return func(t *testing.T) string {
		gvr := schema.GroupVersionResource{
			Group:    sources.SchemeGroupVersion.Group,
			Version:  sources.SchemeGroupVersion.Version,
			Resource: "kafkasource",
		}
		ksUnstr, err := c.Resource(gvr).Namespace(ns).Get(ctx, sacuraSourceName, metav1.GetOptions{})
		require.Nil(t, err)

		ks := sources.KafkaSource{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ksUnstr.UnstructuredContent(), &ks)
		require.Nil(t, err)
		require.NotEmpty(t, ks.Spec.ConsumerGroup)

		return ks.Spec.ConsumerGroup
	}
}

func getKafkaSubscriptionConsumerGroup(ctx context.Context, c dynamic.Interface, ns string) func(t *testing.T) string {
	return func(t *testing.T) string {
		gvr := schema.GroupVersionResource{
			Group:    messaging.SchemeGroupVersion.Group,
			Version:  messaging.SchemeGroupVersion.Version,
			Resource: "subscription",
		}
		subUnstr, err := c.Resource(gvr).Namespace(ns).Get(ctx, sacuraSubscriptionName, metav1.GetOptions{})
		require.Nil(t, err)

		sub := messaging.Subscription{}
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(subUnstr.UnstructuredContent(), &sub)
		require.Nil(t, err)

		return fmt.Sprintf("kafka.%s.%s.%s", c, sacuraChannelName, string(sub.UID))
	}
}

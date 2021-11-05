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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"

	kafkatest "knative.dev/eventing-kafka-broker/test/pkg/kafka"
	pkgtesting "knative.dev/eventing-kafka-broker/test/pkg/testing"
)

const (
	app                            = "sacura"
	sacuraVerifyCommittedOffsetJob = "verify-committed-offset"
	sacuraTriggerName              = "trigger"

	pollTimeout  = 30 * time.Minute
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

func TestSacuraJob(t *testing.T) {
	runSacuraTest(t, SacuraTestConfig{
		Namespace:   "sacura",
		BrokerTopic: pointer.StringPtr("knative-broker-sacura-sacura"),
	})
}

func TestSacuraSinkSourceBrokerChannelJob(t *testing.T) {
	runSacuraTest(t, SacuraTestConfig{
		Namespace:   "sacura-sink-source-broker-channel",
		BrokerTopic: pointer.StringPtr("knative-broker-sacura-sink-source-broker-channel-broker"),
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

	if config.BrokerTopic != nil {
		t.Run("verify committed offset", func(t *testing.T) {
			t.Log(strings.Repeat("-", 30))
			t.Log("Verify committed offset")
			t.Log(strings.Repeat("-", 30))

			trigger, err := c.Eventing.EventingV1().Triggers(config.Namespace).Get(ctx, sacuraTriggerName, metav1.GetOptions{})
			require.Nil(t, err, "Failed to get trigger %s/%s: %v", config.Namespace, sacuraTriggerName)

			err = kafkatest.VerifyCommittedOffset(
				c.Kube,
				c.Tracker,
				types.NamespacedName{
					Namespace: config.Namespace,
					Name:      sacuraVerifyCommittedOffsetJob,
				},
				&kafkatest.AdminConfig{
					BootstrapServers: pkgtesting.BootstrapServersPlaintext,
					Topic:            *config.BrokerTopic,
					Group:            string(trigger.UID),
				},
			)
			require.Nil(t, err, "Failed to verify committed offset")
		})
	}
}

func isJobSucceeded(job *batchv1.Job) (bool, error) {
	if job.Status.Failed > 0 {
		return false, fmt.Errorf("job %s/%s failed", job.Namespace, job.Name)
	}

	return job.Status.Succeeded > 0, nil
}

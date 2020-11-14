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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	testlib "knative.dev/eventing/test/lib"
)

func TestSacuraJob(t *testing.T) {

	c := testlib.Setup(t, false)
	defer testlib.TearDown(c)

	ctx := context.Background()

	const (
		app       = "sacura"
		namespace = "sacura"
	)

	jobPollError := wait.Poll(10*time.Second, 10*time.Minute, func() (done bool, err error) {
		job, err := c.Kube.BatchV1().Jobs(namespace).Get(ctx, app, metav1.GetOptions{})
		assert.Nil(t, err)

		return isJobSucceeded(job)
	})

	logJobOutput(t, app, c, namespace, ctx)

	if jobPollError != nil {
		t.Fatal(jobPollError)
	}
}

func isJobSucceeded(job *batchv1.Job) (bool, error) {
	if job.Status.Failed > 0 {
		return false, fmt.Errorf("job %s/%s failed", job.Namespace, job.Name)
	}

	return job.Status.Succeeded > 0, nil
}

func logJobOutput(t *testing.T, app string, c *testlib.Client, namespace string, ctx context.Context) {

	selector := labels.SelectorFromSet(map[string]string{"app": app})

	pods, err := c.Kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	assert.Nil(t, err)
	assert.Greater(t, len(pods.Items), 0)

	logs := make([]string, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if l, err := c.Kube.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx); err != nil {
			logs = append(logs, err.Error())
		} else {
			logs = append(logs, string(l))
		}
	}

	t.Log(strings.Join(logs, "\n"))
	testlib.ExportLogs(namespace, namespace)
}

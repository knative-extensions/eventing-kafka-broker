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

package testing

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	testlib "knative.dev/eventing/test/lib"
)

func LogJobOutput(t *testing.T, ctx context.Context, c *testlib.Client, namespace string, app string) {

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

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

package kafka

import (
	"context"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	testlib "knative.dev/eventing/test/lib"
	pointer "knative.dev/pkg/ptr"
	pkgtest "knative.dev/pkg/test"
)

const (
	committedOffsetImage = "committed-offset"
)

type AdminConfig struct {
	BootstrapServers string `json:"bootstrapServers" required:"true" split_words:"true"`
	Topic            string `json:"topic" required:"true" split_words:"true"`
	Group            string `json:"group" required:"true" split_words:"true"`
}

func VerifyLagEquals(
	client kubernetes.Interface,
	tracker *testlib.Tracker,
	namespacedName types.NamespacedName,
	config *AdminConfig,
	expectedLag uint64) error {

	ctx := context.Background()

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespacedName.Namespace,
			Name:      namespacedName.Name,
			Labels: map[string]string{
				"app": namespacedName.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32(2),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": namespacedName.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            namespacedName.Name,
							Image:           pkgtest.ImagePath(committedOffsetImage),
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env: []corev1.EnvVar{
								{
									Name:  "BOOTSTRAP_SERVERS",
									Value: config.BootstrapServers,
								},
								{
									Name:  "TOPIC",
									Value: config.Topic,
								},
								{
									Name:  "GROUP",
									Value: config.Group,
								},
								{
									Name:  "EXPECTED_LAG",
									Value: strconv.FormatUint(expectedLag, 10),
								},
							},
						},
					},
					RestartPolicy: "Never",
				},
			},
		},
	}
	return verifyJobSucceeded(ctx, client, tracker, namespacedName, job)
}

func VerifyCommittedOffset(
	client kubernetes.Interface,
	tracker *testlib.Tracker,
	namespacedName types.NamespacedName,
	config *AdminConfig) error {

	return VerifyLagEquals(client, tracker, namespacedName, config, 0)
}

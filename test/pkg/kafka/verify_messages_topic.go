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

package kafka

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/pointer"
	testlib "knative.dev/eventing/test/lib"
	pkgtest "knative.dev/pkg/test"
)

const (
	kafkaConsumerImage = "kafka-consumer"
)

type ConsumerConfig struct {
	BootstrapServers string `json:"bootstrapServers" required:"true" split_words:"true"`
	Topic            string `json:"topic" required:"true" split_words:"true"`
	IDS              string `json:"ids" required:"true" split_words:"true"`
	ContentMode      string `json:"contentMode" required:"true" split_words:"true"`
}

func VerifyMessagesInTopic(
	client kubernetes.Interface,
	tracker *testlib.Tracker,
	namespacedName types.NamespacedName,
	config *ConsumerConfig) error {

	ctx := context.Background()

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespacedName.Namespace,
			Name:      namespacedName.Name,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32Ptr(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            namespacedName.Name,
							Image:           pkgtest.ImagePath(kafkaConsumerImage),
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
									Name:  "IDS",
									Value: config.IDS,
								},
								{
									Name:  "CONTENT_MODE",
									Value: config.ContentMode,
								},
							},
						},
					},
					RestartPolicy: "Never",
				},
			},
		},
	}
	job, err := client.BatchV1().Jobs(namespacedName.Namespace).Create(ctx, job, metav1.CreateOptions{})

	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	gvr, _ := meta.UnsafeGuessKindToResource(job.GroupVersionKind())
	tracker.Add(gvr.Group, gvr.Version, gvr.Resource, namespacedName.Namespace, namespacedName.Name)

	return wait.PollImmediate(interval, timeout, func() (bool, error) {
		job, err := client.BatchV1().Jobs(namespacedName.Namespace).Get(ctx, namespacedName.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get job: %w", err)
		}
		if job.Status.Succeeded >= 1 {
			return true, nil
		}
		return false, nil
	})
}

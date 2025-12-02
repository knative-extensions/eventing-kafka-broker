/*
Copyright 2020 The Knative Authors

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
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	testlib "knative.dev/eventing/test/lib"
	pkgtest "knative.dev/pkg/test"
)

const (
	strimziAPIGroup      = "kafka.strimzi.io"
	strimziAPIVersion    = "v1beta2"
	strimziTopicResource = "kafkatopics"
	strimziUserResource  = "kafkausers"
	interval             = 3 * time.Second
	timeout              = 4 * time.Minute
	kcatImage            = "quay.io/openshift-knative/kcat:1.7.1"
)

var (
	topicGVR = schema.GroupVersionResource{Group: strimziAPIGroup, Version: strimziAPIVersion, Resource: strimziTopicResource}
	userGVR  = schema.GroupVersionResource{Group: strimziAPIGroup, Version: strimziAPIVersion, Resource: strimziUserResource} //nolint:unused
	ImcGVR   = schema.GroupVersionResource{Group: "messaging.knative.dev", Version: "v1", Resource: "inmemorychannels"}       //nolinte:unused
)

func MustPublishKafkaMessage(client *testlib.Client, bootstrapServer string, topic string, key string, headers map[string]string, transactionalID string, value string) {
	cgName := topic + "-" + key + "z"

	payload := value
	if key != "" {
		payload = key + "=" + value
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cgName,
			Namespace: client.Namespace,
		},
		Data: map[string]string{
			"payload": payload,
		},
	}
	_, err := client.Kube.CoreV1().ConfigMaps(client.Namespace).Create(context.Background(), cm, metav1.CreateOptions{})
	if err != nil {
		if !apierrs.IsAlreadyExists(err) {
			client.T.Fatalf("Failed to create configmap %q: %v", cgName, err)
			return
		}
		if _, err = client.Kube.CoreV1().ConfigMaps(client.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
			client.T.Fatalf("failed to update configmap: %q: %v", cgName, err)
		}
	}

	client.Tracker.Add(corev1.SchemeGroupVersion.Group, corev1.SchemeGroupVersion.Version, "configmap", client.Namespace, cgName)

	args := []string{"-P", "-T", "-b", bootstrapServer, "-t", topic}
	if transactionalID != "" {
		args = append(args, "-X", "transactional.id="+transactionalID)
	}
	if key != "" {
		args = append(args, "-K=")
	}
	for k, v := range headers {
		args = append(args, "-H", k+"="+v)
	}
	args = append(args, "-l", "/etc/mounted/payload")

	client.T.Logf("Running kcat %s", strings.Join(args, " "))

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      uuid.New().String() + "-producer",
			Namespace: client.Namespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Image:   kcatImage,
				Name:    cgName + "-producer-container",
				Command: []string{"kcat"},
				Args:    args,
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "event-payload",
					MountPath: "/etc/mounted",
				}},
			}},
			RestartPolicy: corev1.RestartPolicyNever,
			Volumes: []corev1.Volume{{
				Name: "event-payload",
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cgName,
					},
				}},
			}},
		},
	}
	client.CreatePodOrFail(&pod)

	err = pkgtest.WaitForPodState(context.Background(), client.Kube, func(pod *corev1.Pod) (b bool, e error) {
		if pod.Status.Phase == corev1.PodFailed {
			return true, fmt.Errorf("aggregator pod failed with message %s", pod.Status.Message)
		} else if pod.Status.Phase != corev1.PodSucceeded {
			return false, nil
		}
		return true, nil
	}, pod.Name, pod.Namespace)
	if err != nil {
		client.T.Fatalf("Failed waiting for pod for completeness %q: %v", pod.Name, err)
	}
}

func MustCreateTopic(client *testlib.Client, clusterName, clusterNamespace, topicName string, partitions int) {
	obj := unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": topicGVR.GroupVersion().String(),
			"kind":       "KafkaTopic",
			"metadata": map[string]interface{}{
				"name": topicName,
				"labels": map[string]interface{}{
					"strimzi.io/cluster": clusterName,
				},
			},
			"spec": map[string]interface{}{
				"partitions": partitions,
				"replicas":   1,
			},
		},
	}

	_, err := client.Dynamic.Resource(topicGVR).Namespace(clusterNamespace).Create(context.Background(), &obj, metav1.CreateOptions{})
	if err != nil {
		client.T.Fatalf("Error while creating the topic %s: %v", topicName, err)
	}
	client.Tracker.Add(topicGVR.Group, topicGVR.Version, topicGVR.Resource, clusterNamespace, topicName)

	// Wait for the topic to be ready
	if err := WaitForKafkaResourceReady(context.Background(), client, clusterNamespace, topicName, topicGVR); err != nil {
		client.T.Fatalf("Error while creating the topic %s: %v", topicName, err)
	}
}

func WaitForKafkaResourceReady(ctx context.Context, client *testlib.Client, namespace, name string, gvr schema.GroupVersionResource) error {
	like := &duckv1.KResource{}
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx2 context.Context) (bool, error) {
		us, err := client.Dynamic.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrs.IsNotFound(err) {
				log.Println(namespace, name, "not found", err)
				// keep polling
				return false, nil
			}
			return false, err
		}
		obj := like.DeepCopy()
		if err = runtime.DefaultUnstructuredConverter.FromUnstructured(us.Object, obj); err != nil {
			log.Fatalf("Error DefaultUnstructured.Dynamiconverter. %v", err)
		}
		obj.ResourceVersion = gvr.Version
		obj.APIVersion = gvr.GroupVersion().String()

		// First see if the resource has conditions.
		if len(obj.Status.Conditions) == 0 {
			return false, nil // keep polling
		}

		ready := obj.Status.GetCondition(apis.ConditionReady)
		if ready != nil && ready.Status == corev1.ConditionTrue {
			return true, nil
		}

		return false, nil
	})
}

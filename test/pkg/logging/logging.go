/*
 * Copyright 2024 The Knative Authors
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

package logging

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/ptr"
)

type Logger interface {
	Start() error
}

type logger struct {
	labelsByNamespace map[string][]string
	kc                kubernetes.Interface
	ctx               context.Context
}

func NewLogger(ctx context.Context, client kubernetes.Interface, labelsByNamespace map[string][]string) Logger {
	return &logger{
		ctx:               ctx,
		kc:                client,
		labelsByNamespace: labelsByNamespace,
	}
}

func (l *logger) Start() error {
	if len(l.labelsByNamespace) == 0 {
		return fmt.Errorf("you need to have at least one namespace to get logs from")
	}

	artifacts := os.Getenv("ARTIFACTS")

	if _, err := os.Stat(fmt.Sprintf("%s/new-logs", artifacts)); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(fmt.Sprintf("%s/new-logs", artifacts), os.ModePerm)
		if err != nil {
			return err
		}
	}

	for ns, ls := range l.labelsByNamespace {
		requirement, err := labels.NewRequirement("app", selection.In, ls)
		if err != nil {
			return err
		}

		selector := labels.NewSelector().Add(*requirement)
		watcher, err := l.kc.CoreV1().Pods(ns).Watch(l.ctx, metav1.ListOptions{LabelSelector: selector.String()})
		if err != nil {
			return err
		}

		go func() {
			defer watcher.Stop()
			watchedPods := sets.New[string]()

			for {
				select {
				case <-l.ctx.Done():
					return
				case event := <-watcher.ResultChan():
					if event.Object == nil || reflect.ValueOf(event.Object).IsNil() {
						continue
					}

					pod, ok := event.Object.(*corev1.Pod)
					if !ok {
						continue
					}

					switch event.Type {
					case watch.Deleted:
						watchedPods.Delete(pod.Name)
					case watch.Added, watch.Modified:
						if !watchedPods.Has(pod.Name) && isPodReady(pod) {
							if !ok {
								continue
							}
							watchedPods.Insert(pod.Name)
							l.startForPod(pod, artifacts)
						}
					}
				}
			}
		}()
	}

	return nil
}

func (l *logger) startForPod(pod *corev1.Pod, artifactsDir string) {
	for _, container := range pod.Spec.Containers {
		podNs, podName, containerName := pod.Namespace, pod.Name, container.Name

		go func() {
			f, err := os.OpenFile(fmt.Sprintf("%s/new-logs/%s-%s-%s.log", artifactsDir, podName, containerName, time.Now().String()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return
			}
			defer f.Close()

			options := &corev1.PodLogOptions{
				Container:    containerName,
				Follow:       true,
				SinceSeconds: ptr.Int64(1),
			}

			req := l.kc.CoreV1().Pods(podNs).GetLogs(podName, options)
			stream, err := req.Stream(context.Background())
			if err != nil {
				return
			}
			defer stream.Close()

			for scanner := bufio.NewScanner(stream); scanner.Scan(); {
				f.Write(append(scanner.Bytes(), '\n'))
			}
		}()
	}
}

func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return true
			}
		}
	}
	return false
}

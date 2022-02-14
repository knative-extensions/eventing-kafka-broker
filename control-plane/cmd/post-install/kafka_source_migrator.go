/*
 * Copyright 2022 The Knative Authors
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

package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	kcs "knative.dev/eventing-kafka/pkg/client/clientset/versioned"
)

type KafkaSourceMigrator struct {
	kcs kcs.Interface
	k8s kubernetes.Interface
}

func (m *KafkaSourceMigrator) Migrate(ctx context.Context) error {

	// cont takes care of results pagination.
	cont := ""
	first := true
	for first || cont != "" {
		kss, err := m.kcs.
			SourcesV1beta1().
			KafkaSources(corev1.NamespaceAll).
			List(ctx, metav1.ListOptions{Continue: cont})
		if err != nil {
			return fmt.Errorf("failed to list KafkaSources: %w", err)
		}

		for _, ks := range kss.Items {

			// The KafkaSource deployment name isn't predictable, so we use a label selector to get
			// the associated deployment.
			ksLabels := map[string]string{
				// See https://github.com/knative-sandbox/eventing-kafka/blob/2b185a9b59347cc4a16471ed0fb2ba3bdf6e1672/pkg/source/reconciler/source/resources/labels.go#L24-L39
				"eventing.knative.dev/source":     "kafka-source-controller",
				"eventing.knative.dev/sourceName": ks.GetName(),
			}
			lsOpts := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(ksLabels).String()}

			// Instead of listing and then deleting a single deployment,
			// we use delete collection that allows deleting objects based
			// on a label selector.
			pp := metav1.DeletePropagationForeground
			err := m.k8s.AppsV1().
				Deployments(ks.GetNamespace()).
				DeleteCollection(ctx, metav1.DeleteOptions{PropagationPolicy: &pp}, lsOpts)
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete deployments with selector %v: %w", lsOpts.LabelSelector, err)
			}
		}

		cont = kss.Continue
		first = false
	}

	return nil
}

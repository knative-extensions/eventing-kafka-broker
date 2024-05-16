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

package main

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

type kafkaDeploymentDeleter struct {
	k8s kubernetes.Interface
}

func (k *kafkaDeploymentDeleter) DeleteBrokerDeployments(ctx context.Context) error {
	deployments := make([]types.NamespacedName, 0)
	c := ""
	for {
		deploymentList, err := k.k8s.AppsV1().Deployments("").List(ctx, metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/component=kafka-broker-dispatcher",
			Continue:      c,
		})
		if err != nil {
			return fmt.Errorf("failed to list deployments: %w", err)
		}
		for _, d := range deploymentList.Items {
			deployments = append(deployments, types.NamespacedName{
				Namespace: d.Namespace,
				Name:      d.Name,
			})
		}

		if deploymentList.Continue == "" {
			break
		}
		c = deploymentList.Continue
	}

	for _, deployment := range deployments {
		if err := k.deleteDeployment(ctx, deployment); err != nil {
			return fmt.Errorf("failed to delete deployment %s: %v", deployment, err)
		}
	}

	return nil
}

func (k *kafkaDeploymentDeleter) deleteDeployment(ctx context.Context, deployment types.NamespacedName) error {
	err := k.waitStatefulSetExists(ctx, deployment)
	if err != nil {
		return fmt.Errorf("failed while waiting for statefulset to come up: %w", err)
	}

	err = k.k8s.
		AppsV1().
		Deployments(deployment.Namespace).
		Delete(ctx, deployment.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", deployment.Namespace, deployment.Name, err)
	}

	return nil
}

func (k *kafkaDeploymentDeleter) waitStatefulSetExists(ctx context.Context, sts types.NamespacedName) error {
	return wait.PollUntilContextTimeout(ctx, 10*time.Second, 10*time.Minute, false, func(ctx context.Context) (done bool, err error) {
		_, err = k.k8s.AppsV1().StatefulSets(sts.Namespace).Get(ctx, sts.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to get statefulset %s/%s: %w", sts.Namespace, sts.Name, err)
		}
		return true, nil
	})
}

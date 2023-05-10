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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/system"
)

func (d *kafkaSourceDeleter) DeleteDispatcher(ctx context.Context) error {
	if err := waitStatefulSetExists(ctx, d.k8s); err != nil {
		return fmt.Errorf("failed while waiting for statefulset to come up: %w", err)
	}

	// Delete deployment.apps/kafka-source-dispatcher
	const sourceDispatcherDeploymentName = "kafka-source-dispatcher"
	err := d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, sourceDispatcherDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), sourceDispatcherDeploymentName, err)
	}

	return nil
}

func waitStatefulSetExists(ctx context.Context, k8s kubernetes.Interface) error {
	const sourceDispatcherStatefulSetName = "kafka-source-dispatcher"
	return wait.Poll(10*time.Second, 10*time.Minute, func() (done bool, err error) {
		_, err = k8s.AppsV1().StatefulSets(system.Namespace()).Get(ctx, sourceDispatcherStatefulSetName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to get statefulset %s/%s: %w", system.Namespace(), sourceDispatcherStatefulSetName, err)
		}
		return true, nil
	})
}

func (d *kafkaChannelPostMigrationDeleter) DeleteDispatcher(ctx context.Context) error {
	if err := waitStatefulSetExists(ctx, d.k8s); err != nil {
		return fmt.Errorf("failed while waiting for statefulset to come up: %w", err)
	}

	// Delete deployment.apps/kafka-channel-dispatcher
	const channelDispatcherDeploymentName = "kafka-channel-dispatcher"
	err := d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, channelDispatcherDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), channelDispatcherDeploymentName, err)
	}

	return nil
}

func (d *kafkaBrokerDeleter) DeleteDispatcher(ctx context.Context) error {
	if err := waitStatefulSetExists(ctx, d.k8s); err != nil {
		return fmt.Errorf("failed while waiting for statefulset to come up: %w", err)
	}

	// Delete deployment.apps/kafka-broker-dispatcher
	const brokerDispatcherDeploymentName = "kafka-broker-dispatcher"
	err := d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, brokerDispatcherDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), brokerDispatcherDeploymentName, err)
	}

	return nil
}

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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/system"
)

type kafkaChannelPostMigrationDeleter struct {
	k8s kubernetes.Interface
}

func (d *kafkaChannelPostMigrationDeleter) Delete(ctx context.Context) error {
	// at this stage,
	// - we deleted the old controler
	// - we made the existing channel services to point to new data plane pods
	// - we can now delete the old dispatcher resources and all the other leftovers

	///////////////////////////////////////////////////////////////////////////////
	/////////// START DELETING DISPATCHER RESOURCES
	///////////////////////////////////////////////////////////////////////////////

	// Delete service knative-eventing/kafka-ch-dispatcher
	const dispatcherServiceName = "kafka-ch-dispatcher"
	err := d.k8s.
		CoreV1().
		Services(system.Namespace()).
		Delete(ctx, dispatcherServiceName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service %s/%s: %w", system.Namespace(), dispatcherServiceName, err)
	}

	// Delete deployment.apps/kafka-ch-dispatcher
	const dispatcherDeploymentName = "kafka-ch-dispatcher"
	err = d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, dispatcherDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), dispatcherDeploymentName, err)
	}

	// Delete clusterrolebinding/kafka-ch-dispatcher
	const dispatcherClusterRoleBinding = "kafka-ch-dispatcher"
	err = d.k8s.
		RbacV1().
		ClusterRoleBindings().
		Delete(ctx, dispatcherClusterRoleBinding, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", dispatcherClusterRoleBinding, err)
	}

	// Delete clusterrole/kafka-ch-dispatcher
	const dispatcherClusterRole = "kafka-ch-dispatcher"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, dispatcherClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", dispatcherClusterRole, err)
	}

	// Delete serviceaccount/kafka-ch-dispatcher
	const dispatcherServiceAccount = "kafka-ch-dispatcher"
	err = d.k8s.
		CoreV1().
		ServiceAccounts(system.Namespace()).
		Delete(ctx, dispatcherServiceAccount, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service account %s/%s: %w", system.Namespace(), dispatcherServiceAccount, err)
	}

	// Delete configmap/config-leader-election-kafkachannel
	// Delete clusterrole/kafka-ch-dispatcher
	const kafkaChannelLeaderElectionConfigmap = "config-leader-election-kafkachannel"
	err = d.k8s.
		CoreV1().
		ConfigMaps(system.Namespace()).
		Delete(ctx, kafkaChannelLeaderElectionConfigmap, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Configmap %s: %w", kafkaChannelLeaderElectionConfigmap, err)
	}

	// TODO: DO NOT delete configmap/config-kafka

	// TODO: leases need some work!
	// --- This needs to be deleted
	// kafkachannel-dispatcher.knative.dev.eventing-kafka.pkg.channel.consolidated.reconciler.dispatcher.reconciler.00-of-01

	return nil
}

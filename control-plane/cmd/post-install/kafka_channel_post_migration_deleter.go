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

	"k8s.io/apimachinery/pkg/util/wait"
	"knative.dev/pkg/logging"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/system"
)

const (
	ControlPlaneReadinessCheckInterval = 10 * time.Second
	ControlPlaneReadinessCheckTimeout  = 10 * time.Minute

	WaitDurationBeforePostMigration = 10 * time.Minute

	NewControllerDeploymentName = "kafka-controller"
)

type kafkaChannelPostMigrationDeleter struct {
	k8s kubernetes.Interface
}

func (d *kafkaChannelPostMigrationDeleter) Delete(ctx context.Context) error {
	// at this stage,
	// - we deleted the old controler
	// - we made the existing channel services to point to new data plane pods
	// - we can now delete the old dispatcher resources and all the other leftovers
	// HOWEVER, we need to check if the new controller is up and running.

	logger := logging.FromContext(ctx)
	logger.Infof("Waiting %s for the new control plane to become ready before the migration.", ControlPlaneReadinessCheckTimeout)

	// wait until the new control plane is ready
	err := d.waitForNewControlPlaneReady(ctx)
	if err != nil {
		if err == wait.ErrWaitTimeout {
			logger.Infof("Channel data plane does not exist - ignore migration")
			return nil
		}
		return fmt.Errorf("error while waiting the new control plane to become ready %w", err)
	}

	logger.Infof("New control plane is ready, waiting %s before deleting old data plane", WaitDurationBeforePostMigration)
	time.Sleep(WaitDurationBeforePostMigration)
	logger.Infof("Done waiting %s. Deleting old data plane...", WaitDurationBeforePostMigration)

	///////////////////////////////////////////////////////////////////////////////
	/////////// START DELETING DISPATCHER RESOURCES
	///////////////////////////////////////////////////////////////////////////////

	// Delete service knative-eventing/kafka-ch-dispatcher
	const dispatcherServiceName = "kafka-ch-dispatcher"
	err = d.k8s.
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
	const kafkaChannelLeaderElectionConfigmap = "config-leader-election-kafkachannel"
	err = d.k8s.
		CoreV1().
		ConfigMaps(system.Namespace()).
		Delete(ctx, kafkaChannelLeaderElectionConfigmap, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Configmap %s: %w", kafkaChannelLeaderElectionConfigmap, err)
	}

	// Delete lease/kafkachannel-dispatcher.knative.dev.eventing-kafka.pkg.channel.consolidated.reconciler.dispatcher.reconciler.00-of-01
	const kafkaChannelDispatcherLease = "kafkachannel-dispatcher.knative.dev.eventing-kafka.pkg.channel.consolidated.reconciler.dispatcher.reconciler.00-of-01"
	err = d.k8s.
		CoordinationV1().
		Leases(system.Namespace()).
		Delete(ctx, kafkaChannelDispatcherLease, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete Lease %s: %w", kafkaChannelDispatcherLease, err)
	}

	return nil
}

func (d *kafkaChannelPostMigrationDeleter) waitForNewControlPlaneReady(ctx context.Context) error {
	return wait.PollImmediate(ControlPlaneReadinessCheckInterval, ControlPlaneReadinessCheckTimeout, func() (bool, error) {
		return isDeploymentReady(ctx, d.k8s, system.Namespace(), NewControllerDeploymentName)
	})
}

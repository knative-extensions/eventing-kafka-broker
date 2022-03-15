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

type kafkaChannelPreMigrationDeleter struct {
	k8s kubernetes.Interface
}

func (d *kafkaChannelPreMigrationDeleter) Delete(ctx context.Context) error {
	/////////////////////////////////////////////////////////////////////////////
	///////// START DELETING WEBHOOK RESOURCES
	/////////////////////////////////////////////////////////////////////////////

	// Delete mutatingwebhookconfiguration/defaulting.webhook.kafka.messaging.knative.dev
	const channelMutatingWebhookConfigurationName = "defaulting.webhook.kafka.messaging.knative.dev"
	err := d.k8s.
		AdmissionregistrationV1().
		MutatingWebhookConfigurations().
		Delete(ctx, channelMutatingWebhookConfigurationName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete mutating webhook configuration %s: %w", channelMutatingWebhookConfigurationName, err)
	}

	// Delete validatingwebhookconfiguration/validation.webhook.kafka.messaging.knative.dev
	const channelValidatingWebhookConfigurationName = "validation.webhook.kafka.messaging.knative.dev"
	err = d.k8s.
		AdmissionregistrationV1().
		ValidatingWebhookConfigurations().
		Delete(ctx, channelValidatingWebhookConfigurationName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete validating webhook configuration %s: %w", channelMutatingWebhookConfigurationName, err)
	}

	// Delete service knative-eventing/kafka-webhook
	const channelWebhookServiceName = "kafka-webhook"
	err = d.k8s.
		CoreV1().
		Services(system.Namespace()).
		Delete(ctx, channelWebhookServiceName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service %s/%s: %w", system.Namespace(), channelWebhookServiceName, err)
	}

	// Delete deployment.apps/kafka-webhook
	const channelWebhookDeploymentName = "kafka-webhook"
	err = d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, channelWebhookDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), channelWebhookDeploymentName, err)
	}

	// Delete horizontalpodautoscaler/kafka-webhook
	const channelWebhookHpaName = "kafka-webhook"
	err = d.k8s.
		AutoscalingV1().
		HorizontalPodAutoscalers(system.Namespace()).
		Delete(ctx, channelWebhookHpaName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete HPA %s/%s: %w", system.Namespace(), channelWebhookHpaName, err)
	}

	// Delete poddisruptionbudget/kafka-webhook
	const channelWebhookPdbName = "kafka-webhook"
	err = d.k8s.
		PolicyV1().
		PodDisruptionBudgets(system.Namespace()).
		Delete(ctx, channelWebhookPdbName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete poddisruptionbudget %s/%s: %w", system.Namespace(), channelWebhookPdbName, err)
	}

	// Delete serviceaccount/kafka-webhook
	const channelWebhookServiceAccount = "kafka-webhook"
	err = d.k8s.
		CoreV1().
		ServiceAccounts(system.Namespace()).
		Delete(ctx, channelWebhookServiceAccount, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service account %s/%s: %w", system.Namespace(), channelWebhookServiceAccount, err)
	}

	// Delete clusterrolebinding/kafka-webhook
	const channelWebhookClusterRoleBinding = "kafka-webhook"
	err = d.k8s.
		RbacV1().
		ClusterRoleBindings().
		Delete(ctx, channelWebhookClusterRoleBinding, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", channelWebhookClusterRoleBinding, err)
	}

	// Delete clusterrole/kafka-webhook
	const channelWebhookClusterRole = "kafka-webhook"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, channelWebhookClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", channelWebhookClusterRole, err)
	}

	// Delete secret/messaging-webhook-certs
	const channelWebhookSecret = "messaging-webhook-certs"
	err = d.k8s.
		CoreV1().
		Secrets(system.Namespace()).
		Delete(ctx, channelWebhookSecret, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete secret %s/%s: %w", system.Namespace(), channelWebhookSecret, err)
	}

	/////////////////////////////////////////////////////////////////////////////
	///////// START DELETING CONTROLLER RESOURCES
	/////////////////////////////////////////////////////////////////////////////

	// Delete deployment/kafka-ch-controller
	const channelControllerDeploymentName = "kafka-ch-controller"
	err = d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, channelControllerDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), channelControllerDeploymentName, err)
	}

	// Delete clusterrolebinding/kafka-ch-controller
	const channelControllerClusterRoleBinding = "kafka-ch-controller"
	err = d.k8s.
		RbacV1().
		ClusterRoleBindings().
		Delete(ctx, channelControllerClusterRoleBinding, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", channelControllerClusterRoleBinding, err)
	}

	// Delete clusterrole/kafka-ch-controller
	const channelControllerClusterRole = "kafka-ch-controller"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, channelControllerClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", channelControllerClusterRole, err)
	}

	// Delete serviceaccount/kafka-ch-controller
	const channelControllerServiceAccount = "kafka-ch-controller"
	err = d.k8s.
		CoreV1().
		ServiceAccounts(system.Namespace()).
		Delete(ctx, channelControllerServiceAccount, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service account %s/%s: %w", system.Namespace(), channelControllerServiceAccount, err)
	}

	// Delete clusterrole/kafka-addressable-resolver
	const channelAddressableResolverClusterRole = "kafka-addressable-resolver"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, channelAddressableResolverClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", channelAddressableResolverClusterRole, err)
	}

	// Delete clusterrole/kafka-channelable-manipulator
	const channelChannelableManipulatorClusterRole = "kafka-channelable-manipulator"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, channelChannelableManipulatorClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", channelChannelableManipulatorClusterRole, err)
	}

	return nil
}

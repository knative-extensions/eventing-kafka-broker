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

type kafkaSourceDeleter struct {
	k8s kubernetes.Interface
}

func (d *kafkaSourceDeleter) Delete(ctx context.Context) error {
	// Delete mutatingwebhookconfigurations.admissionregistration.k8s.io/defaulting.webhook.kafka.sources.knative.dev
	const sourceMutatingWebhookConfigurationName = "defaulting.webhook.kafka.sources.knative.dev"
	err := d.k8s.
		AdmissionregistrationV1().
		MutatingWebhookConfigurations().
		Delete(ctx, sourceMutatingWebhookConfigurationName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete mutating webhook configuration %s: %w", sourceMutatingWebhookConfigurationName, err)
	}

	// Delete validatingwebhookconfigurations.admissionregistration.k8s.io/validation.webhook.kafka.sources.knative.dev
	const sourceValidatingWebhookConfigurationName = "validation.webhook.kafka.sources.knative.dev"
	err = d.k8s.
		AdmissionregistrationV1().
		ValidatingWebhookConfigurations().
		Delete(ctx, sourceValidatingWebhookConfigurationName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete validating webhook configuration %s: %w", sourceMutatingWebhookConfigurationName, err)
	}

	// Delete validatingwebhookconfigurations.admissionregistration.k8s.io/config.webhook.kafka.sources.knative.dev
	const sourceConfigValidatingWebhookConfigurationName = "config.webhook.kafka.sources.knative.dev"
	err = d.k8s.
		AdmissionregistrationV1().
		ValidatingWebhookConfigurations().
		Delete(ctx, sourceConfigValidatingWebhookConfigurationName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete validating webhook configuration %s: %w", sourceConfigValidatingWebhookConfigurationName, err)
	}

	// Delete service knative-eventing/kafka-source-webhook
	const sourceWebhookServiceName = "kafka-source-webhook"
	err = d.k8s.
		CoreV1().
		Services(system.Namespace()).
		Delete(ctx, sourceWebhookServiceName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service %s/%s: %w", system.Namespace(), sourceWebhookServiceName, err)
	}

	// Delete service knative-eventing/kafka-controller
	const sourceControllerServiceName = "kafka-controller"
	err = d.k8s.
		CoreV1().
		Services(system.Namespace()).
		Delete(ctx, sourceControllerServiceName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service %s/%s: %w", system.Namespace(), sourceControllerServiceName, err)
	}

	// Delete deployment.apps/kafka-controller-manager
	const sourceControllerDeploymentName = "kafka-controller-manager"
	err = d.k8s.
		AppsV1().
		Deployments(system.Namespace()).
		Delete(ctx, sourceControllerDeploymentName, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment %s/%s: %w", system.Namespace(), sourceControllerDeploymentName, err)
	}

	const kafkaControllerClusterRoleBinding = "eventing-sources-kafka-controller"
	err = d.k8s.
		RbacV1().
		ClusterRoleBindings().
		Delete(ctx, kafkaControllerClusterRoleBinding, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", kafkaControllerClusterRoleBinding, err)
	}

	const kafkaControllerAddressableResolverClusterRoleBinding = "eventing-sources-kafka-controller-addressable-resolver"
	err = d.k8s.
		RbacV1().
		ClusterRoleBindings().
		Delete(ctx, kafkaControllerAddressableResolverClusterRoleBinding, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRoleBinding %s: %w", kafkaControllerAddressableResolverClusterRoleBinding, err)
	}

	const kafkaControllerClusterRole = "eventing-sources-kafka-controller"
	err = d.k8s.
		RbacV1().
		ClusterRoles().
		Delete(ctx, kafkaControllerClusterRole, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete ClusterRole %s: %w", kafkaControllerClusterRole, err)
	}

	const kafkaControllerServiceAccount = "kafka-controller-manager"
	err = d.k8s.
		CoreV1().
		ServiceAccounts(system.Namespace()).
		Delete(ctx, kafkaControllerServiceAccount, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service account %s/%s: %w", system.Namespace(), kafkaControllerServiceAccount, err)
	}

	const kafkaSourceWebhookSecret = "kafka-source-webhook-certs"
	err = d.k8s.
		CoreV1().
		Secrets(system.Namespace()).
		Delete(ctx, kafkaSourceWebhookSecret, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete secret %s/%s: %w", system.Namespace(), kafkaSourceWebhookSecret, err)
	}

	return nil
}

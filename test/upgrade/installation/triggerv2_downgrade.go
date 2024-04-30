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

package installation

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/system"
	pkgupgrade "knative.dev/pkg/test/upgrade"

	internalsclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client"

	"knative.dev/reconciler-test/pkg/environment"
)

func cleanupTriggerv2ConsumerGroups(c pkgupgrade.Context, glob environment.GlobalEnvironment) {
	ctx, _ := glob.Environment()
	client := kubeclient.Get(ctx)

	err := deleteConsumerGroups(ctx, client)
	if err != nil {
		c.T.Fatal("failed to downgrade from triggerv2", err.Error())
	}
}

func cleanupTriggerv2Deployments(c pkgupgrade.Context, glob environment.GlobalEnvironment) {
	ctx, _ := glob.Environment()
	client := kubeclient.Get(ctx)

	err := deleteStatefulSet(ctx, client, "kafka-broker-dispatcher", system.Namespace())
	if err != nil {
		c.T.Fatal("failed to downgrade from triggerv2", err.Error())
	}
}

func deleteStatefulSet(ctx context.Context, client kubernetes.Interface, name string, namespace string) error {
	err := waitDeploymentExists(ctx, client, name, namespace)
	if err != nil {
		return err
	}

	err = client.AppsV1().StatefulSets(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete statefulset: %w", err)
	}

	return nil
}

func waitDeploymentExists(ctx context.Context, client kubernetes.Interface, name string, namespace string) error {
	return wait.PollUntilContextTimeout(ctx, time.Second*10, time.Minute*3, true, func(ctx context.Context) (bool, error) {
		_, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return false, nil
		}

		if err != nil {
			return false, fmt.Errorf("failed to get deployment: %w", err)
		}

		return true, nil
	})
}

func deleteConsumerGroups(ctx context.Context, client kubernetes.Interface) error {
	namespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	internalClient := internalsclient.Get(ctx)

	for _, ns := range namespaces.Items {
		cgClient := internalClient.InternalV1alpha1().ConsumerGroups(ns.Name)
		cgList, err := cgClient.List(ctx, metav1.ListOptions{})

		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		for _, cg := range cgList.Items {
			for _, owner := range cg.OwnerReferences {
				if owner.Kind == "Trigger" {
					err := cgClient.Delete(ctx, cg.Name, metav1.DeleteOptions{})
					if err != nil && !errors.IsNotFound(err) {
						return err
					}
				}
			}
		}
	}

	return nil
}

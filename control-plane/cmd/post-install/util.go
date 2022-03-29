package main

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func isDeploymentReady(ctx context.Context, k8s kubernetes.Interface, namespace, name string) (bool, error) {
	deployment, err := k8s.
		AppsV1().
		Deployments(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// Return false as we are not done yet.
		// We swallow the error to keep on polling.
		return false, nil
	} else if err != nil {
		// Return error to stop the polling.
		return false, err
	}

	ready := GetDeploymentCondition(deployment.Status, appsv1.DeploymentAvailable)
	return ready != nil && ready.Status == corev1.ConditionTrue, nil
}

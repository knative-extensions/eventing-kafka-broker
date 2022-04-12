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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/system"
)

type KafkaSourceSoDeleter struct {
	dynamic dynamic.Interface
	k8s     kubernetes.Interface
}

func (d KafkaSourceSoDeleter) Delete(ctx context.Context) error {
	smGVR := schema.GroupVersionResource{
		Group:    "monitoring.coreos.com",
		Version:  "v1",
		Resource: "servicemonitors",
	}

	controllerServiceMonitor := "kafka-controller-manager-sm"
	err := d.dynamic.Resource(smGVR).
		Namespace(system.Namespace()).
		Delete(ctx, controllerServiceMonitor, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service monitor %s/%s: %w", system.Namespace(), controllerServiceMonitor, err)
	}

	controllerMonitoringService := "kafka-controller-manager-sm-service"
	err = d.k8s.CoreV1().
		Services(system.Namespace()).
		Delete(ctx, controllerMonitoringService, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete service %s/%s: %w", system.Namespace(), controllerMonitoringService, err)
	}

	return nil
}

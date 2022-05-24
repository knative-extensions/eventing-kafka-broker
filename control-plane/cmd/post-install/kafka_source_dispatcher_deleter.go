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
	"knative.dev/pkg/system"
)

func (d *kafkaSourceDeleter) DeleteDispatcher(ctx context.Context) error {
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

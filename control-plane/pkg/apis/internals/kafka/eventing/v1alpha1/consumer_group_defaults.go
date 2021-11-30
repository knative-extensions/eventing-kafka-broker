/*
 * Copyright 2021 The Knative Authors
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

package v1alpha1

import (
	"context"

	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
)

var (
	_ apis.Defaultable = &ConsumerGroup{}
)

// SetDefaults implements apis.Defaultable.
func (cg *ConsumerGroup) SetDefaults(ctx context.Context) {
	ctx = apis.WithinParent(ctx, cg.ObjectMeta)

	// Replicas is the number of Consumers for this ConsumerGroup.
	// When unset, set it to 1.
	if cg.Spec.Replicas == nil {
		cg.Spec.Replicas = pointer.Int32Ptr(1)
	}
	// Selector is a label query over consumers that should match the Replicas count.
	// If Selector is empty, it is defaulted to the labels present on the template.
	if cg.Spec.Selector == nil || len(cg.Spec.Selector) == 0 {
		cg.Spec.Selector = cg.Spec.Template.Labels
	}

	// Force template namespace to be set to ConsumerGroup's namespace.
	cg.Spec.Template.Namespace = cg.Namespace
	cg.Spec.Template.Spec.SetDefaults(ctx)
}

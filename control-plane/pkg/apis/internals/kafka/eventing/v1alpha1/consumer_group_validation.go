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

	"knative.dev/pkg/apis"
)

var (
	_ apis.Validatable = &ConsumerGroup{}
)

func (c *ConsumerGroup) Validate(ctx context.Context) *apis.FieldError {
	ctx = apis.WithinParent(ctx, c.ObjectMeta)
	return c.Spec.Validate(ctx).ViaField("spec")
}

func (cgs *ConsumerGroupSpec) Validate(ctx context.Context) *apis.FieldError {
	if cgs.Replicas == nil {
		return apis.ErrMissingField("replicas")
	}
	if cgs.Selector == nil {
		return apis.ErrMissingField("selector")
	}
	return cgs.Template.Validate(ctx).ViaField("template")
}

func (cts *ConsumerTemplateSpec) Validate(ctx context.Context) *apis.FieldError {
	return cts.Spec.Validate(ctx).ViaField("spec")
}

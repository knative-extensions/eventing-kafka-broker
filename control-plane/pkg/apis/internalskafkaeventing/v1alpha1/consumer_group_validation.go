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
	if apis.IsInUpdate(ctx) {
		err := c.CheckImmutableFields(ctx, apis.GetBaseline(ctx).(*ConsumerGroup).Labels)
		if err != nil {
			return err
		}
	}
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
	specCtx := ctx
	var err *apis.FieldError
	if apis.IsInUpdate(ctx) {
		specCtx = apis.WithinUpdate(ctx, apis.GetBaseline(ctx).(*ConsumerGroup).Spec.Template.Spec)
	}
	err = err.Also( // fields with defaults
		cts.Spec.Delivery.Validate(specCtx).ViaField("delivery"),
		cts.Spec.Subscriber.Validate(specCtx).ViaField("subscriber"),
	)
	if cts.Spec.Configs.Configs == nil {
		err = err.Also(apis.ErrMissingField("spec.configs"))
	}
	return err
}

func (c *ConsumerGroup) CheckImmutableFields(ctx context.Context, original map[string]string) *apis.FieldError {
	if orig, ok := original[KafkaChannelNameLabel]; ok {
		if new, ok := c.Labels[KafkaChannelNameLabel]; !ok || orig != new {
			return ErrImmutableField("Consumer Group Label",
				"Removing or modifying the consumer group label is unsupported")
		}
	}
	return nil
}

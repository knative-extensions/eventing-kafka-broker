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

func (c *Consumer) Validate(ctx context.Context) *apis.FieldError {
	specCtx := ctx
	if apis.IsInUpdate(ctx) {
		specCtx = apis.WithinUpdate(ctx, apis.GetBaseline(ctx).(*Consumer).Spec)
	}
	return c.Spec.Validate(specCtx).ViaField("spec")
}

func (cs *ConsumerSpec) Validate(ctx context.Context) *apis.FieldError {
	if cs == nil {
		return nil
	}
	var err *apis.FieldError
	if len(cs.Topics) == 0 {
		return apis.ErrMissingField("topics")
	}
	err = err.Also(
		cs.Delivery.Validate(ctx).ViaField("delivery"),
		cs.Configs.Validate(ctx).ViaField("configs"),
		cs.Filters.Validate(ctx).ViaField("filters"),
		cs.Subscriber.Validate(ctx).ViaField("subscriber"),
		cs.PodBind.Validate(ctx).ViaField("podBind"),
		cs.CloudEventOverrides.Validate(ctx).ViaField("ceOverrides"),
	)
	return err
}

func (d *DeliverySpec) Validate(ctx context.Context) *apis.FieldError {
	if d == nil {
		return nil
	}
	return d.DeliverySpec.Validate(ctx).ViaField(apis.CurrentField)
}

func (cc *ConsumerConfigs) Validate(ctx context.Context) *apis.FieldError {
	expected := []string{
		"group.id",
		"bootstrap.servers",
	}
	for _, key := range expected {
		if v, ok := cc.Configs[key]; !ok || v == "" {
			return apis.ErrMissingField(key)
		}
	}
	return nil
}

func (f *Filters) Validate(ctx context.Context) *apis.FieldError {
	return nil
}

func (p *PodBind) Validate(ctx context.Context) *apis.FieldError {
	if p == nil {
		return apis.ErrMissingField("")
	}
	if len(p.PodName) == 0 {
		return apis.ErrMissingField("podName")
	}
	if len(p.PodNamespace) == 0 {
		return apis.ErrMissingField("podNamespace")
	}
	if apis.IsInUpdate(ctx) {
		return p.CheckImmutableFields(ctx, apis.GetBaseline(ctx).(ConsumerSpec).PodBind)
	}
	return nil
}

func (p PodBind) CheckImmutableFields(ctx context.Context, original *PodBind) *apis.FieldError {
	if p.PodName != original.PodName || p.PodNamespace != original.PodNamespace {
		return ErrImmutableField("podBind",
			"Moving a consumer to a different pod is unsupported, to move a consumer to another pod, "+
				"remove this one and create a new consumer with the same spec",
		)
	}
	return nil
}

func ErrImmutableField(field, details string) *apis.FieldError {
	return &apis.FieldError{
		Message: "Immutable field updated",
		Paths:   []string{field},
		Details: details,
	}
}

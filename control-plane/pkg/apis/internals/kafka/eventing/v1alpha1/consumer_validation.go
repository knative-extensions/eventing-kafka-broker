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

func (cs *ConsumerSpec) Validate(ctx context.Context) *apis.FieldError {
	var err *apis.FieldError
	if len(cs.Topics) == 0 {
		return apis.ErrMissingField("topics")
	}
	return err.Also(
		cs.Delivery.Validate(ctx).ViaField("delivery"),
		cs.Configs.Validate(ctx).ViaField("configs"),
		cs.Filters.Validate(ctx).ViaField("filters"),
		cs.Subscriber.Validate(ctx).ViaField("subscriber"),
	)
}

func (d *DeliverySpec) Validate(ctx context.Context) *apis.FieldError {
	if d == nil {
		return nil
	}
	return d.DeliverySpec.Validate(ctx).ViaField(apis.CurrentField)
}

func (cc *ConsumerConfigs) Validate(ctx context.Context) *apis.FieldError {
	if v, ok := cc.Configs["group.id"]; !ok || v == "" {
		return apis.ErrMissingField("group.id")
	}
	return nil
}

func (f *Filters) Validate(ctx context.Context) *apis.FieldError {
	return nil
}

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
	_ apis.Defaultable = &Consumer{}
)

// SetDefaults implements apis.Defaultable.
func (c *Consumer) SetDefaults(ctx context.Context) {
	ctx = apis.WithinParent(ctx, c.ObjectMeta)
	c.Spec.SetDefaults(ctx)
}

func (c *ConsumerSpec) SetDefaults(ctx context.Context) {
	c.Delivery.SetDefaults(ctx)
	c.Subscriber.SetDefaults(ctx)
}

func (d *DeliverySpec) SetDefaults(ctx context.Context) {
	if d == nil {
		return
	}
	d.DeliverySpec.SetDefaults(ctx)
}

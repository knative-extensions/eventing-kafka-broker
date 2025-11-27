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

	sources "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1"
)

var (
	_ apis.Defaultable = &Consumer{}
)

// SetDefaults implements apis.Defaultable.
func (c *Consumer) SetDefaults(ctx context.Context) {
	ctx = apis.WithinParent(ctx, c.ObjectMeta)
	c.Spec.SetDefaults(ctx)
}

func (cs *ConsumerSpec) SetDefaults(ctx context.Context) {
	if cs.Delivery == nil {
		cs.Delivery = &DeliverySpec{}
	}
	cs.Delivery.SetDefaults(ctx)
	cs.Subscriber.SetDefaults(ctx)
}

// Since this is a pointer receiver method, the caller is responsible for ensuring a non-nil receiver
func (ds *DeliverySpec) SetDefaults(ctx context.Context) {
	if ds.InitialOffset == "" {
		ds.InitialOffset = sources.OffsetLatest
	}
	ds.DeliverySpec.SetDefaults(ctx)
}

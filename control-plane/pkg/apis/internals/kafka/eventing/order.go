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

package eventing

import (
	"context"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/apis"
)

const (
	// Ordered is a per partition blocking consumer.
	// It waits for a successful response of the sink before
	// sending the next message in the partition.
	Ordered DeliveryOrdering = "ordered"
	// Unordered is non-blocking consumer that delivers
	// events out of any particular order.
	Unordered DeliveryOrdering = "unordered"
)

var (
	deliveryOrders = sets.NewString(
		string(Ordered),
		string(Unordered),
	)

	deliveryOrdersString = strings.Join(deliveryOrders.List(), ",")
)

type DeliveryOrdering string

func (d DeliveryOrdering) Validate(context.Context) *apis.FieldError {
	if !deliveryOrders.Has(string(d)) {
		return apis.ErrInvalidValue(d, "", "expected one of: ["+deliveryOrdersString+"]")
	}
	return nil
}

var (
	_ apis.Validatable = DeliveryOrdering("")
)

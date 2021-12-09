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
	eventing "knative.dev/eventing/pkg/apis/eventing/v1"
)

// TODO Add this type in Eventing core

// Filters is the filter to apply against all events. Only events that pass this
// filter will be sent to the Subscriber.
// If not specified, will default to allowing all events.
type Filters struct {

	// Filter is the filter to apply against all events from the Broker. Only events that pass this
	// filter will be sent to the Subscriber. If not specified, will default to allowing all events.
	//
	// +optional
	Filter *eventing.TriggerFilter `json:"filter,omitempty"`

	// Filters is an experimental field that conforms to the CNCF CloudEvents Subscriptions
	// API. It's an array of filter expressions that evaluate to true or false.
	// If any filter expression in the array evaluates to false, the event MUST
	// NOT be sent to the Subscriber. If all the filter expressions in the array
	// evaluate to true, the event MUST be attempted to be delivered. Absence of
	// a filter or empty array implies a value of true. In the event of users
	// specifying both Filters and Filters, then the latter will override the former.
	// This will allow users to try out the effect of the new Filters field
	// without compromising the existing attribute-based Filters and try it out on existing
	// Trigger objects.
	//
	// +optional
	Filters []eventing.SubscriptionsAPIFilter `json:"filters,omitempty"`
}

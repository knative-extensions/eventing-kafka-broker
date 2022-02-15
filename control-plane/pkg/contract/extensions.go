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

package contract

import v1 "knative.dev/eventing/pkg/apis/eventing/v1"

// IncrementGeneration increments Generation.
func (x *Contract) IncrementGeneration() {
	x.Generation++
}

// NewExactFilter converts the SubscriptionsAPIFilter into the exact dialect of
// the DialectedFilter as defined in CloudEvents Subscriptions API.
//
// Exact contains exactly one attribute where the key is the name of the CloudEvent
// attribute and its value is the string value which must exactly match the value
// of the CloudEvent attribute.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#all-filter-dialect
func NewExactFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {
	return &DialectedFilter{
		Filter: &DialectedFilter_Exact{
			Exact: &Exact{
				Attributes: f.Exact,
			},
		},
	}
}

// NewPrefixFilter converts the SubscriptionsAPIFilter into the suffix dialect of
// the DialectedFilter as defined in CloudEvents Subscriptions API.
//
// Prefix contains exactly one attribute where the key is the name of the CloudEvent
// attribute which value must start with the value specified.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#prefix-filter-dialect
func NewPrefixFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {

	p := &Prefix{}
	// Eventing Trigger API is conforming to the Subscriptions API, so this map has only a single
	// key-value pair
	for k, v := range f.Prefix {
		p.Attribute = k
		p.Prefix = v
		// We only expect a single element, but let's break anyway
		break
	}

	return &DialectedFilter{
		Filter: &DialectedFilter_Prefix{
			Prefix: p,
		},
	}
}

// NewSuffixFilter converts the SubscriptionsAPIFilter into the suffix dialect of
// the DialectedFilter as defined in CloudEvents Subscriptions API.
//
// Suffix contains exactly one attribute where the key is the name of the CloudEvent
// attribute which value must end with the value specified.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#suffix-filter-dialect
func NewSuffixFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {

	p := &Suffix{}
	// Eventing Trigger API is conforming to the Subscriptions API, so this map has only a single
	// key-value pair
	for k, v := range f.Suffix {
		p.Attribute = k
		p.Suffix = v
		// We only expect a single element, but let's break anyway
		break
	}

	return &DialectedFilter{
		Filter: &DialectedFilter_Suffix{
			Suffix: p,
		},
	}
}

// NewAllFilter converts the SubscriptionsAPIFilter into the all dialect of the
// DialectedFilter as defined in CloudEvents Subscriptions API.
//
// All filter evaluates to true when all nested filters evaluate to true.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#all-filter-dialect
func NewAllFilter(filters []v1.SubscriptionsAPIFilter) *DialectedFilter {
	all := All{
		Filters: []*DialectedFilter{},
	}
	for _, f := range filters {
		all.Filters = append(all.Filters, FromSubscriptionFilter(f))
	}
	return &DialectedFilter{
		Filter: &DialectedFilter_All{
			All: &all,
		},
	}
}

// NewAnyFilter converts the SubscriptionsAPIFilter into the any dialect of the
// DialectedFilter as defined in CloudEvents Subscriptions API.
//
// All filter evaluates to true when all nested filters evaluate to true.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#any-filter-dialect
func NewAnyFilter(filters []v1.SubscriptionsAPIFilter) *DialectedFilter {
	any := Any{
		Filters: []*DialectedFilter{},
	}
	for _, f := range filters {
		any.Filters = append(any.Filters, FromSubscriptionFilter(f))
	}
	return &DialectedFilter{
		Filter: &DialectedFilter_Any{
			Any: &any,
		},
	}
}

// NewNotFilter converts the SubscriptionsAPIFilter into the not dialect of the
// DialectedFilter as defined in CloudEvents Subscriptions API.
//
// Not filter evaluates to true when the nested filter evaluates to false.
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/spec.md#not-filter-dialect
func NewNotFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {
	return &DialectedFilter{
		Filter: &DialectedFilter_Not{
			Not: &Not{
				Filter: FromSubscriptionFilter(f),
			},
		},
	}
}

// NewCESQLFilter converts the SubscriptionsAPIFilter into the sql dialect of the
// DialectedFilter as defined in CloudEvents Subscriptions API.
//
// CESOL filter is a Cloud Events SQL Expression
//
// See "CNCF CloudEvents Subscriptions API" > "3.2.4.1 Filters Dialects"
// https://github.com/cloudevents/spec/blob/main/subscriptions/sql.md#any-filter-dialect
//
// See "CNCF CloudEvents SQL Expression Language"
// https://github.com/cloudevents/spec/blob/main/cesql/spec.md
func NewCESQLFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {
	return &DialectedFilter{
		Filter: &DialectedFilter_Cesql{
			Cesql: &CESQL{
				Expression: f.SQL,
			},
		},
	}
}

// FromSubscriptionFilter converts a SubscriptionsAPIFilter to the corresponding
// DialectedFilter based on the property set in the SubscriptionsAPIFilter.
//
// E.g a SubscriptionsAPIFilter with the All field set will be converted to a DialectedFilter
// which Filter is of type DialectedFilter_All.
func FromSubscriptionFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {
	switch {
	case len(f.All) > 0:
		return NewAllFilter(f.All)
	case len(f.Any) > 0:
		return NewAnyFilter(f.Any)
	case f.Not != nil:
		return NewNotFilter(*f.Not)
	case f.Exact != nil:
		return NewExactFilter(f)
	case f.Prefix != nil:
		return NewPrefixFilter(f)
	case f.Suffix != nil:
		return NewSuffixFilter(f)
	case f.SQL != "":
		return NewCESQLFilter(f)
	}
	return &DialectedFilter{}
}

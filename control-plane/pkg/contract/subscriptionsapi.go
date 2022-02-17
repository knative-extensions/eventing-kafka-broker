/*
 * Copyright 2022 The Knative Authors
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
	return &DialectedFilter{
		Filter: &DialectedFilter_Prefix{
			Prefix: &Prefix{
				Attributes: f.Prefix,
			},
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
	return &DialectedFilter{
		Filter: &DialectedFilter_Suffix{
			Suffix: &Suffix{
				Attributes: f.Suffix,
			},
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
		Filters: make([]*DialectedFilter, 0, len(filters)),
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
		Filters: make([]*DialectedFilter, 0, len(filters)),
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
// DialectedFilter based on the property set in the SubscriptionsAPIFilter. If
// the SubscriptionsAPIFilter has multiple dialects set, they will be wrapped inside
// a Filter of the type DialectedFilter_All
//
// E.g a SubscriptionsAPIFilter with the Exact field set will be converted to a DialectedFilter
// which Filter is of type DialectedFilter_Exact.
func FromSubscriptionFilter(f v1.SubscriptionsAPIFilter) *DialectedFilter {
	filters := make([]*DialectedFilter, 0, 0)
	if len(f.All) > 0 {
		filters = append(filters, NewAllFilter(f.All))
	}
	if len(f.Any) > 0 {
		filters = append(filters, NewAnyFilter(f.All))
	}
	if f.Not != nil {
		filters = append(filters, NewNotFilter(*f.Not))
	}
	if f.Exact != nil {
		filters = append(filters, NewExactFilter(f))
	}
	if f.Prefix != nil {
		filters = append(filters, NewPrefixFilter(f))
	}
	if f.Suffix != nil {
		filters = append(filters, NewSuffixFilter(f))
	}
	if f.SQL != "" {
		filters = append(filters, NewCESQLFilter(f))
	}
	switch {
	case len(filters) == 1:
		return filters[0]
	case len(filters) > 1:
		return &DialectedFilter{
			Filter: &DialectedFilter_All{
				All: &All{
					Filters: filters,
				},
			},
		}
	}
	return &DialectedFilter{}
}

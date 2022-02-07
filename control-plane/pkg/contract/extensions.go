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

func NewAllFilter(filters []v1.SubscriptionsAPIFilter) *Filter {
	return newCollectionFilter(filters, Dialect_ALL)
}

func NewAnyFilter(filters []v1.SubscriptionsAPIFilter) *Filter {
	return newCollectionFilter(filters, Dialect_ANY)
}

func NewNotFilter(f v1.SubscriptionsAPIFilter) *Filter {
	return &Filter{
		Dialect: Dialect_NOT,
		Filters: []*Filter{
			FromSubscriptionFilter(f),
		},
	}
}

func NewExactFilter(f v1.SubscriptionsAPIFilter) *Filter {
	return &Filter{
		Dialect:    Dialect_EXACT,
		Attributes: f.Exact,
	}
}

func NewPrefixFilter(f v1.SubscriptionsAPIFilter) *Filter {
	return &Filter{
		Dialect:    Dialect_PREFIX,
		Attributes: f.Prefix,
	}
}

func NewSuffixFilter(f v1.SubscriptionsAPIFilter) *Filter {
	return &Filter{
		Dialect:    Dialect_SUFFIX,
		Attributes: f.Suffix,
	}
}

func NewCESQLFilter(f v1.SubscriptionsAPIFilter) *Filter {
	return &Filter{
		Dialect:    Dialect_CESQL,
		Expression: f.SQL,
	}
}

func FromSubscriptionFilter(f v1.SubscriptionsAPIFilter) *Filter {
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
	return nil
}

func newCollectionFilter(filters []v1.SubscriptionsAPIFilter, dialect Dialect) *Filter {
	cf := Filter{
		Dialect: dialect,
		Filters: make([]*Filter, 0, len(filters)),
	}
	for _, f := range filters {
		cf.Filters = append(cf.Filters, FromSubscriptionFilter(f))
	}
	return &cf
}

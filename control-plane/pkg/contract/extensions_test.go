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

package contract_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	v1 "knative.dev/eventing/pkg/apis/eventing/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

func TestContractIncrementGeneration(t *testing.T) {
	c := &contract.Contract{Generation: 42}
	c.IncrementGeneration()

	require.Equal(t, uint64(43), c.Generation)
}

func TestFromSubscriptionFilter(t *testing.T) {
	tt := []struct {
		name      string
		apiFilter v1.SubscriptionsAPIFilter
		want      *contract.DialectedFilter
	}{
		{
			name:      "empty all filter",
			apiFilter: v1.SubscriptionsAPIFilter{},
			want:      &contract.DialectedFilter{},
		},
		{
			name: "all filter with exact, prefix & suffix",
			apiFilter: v1.SubscriptionsAPIFilter{
				All: []v1.SubscriptionsAPIFilter{
					{
						Exact: map[string]string{"type": "dev.knative.kafkasrc.kafkarecord"},
					},
					{
						Prefix: map[string]string{"source": "dev.knative"},
					},
					{
						Suffix: map[string]string{"subject": "source"},
					},
					{
						Not: &v1.SubscriptionsAPIFilter{
							Prefix: map[string]string{"source": "dev.knative"},
						},
					},
					{
						SQL: "subject = 'source'",
					},
				},
			},
			want: &contract.DialectedFilter{
				Filter: &contract.DialectedFilter_All{
					All: &contract.All{
						Filters: []*contract.DialectedFilter{
							{
								Filter: &contract.DialectedFilter_Exact{
									Exact: &contract.Exact{
										Attributes: map[string]string{"type": "dev.knative.kafkasrc.kafkarecord"},
									},
								},
							},
							{
								Filter: &contract.DialectedFilter_Prefix{
									Prefix: &contract.Prefix{
										Attribute: "source",
										Prefix:    "dev.knative",
									},
								},
							},
							{
								Filter: &contract.DialectedFilter_Suffix{
									Suffix: &contract.Suffix{
										Attribute: "subject",
										Suffix:    "source",
									},
								},
							},
							{
								Filter: &contract.DialectedFilter_Not{
									Not: &contract.Not{
										Filter: &contract.DialectedFilter{
											Filter: &contract.DialectedFilter_Prefix{
												Prefix: &contract.Prefix{
													Attribute: "source",
													Prefix:    "dev.knative",
												},
											},
										},
									},
								},
							},
							{
								Filter: &contract.DialectedFilter_Cesql{
									Cesql: &contract.CESQL{
										Expression: "subject = 'source'",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			got := contract.FromSubscriptionFilter(tc.apiFilter)
			if cmp.Diff(got, tc.want, cmpopts.IgnoreUnexported(contract.DialectedFilter{},
				contract.All{}, contract.Prefix{}, contract.Suffix{}, contract.Exact{}, contract.Not{}, contract.CESQL{})) != "" {
				t.Errorf("FromSubscriptionFilter() = %v, want %v", got, tc.want)
			}
		})
	}

}

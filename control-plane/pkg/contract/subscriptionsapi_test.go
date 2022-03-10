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

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	v1 "knative.dev/eventing/pkg/apis/eventing/v1"
)

func TestFromSubscriptionFilter(t *testing.T) {
	tt := []struct {
		name      string
		apiFilter v1.SubscriptionsAPIFilter
		want      *DialectedFilter
	}{
		{
			name:      "empty all filter",
			apiFilter: v1.SubscriptionsAPIFilter{},
			want:      &DialectedFilter{},
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
						CESQL: "subject = 'source'",
					},
				},
			},
			want: &DialectedFilter{
				Filter: &DialectedFilter_All{
					All: &All{
						Filters: []*DialectedFilter{
							{
								Filter: &DialectedFilter_Exact{
									Exact: &Exact{
										Attributes: map[string]string{"type": "dev.knative.kafkasrc.kafkarecord"},
									},
								},
							},
							{
								Filter: &DialectedFilter_Prefix{
									Prefix: &Prefix{
										Attributes: map[string]string{"source": "dev.knative"},
									},
								},
							},
							{
								Filter: &DialectedFilter_Suffix{
									Suffix: &Suffix{
										Attributes: map[string]string{"subject": "source"},
									},
								},
							},
							{
								Filter: &DialectedFilter_Not{
									Not: &Not{
										Filter: &DialectedFilter{
											Filter: &DialectedFilter_Prefix{
												Prefix: &Prefix{
													Attributes: map[string]string{"source": "dev.knative"},
												},
											},
										},
									},
								},
							},
							{
								Filter: &DialectedFilter_Cesql{
									Cesql: &CESQL{
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
			got := FromSubscriptionFilter(tc.apiFilter)
			if cmp.Diff(got, tc.want, cmpopts.IgnoreUnexported(DialectedFilter{},
				All{}, Prefix{}, Suffix{}, Exact{}, Not{}, CESQL{})) != "" {
				t.Errorf("FromSubscriptionFilter() = %v, want %v", got, tc.want)
			}
		})
	}

}

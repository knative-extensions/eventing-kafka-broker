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
						SQL: "subject = 'source'",
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
										Attribute: "source",
										Prefix:    "dev.knative",
									},
								},
							},
							{
								Filter: &DialectedFilter_Suffix{
									Suffix: &Suffix{
										Attribute: "subject",
										Suffix:    "source",
									},
								},
							},
							{
								Filter: &DialectedFilter_Not{
									Not: &Not{
										Filter: &DialectedFilter{
											Filter: &DialectedFilter_Prefix{
												Prefix: &Prefix{
													Attribute: "source",
													Prefix:    "dev.knative",
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

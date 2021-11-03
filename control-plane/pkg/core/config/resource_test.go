/*
 * Copyright 2020 The Knative Authors
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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/testing/protocmp"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
)

func TestFindResource(t *testing.T) {
	type args struct {
		contract *contract.Contract
		resource types.UID
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "resource not found",
			args: args{
				contract: &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid: "2",
						},
					},
					Generation: 1,
				},
				resource: "1",
			},
			want: NoResource,
		},
		{
			name: "resource found",
			args: args{
				contract: &contract.Contract{
					Resources: []*contract.Resource{
						{
							Uid: "1",
						},
					},
					Generation: 1,
				},
				resource: "1",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindResource(tt.args.contract, tt.args.resource); got != tt.want {
				t.Errorf("FindResource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddOrUpdateResourcesConfig(t *testing.T) {
	tests := []struct {
		name         string
		haveContract *contract.Contract
		newResource  *contract.Resource
		index        int
		wantContract *contract.Contract
		changed      int
	}{
		{
			name: "resource not found - add resource",
			haveContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "2",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Uid: "egress-1",
							},
						},
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
						BootstrapServers: "broker:9092",
					},
				},
				Generation: 1,
			},
			newResource: &contract.Resource{
				Uid:    "1",
				Topics: []string{"topic-name-1"},
				Ingress: &contract.Ingress{
					IngressType: &contract.Ingress_Path{
						Path: "/broker-ns/broker-name",
					},
					ContentMode: contract.ContentMode_STRUCTURED,
				},
				Egresses: []*contract.Egress{
					{
						Destination:   "http://localhost:8080",
						ConsumerGroup: "egress-1",
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Uid: "egress-1",
					},
				},
				BootstrapServers: "broker:9092",
			},
			index: NoResource,
			wantContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "2",
						Topics: []string{"topic-name-1"},
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
						Egresses: []*contract.Egress{
							{
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Uid: "egress-1",
							},
						},
						BootstrapServers: "broker:9092",
					},
					{
						Uid:    "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Uid: "egress-1",
							},
						},
						BootstrapServers: "broker:9092",
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
					},
				},
				Generation: 1,
			},
		},
		{
			name: "resource found - update resource",
			haveContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Uid:           "egress-1",
							},
						},
						BootstrapServers: "broker:9092",
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
					},
				},
				Generation: 1,
			},
			newResource: &contract.Resource{
				Uid:    "1",
				Topics: []string{"topic-name-1"},
				// Any Trigger will be ignored, since the function will be called when we're reconciling a contract.Resource,
				// and it should preserve Egresses already added to the ConfigMap.
				Egresses: []*contract.Egress{
					{
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Destination:   "http://localhost:8080",
						ConsumerGroup: "egress-1",
						Uid:           "egress-1",
					},
				},
				BootstrapServers: "broker:9092,broker-2:9092",
				Ingress: &contract.Ingress{
					IngressType: &contract.Ingress_Path{
						Path: "/broker-ns/broker-name",
					},
					ContentMode: contract.ContentMode_STRUCTURED,
				},
			},
			index: 0,
			wantContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Uid:           "egress-1",
							},
						},
						BootstrapServers: "broker:9092,broker-2:9092",
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
					},
				},
				Generation: 1,
			},
		},
		{
			name: "resource found - update resource - unchanged",
			haveContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Uid:           "egress-1",
							},
						},
						BootstrapServers: "broker:9092",
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
					},
				},
				Generation: 1,
			},
			newResource: &contract.Resource{
				Uid:    "1",
				Topics: []string{"topic-name-1"},
				Egresses: []*contract.Egress{
					{
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Destination:   "http://localhost:8080",
						ConsumerGroup: "egress-1",
						Uid:           "egress-1",
					},
				},
				BootstrapServers: "broker:9092",
				Ingress: &contract.Ingress{
					IngressType: &contract.Ingress_Path{
						Path: "/broker-ns/broker-name",
					},
					ContentMode: contract.ContentMode_STRUCTURED,
				},
			},
			index:   0,
			changed: ResourceUnchanged,
			wantContract: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid:    "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								Uid:           "egress-1",
							},
						},
						BootstrapServers: "broker:9092",
						Ingress: &contract.Ingress{
							IngressType: &contract.Ingress_Path{
								Path: "/broker-ns/broker-name",
							},
							ContentMode: contract.ContentMode_STRUCTURED,
						},
					},
				},
				Generation: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := AddOrUpdateResourceConfig(tt.haveContract, tt.newResource, tt.index, zap.NewNop())

			if diff := cmp.Diff(tt.wantContract, tt.haveContract, protocmp.Transform()); diff != "" {
				t.Errorf("(-want, +got) %s", diff)
			}
			if changed != tt.changed {
				t.Errorf("Changed want %d got %d", tt.changed, changed)
			}
		})
	}
}

func TestDeleteResource(t *testing.T) {

	tests := []struct {
		name  string
		ct    *contract.Contract
		index int
		want  contract.Contract
	}{
		{
			name: "1 resource",
			ct: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
				},
				Generation: 200,
			},
			index: 0,
			want: contract.Contract{
				Generation: 200,
			},
		},
		{
			name: "2 resource",
			ct: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
					{
						Uid: "2",
					},
				},
			},
			index: 0,
			want: contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "2",
					},
				},
			},
		},
		{
			name: "3 resource - delete last",
			ct: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
					{
						Uid: "2",
					},
					{
						Uid: "3",
					},
				},
			},
			index: 2,
			want: contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
					{
						Uid: "2",
					},
				},
			},
		},
		{
			name: "3 broker - middle",
			ct: &contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
					{
						Uid: "2",
					},
					{
						Uid: "3",
					},
				},
				Generation: 200,
			},
			index: 1,
			want: contract.Contract{
				Resources: []*contract.Resource{
					{
						Uid: "1",
					},
					{
						Uid: "3",
					},
				},
				Generation: 200,
			},
		},
	}
	for i := range tests {
		t.Run(tests[i].name, func(t *testing.T) {

			DeleteResource(tests[i].ct, tests[i].index)

			assert.Equal(t, tests[i].ct, &tests[i].want)
		})
	}
}

func TestSetResourceEgressesFromContract(t *testing.T) {
	tests := []struct {
		name         string
		contract     *contract.Contract
		resource     *contract.Resource
		wantResource *contract.Resource
		index        int
	}{
		{
			name:         "copy egresses",
			contract:     &contract.Contract{Resources: []*contract.Resource{{Uid: "aaa", Egresses: []*contract.Egress{{Uid: "bbb"}}}}},
			resource:     &contract.Resource{Uid: "aaa"},
			wantResource: &contract.Resource{Uid: "aaa", Egresses: []*contract.Egress{{Uid: "bbb"}}},
			index:        0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetResourceEgressesFromContract(tt.contract, tt.resource, tt.index)
			if diff := cmp.Diff(tt.wantResource, tt.resource, protocmp.Transform()); diff != "" {
				t.Error("(-want, +got)", diff)
			}
		})
	}
}

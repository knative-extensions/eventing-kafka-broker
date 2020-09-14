package config

import (
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"testing"

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
							Id: "2",
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
							Id: "1",
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
	type args struct {
		brokers      *contract.Contract
		brokerConfig *contract.Resource
		index        int
	}
	tests := []struct {
		name    string
		args    args
		brokers contract.Contract
		want    contract.Contract
	}{
		{
			name: "resource not found - add resource",
			args: args{
				brokers: &contract.Contract{
					Resources: []*contract.Resource{
						{
							Id:     "2",
							Topics: []string{"topic-name-1"},
							Egresses: []*contract.Egress{
								{
									Destination:   "http://localhost:8080",
									ConsumerGroup: "egress-1",
									DeadLetter:    "http://localhost:8080",
									Filter: &contract.Filter{
										Attributes: map[string]string{
											"source": "source1",
										},
									},
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
				brokerConfig: &contract.Resource{
					Id:     "1",
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
							DeadLetter:    "http://localhost:8080",
							Filter: &contract.Filter{
								Attributes: map[string]string{
									"source": "source1",
								},
							},
						},
					},
					BootstrapServers: "broker:9092",
				},
				index: NoResource,
			},
			want: contract.Contract{
				Resources: []*contract.Resource{
					{
						Id:     "2",
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
								DeadLetter:    "http://localhost:8080",
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
							},
						},
						BootstrapServers: "broker:9092",
					},
					{
						Id:     "1",
						Topics: []string{"topic-name-1"},
						Egresses: []*contract.Egress{
							{
								Destination:   "http://localhost:8080",
								ConsumerGroup: "egress-1",
								DeadLetter:    "http://localhost:8080",
								Filter: &contract.Filter{
									Attributes: map[string]string{
										"source": "source1",
									},
								},
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
			args: args{
				brokers: &contract.Contract{
					Resources: []*contract.Resource{
						{
							Id:     "1",
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
									DeadLetter:    "http://localhost:8080",
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
				brokerConfig: &contract.Resource{
					Id:     "1",
					Topics: []string{"topic-name-1"},
					// Any Trigger will be ignored, since the function will be called when we're reconciling a contract.Resource,
					// and it should preserve Egresses already added to the ConfigMap.
					Egresses: []*contract.Egress{
						{
							Filter: &contract.Filter{
								Attributes: map[string]string{
									"source": "source2",
								},
							},
							Destination:   "http://localhost:8080",
							ConsumerGroup: "egress-1",
							DeadLetter:    "http://localhost:8080",
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
			},
			want: contract.Contract{
				Resources: []*contract.Resource{
					{
						Id:     "1",
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
								DeadLetter:    "http://localhost:8080",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AddOrUpdateResourceConfig(tt.args.brokers, tt.args.brokerConfig, tt.args.index, zap.NewNop())

			if diff := cmp.Diff(tt.want, *tt.args.brokers); diff != "" {
				t.Errorf("(-want, +got) %s", diff)
			}
		})
	}
}

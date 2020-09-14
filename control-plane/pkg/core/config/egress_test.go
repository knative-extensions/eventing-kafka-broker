package config

import (
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestFindEgress(t *testing.T) {
	type args struct {
		egresses []*contract.Egress
		egress   types.UID
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "egress not found",
			args: args{
				egresses: []*contract.Egress{
					{
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Destination:   "http://localhost:9090",
						ConsumerGroup: "2",
					},
				},
				egress: "1",
			},
			want: NoEgress,
		},
		{
			name: "egress found",
			args: args{
				egresses: []*contract.Egress{
					{
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Destination:   "http://localhost:9090",
						ConsumerGroup: "2",
					},
					{
						Filter: &contract.Filter{
							Attributes: map[string]string{
								"source": "source1",
							},
						},
						Destination:   "http://localhost:9090",
						ConsumerGroup: "1",
					},
				},
				egress: "1",
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindEgress(tt.args.egresses, tt.args.egress); got != tt.want {
				t.Errorf("FindEgress() = %v, want %v", got, tt.want)
			}
		})
	}
}

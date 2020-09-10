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

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"

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

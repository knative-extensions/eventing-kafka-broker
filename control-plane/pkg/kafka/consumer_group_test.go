/*
 * Copyright 2023 The Knative Authors
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

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"

	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
)

func TestNewClusterAdminClientFuncIsConsumerGroupPresent(t *testing.T) {
	tests := []struct {
		name           string
		clusterAdmin   sarama.ClusterAdmin
		consumerGroups []string
		want           bool
		wantErr        bool
	}{
		{
			name: "consumergroup does not exist",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedConsumerGroups: []string{"consumer-group-name-1"},
				ExpectedGroupDescriptionOnDescribeConsumerGroups: []*sarama.GroupDescription{
					{
						GroupId: "consumer-group-name-1",
						State:   "Dead",
					},
				},
				T: t,
			},
			consumerGroups: []string{"consumer-group-name-1"},
			want:           false,
			wantErr:        false,
		},
		{
			name: "consumergroup exists (empty)",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedConsumerGroups: []string{"consumer-group-name-1"},
				ExpectedGroupDescriptionOnDescribeConsumerGroups: []*sarama.GroupDescription{
					{
						GroupId: "consumer-group-name-1",
						State:   "Empty",
					},
				},
				T: t,
			},
			consumerGroups: []string{"consumer-group-name-1"},
			want:           true,
			wantErr:        false,
		},
		{
			name: "consumergroup exists (Stable)",
			clusterAdmin: &kafkatesting.MockKafkaClusterAdmin{
				ExpectedConsumerGroups: []string{"consumer-group-name-1"},
				ExpectedGroupDescriptionOnDescribeConsumerGroups: []*sarama.GroupDescription{
					{
						GroupId: "consumer-group-name-1",
						State:   "Stable",
					},
				},
				T: t,
			},
			consumerGroups: []string{"consumer-group-name-1"},
			want:           true,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AreConsumerGroupsPresentAndValid(tt.clusterAdmin, tt.consumerGroups...)
			if (err != nil) != tt.wantErr {
				t.Errorf("AreConsumerGroupsPresentAndValid() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AreConsumerGroupsPresentAndValid() got = %v, want %v", got, tt.want)
			}
		})
	}
}

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"

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
			wantErr:        true,
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

func TestInvalidOrNotPresentConsumerGroup(t *testing.T) {
	err := &InvalidOrNotPresentConsumerGroup{ConsumerGroup: "groupname"}

	require.Contains(t, err.Error(), err.ConsumerGroup)
}

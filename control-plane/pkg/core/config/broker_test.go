package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/types"
)

func TestFindBroker(t *testing.T) {
	type args struct {
		brokers *Brokers
		broker  types.UID
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "broker not found",
			args: args{
				brokers: &Brokers{
					Brokers: []*Broker{
						{
							Id: "2",
						},
					},
					VolumeGeneration: 1,
				},
				broker: "1",
			},
			want: NoBroker,
		},
		{
			name: "broker found",
			args: args{
				brokers: &Brokers{
					Brokers: []*Broker{
						{
							Id: "1",
						},
					},
					VolumeGeneration: 1,
				},
				broker: "1",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FindBroker(tt.args.brokers, tt.args.broker); got != tt.want {
				t.Errorf("FindBroker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddOrUpdateBrokersConfig(t *testing.T) {
	type args struct {
		brokers      *Brokers
		brokerConfig *Broker
		index        int
	}
	tests := []struct {
		name    string
		args    args
		brokers Brokers
		want    Brokers
	}{
		{
			name: "broker not found - add broker",
			args: args{
				brokers: &Brokers{
					Brokers: []*Broker{
						{
							Id:             "2",
							Topic:          "topic-name-1",
							DeadLetterSink: "http://localhost:8080",
							Triggers: []*Trigger{
								{
									Attributes: map[string]string{
										"source": "source1",
									},
									Destination: "http://localhost:8080",
									Id:          "trigger-1",
								},
							},
							Path:             "/broker-ns/broker-name",
							BootstrapServers: "broker:9092",
							ContentMode:      ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				},
				brokerConfig: &Broker{
					Id:             "1",
					Topic:          "topic-name-1",
					DeadLetterSink: "http://localhost:8080",
					Triggers: []*Trigger{
						{
							Attributes: map[string]string{
								"source": "source1",
							},
							Destination: "http://localhost:8080",
							Id:          "trigger-1",
						},
					},
					Path:             "/broker-ns/broker-name",
					BootstrapServers: "broker:9092",
					ContentMode:      ContentMode_STRUCTURED,
				},
				index: NoBroker,
			},
			want: Brokers{
				Brokers: []*Broker{
					{
						Id:             "2",
						Topic:          "topic-name-1",
						DeadLetterSink: "http://localhost:8080",
						Triggers: []*Trigger{
							{
								Attributes: map[string]string{
									"source": "source1",
								},
								Destination: "http://localhost:8080",
								Id:          "trigger-1",
							},
						},
						Path:             "/broker-ns/broker-name",
						BootstrapServers: "broker:9092",
						ContentMode:      ContentMode_STRUCTURED,
					},
					{
						Id:             "1",
						Topic:          "topic-name-1",
						DeadLetterSink: "http://localhost:8080",
						Triggers: []*Trigger{
							{
								Attributes: map[string]string{
									"source": "source1",
								},
								Destination: "http://localhost:8080",
								Id:          "trigger-1",
							},
						},
						Path:             "/broker-ns/broker-name",
						BootstrapServers: "broker:9092",
						ContentMode:      ContentMode_STRUCTURED,
					},
				},
				VolumeGeneration: 1,
			},
		},
		{
			name: "broker found - update broker",
			args: args{
				brokers: &Brokers{
					Brokers: []*Broker{
						{
							Id:             "1",
							Topic:          "topic-name-1",
							DeadLetterSink: "http://localhost:8080",
							Triggers: []*Trigger{
								{
									Attributes: map[string]string{
										"source": "source1",
									},
									Destination: "http://localhost:8080",
									Id:          "trigger-1",
								},
							},
							Path:             "/broker-ns/broker-name",
							BootstrapServers: "broker:9092",
							ContentMode:      ContentMode_STRUCTURED,
						},
					},
					VolumeGeneration: 1,
				},
				brokerConfig: &Broker{
					Id:             "1",
					Topic:          "topic-name-1",
					DeadLetterSink: "http://localhost:8080",
					// Any Trigger will be ignored, since the function will be called when we're reconciling a Broker,
					// and it should preserve Triggers already added to the ConfigMap.
					Triggers: []*Trigger{
						{
							Attributes: map[string]string{
								"source": "source2",
							},
							Destination: "http://localhost:8080",
							Id:          "trigger-1",
						},
					},
					Path:             "/broker-ns/broker-name",
					BootstrapServers: "broker:9092,broker-2:9092",
					ContentMode:      ContentMode_STRUCTURED,
				},
				index: 0,
			},
			want: Brokers{
				Brokers: []*Broker{
					{
						Id:             "1",
						Topic:          "topic-name-1",
						DeadLetterSink: "http://localhost:8080",
						Triggers: []*Trigger{
							{
								Attributes: map[string]string{
									"source": "source1",
								},
								Destination: "http://localhost:8080",
								Id:          "trigger-1",
							},
						},
						Path:             "/broker-ns/broker-name",
						BootstrapServers: "broker:9092,broker-2:9092",
						ContentMode:      ContentMode_STRUCTURED,
					},
				},
				VolumeGeneration: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AddOrUpdateBrokersConfig(tt.args.brokers, tt.args.brokerConfig, tt.args.index, zap.NewNop())

			if diff := cmp.Diff(tt.want, *tt.args.brokers); diff != "" {
				t.Errorf("(-want, +got) %s", diff)
			}
		})
	}
}

package v1alpha1

import (
	"context"
	"testing"

	"k8s.io/utils/pointer"
	"knative.dev/pkg/apis"
)

func TestKafkaSink_Validate(t *testing.T) {

	tests := []struct {
		name string
		ks   *KafkaSink
		ctx  context.Context
		want *apis.FieldError
	}{
		{
			name: "no bootstrap servers",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "",
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("", "spec.bootstrapServers"),
		},
		{
			name: "no bootstrap servers",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					BootstrapServers: "broker-1:9092",
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("", "spec.topic"),
		},
		{
			name: "structured content mode",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-1:9092",
					ContentMode:      pointer.StringPtr(ModeStructured),
				},
			},
			ctx: context.Background(),
		},
		{
			name: "binary content mode",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-1:9092",
					ContentMode:      pointer.StringPtr(ModeBinary),
				},
			},
			ctx: context.Background(),
		},
		{
			name: "invalid content mode",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-1:9092",
					ContentMode:      pointer.StringPtr("str"),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("str", "spec.contentMode"),
		},
		{
			name: "immutable replication factor",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  "broker-1:9092",
					ReplicationFactor: 10,
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  "broker-2:9092",
					ReplicationFactor: 11,
				},
			}),
			want: ErrImmutableField("spec.replicationFactor"),
		},
		{
			name: "immutable num partitions",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-1:9092",
					NumPartitions:    10,
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-2:9092",
					NumPartitions:    11,
				},
			}),
			want: ErrImmutableField("spec.numPartitions"),
		},
		{
			name: "immutable topic",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: "broker-1:9092",
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-2",
					BootstrapServers: "broker-2:9092",
				},
			}),
			want: ErrImmutableField("spec.topic"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.ks.SetDefaults(tt.ctx)

			if got := tt.ks.Validate(tt.ctx); got.Error() != tt.want.Error() {
				t.Errorf("Validate() = %v, want %v", got, tt.want)
			}
		})
	}
}

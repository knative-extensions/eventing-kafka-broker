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
					BootstrapServers: []string{},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue([]string{}, "spec.bootstrapServers"),
		},
		{
			name: "no topic",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					BootstrapServers: []string{"broker-1:9092"},
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
					BootstrapServers: []string{"broker-1:9092"},
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
					BootstrapServers: []string{"broker-1:9092"},
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
					BootstrapServers: []string{"broker-1:9092"},
					ContentMode:      pointer.StringPtr("str"),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("str", "spec.contentMode"),
		},
		{
			name: "invalid num partitions 0",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ContentMode:       pointer.StringPtr(ModeBinary),
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32Ptr(0),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("0", "spec.numPartitions"),
		},
		{
			name: "invalid num partitions",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ContentMode:       pointer.StringPtr(ModeBinary),
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32Ptr(-10),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("-10", "spec.numPartitions"),
		},
		{
			name: "invalid replication factor 0",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ContentMode:       pointer.StringPtr(ModeBinary),
					ReplicationFactor: pointerInt16(0),
					NumPartitions:     pointer.Int32Ptr(10),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("0", "spec.replicationFactor"),
		},
		{
			name: "invalid replication factor",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ContentMode:       pointer.StringPtr(ModeBinary),
					ReplicationFactor: pointerInt16(-10),
					NumPartitions:     pointer.Int32Ptr(10),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("-10", "spec.replicationFactor"),
		},
		{
			name: "immutable replication factor",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32Ptr(10),
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-2:9092"},
					ReplicationFactor: pointerInt16(11),
					NumPartitions:     pointer.Int32Ptr(10),
				},
			}),
			want: ErrImmutableField("spec.replicationFactor"),
		},
		{
			name: "immutable num partitions",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ReplicationFactor: pointerInt16(11),
					NumPartitions:     pointer.Int32Ptr(10),
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-2:9092"},
					ReplicationFactor: pointerInt16(11),
					NumPartitions:     pointer.Int32Ptr(11),
				},
			}),
			want: ErrImmutableField("spec.numPartitions"),
		},
		{
			name: "immutable topic",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: []string{"broker-1:9092"},
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-2",
					BootstrapServers: []string{"broker-2:9092"},
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

func pointerInt16(rf int16) *int16 {
	return &rf
}

/*
 * Copyright 2021 The Knative Authors
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

package v1alpha1

import (
	"context"
	"testing"

	"knative.dev/pkg/apis"
	pointer "knative.dev/pkg/ptr"
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
					ContentMode:      pointer.String(ModeStructured),
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
					ContentMode:      pointer.String(ModeBinary),
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
					ContentMode:      pointer.String("str"),
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
					ContentMode:       pointer.String(ModeBinary),
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32(0),
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
					ContentMode:       pointer.String(ModeBinary),
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32(-10),
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
					ContentMode:       pointer.String(ModeBinary),
					ReplicationFactor: pointerInt16(0),
					NumPartitions:     pointer.Int32(10),
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
					ContentMode:       pointer.String(ModeBinary),
					ReplicationFactor: pointerInt16(-10),
					NumPartitions:     pointer.Int32(10),
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("-10", "spec.replicationFactor"),
		},
		{
			name: "invalid secret name",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:            "topic-name-1",
					BootstrapServers: []string{"broker-1:9092"},
					ContentMode:      pointer.String(ModeStructured),
					Auth:             &Auth{Secret: &Secret{Ref: &SecretReference{}}},
				},
			},
			ctx:  context.Background(),
			want: apis.ErrInvalidValue("", "spec.auth.secret.ref.name"),
		},
		{
			name: "immutable replication factor",
			ks: &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-1:9092"},
					ReplicationFactor: pointerInt16(10),
					NumPartitions:     pointer.Int32(10),
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-2:9092"},
					ReplicationFactor: pointerInt16(11),
					NumPartitions:     pointer.Int32(10),
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
					NumPartitions:     pointer.Int32(10),
				},
			},
			ctx: apis.WithinUpdate(context.Background(), &KafkaSink{
				Spec: KafkaSinkSpec{
					Topic:             "topic-name-1",
					BootstrapServers:  []string{"broker-2:9092"},
					ReplicationFactor: pointerInt16(11),
					NumPartitions:     pointer.Int32(11),
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

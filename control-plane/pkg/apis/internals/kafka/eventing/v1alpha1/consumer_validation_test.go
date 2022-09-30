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

	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

func TestConsumer_Validate(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		given   *Consumer
		wantErr bool
	}{
		{
			name:    "no topics",
			ctx:     context.Background(),
			given:   &Consumer{},
			wantErr: true,
		},
		{
			name: "missing group.id",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid multiple reply strategies (topic, destination)",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Reply: &ReplyStrategy{
						TopicReply: &TopicReply{Enabled: true},
						URLReply:   &DestinationReply{Enabled: true},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid no pod name",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodNamespace: "ns",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid no pod namespace",
			ctx:  context.Background(),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName: "p-0",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid - no updates",
			ctx: apis.WithinUpdate(context.Background(), &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns",
					},
				},
			},
			),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid pod name update",
			ctx: apis.WithinUpdate(context.Background(), &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-1",
						PodNamespace: "ns",
					},
				},
			},
			),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid pod name update",
			ctx: apis.WithinUpdate(context.Background(), &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns-1",
					},
				},
			},
			),
			given: &Consumer{
				Spec: ConsumerSpec{
					Topics: []string{"t1"},
					Configs: ConsumerConfigs{
						Configs: map[string]string{
							"group.id":          "g1",
							"bootstrap.servers": "kafka:9092",
						},
					},
					Delivery: &DeliverySpec{
						DeliverySpec: &eventingduck.DeliverySpec{},
					},
					Subscriber: duckv1.Destination{
						URI: &apis.URL{
							Scheme: "http",
							Host:   "127.0.0.1",
						},
					},
					PodBind: &PodBind{
						PodName:      "p-0",
						PodNamespace: "ns-2",
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.given.Validate(tt.ctx); (err != nil) != tt.wantErr {
				t.Errorf("want err = %v, got err %v", tt.wantErr, err)
			}
		})
	}
}

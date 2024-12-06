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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	kafkasource "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/sources/v1beta1"
)

const (
	DefaultDeliveryOrder = kafkasource.Ordered
)

func TestConsumerGroup_Validate(t *testing.T) {
	backoffPolicy := eventingduck.BackoffPolicyExponential

	tests := []struct {
		name    string
		ctx     context.Context
		given   *ConsumerGroup
		wantErr bool
	}{
		{
			name:    "no replicas",
			ctx:     context.Background(),
			given:   &ConsumerGroup{},
			wantErr: true,
		},
		{
			name: "no selector",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
				},
			},
			wantErr: true,
		},
		{
			name: "no consumer template",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
				},
			},
			wantErr: true,
		},
		{
			name: "no subscriber",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{
									Retry:         pointer.Int32(10),
									BackoffPolicy: &backoffPolicy,
									BackoffDelay:  pointer.String("PT0.3S"),
									Timeout:       pointer.String("PT600S"),
								},
								Ordering: DefaultDeliveryOrder,
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no delivery",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "subscriber different namespace",
			ctx:  apis.AllowDifferentNamespace(context.Background()),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-cg",
					Namespace: "my-ns",
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Subscriber: duckv1.Destination{
								Ref: &duckv1.KReference{
									Kind:       "Sequence",
									Namespace:  "ma-ns-2",
									Name:       "my-seq",
									APIVersion: "flows.knative.dev/v1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid delivery - timeout feature disabled",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{
									Retry:   pointer.Int32(-10),
									Timeout: pointer.String("PT600S"),
								},
								Ordering: DefaultDeliveryOrder,
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "valid delivery - timeout feature enabled",
			ctx:  feature.ToContext(context.Background(), feature.Flags{feature.DeliveryTimeout: feature.Enabled}),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{
									Timeout: pointer.String("PT600S"),
								},
								Ordering: DefaultDeliveryOrder,
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid",
			ctx:  context.Background(),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid with channel label",
			ctx: apis.WithinUpdate(context.Background(), &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						KafkaChannelNameLabel: "channelName", // identifies the new ConsumerGroup as associated with this channel
					},
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			}),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						KafkaChannelNameLabel: "channelName", // identifies the new ConsumerGroup as associated with this channel
					},
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
							Configs: ConsumerConfigs{
								Configs: map[string]string{},
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid without channel label",
			ctx: apis.WithinUpdate(context.Background(), &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						KafkaChannelNameLabel: "channelName", // identifies the new ConsumerGroup as associated with this channel
					},
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
						},
					},
				},
			}),
			given: &ConsumerGroup{
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid with different channel label value",
			ctx: apis.WithinUpdate(context.Background(), &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						KafkaChannelNameLabel: "channelName", // identifies the new ConsumerGroup as associated with this channel
					},
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
						},
					},
				},
			}),
			given: &ConsumerGroup{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						KafkaChannelNameLabel: "channelBadName", // identifies the new ConsumerGroup as associated with this channel
					},
				},
				Spec: ConsumerGroupSpec{
					Replicas: pointer.Int32(1),
					Selector: map[string]string{"app": "app"},
					Template: ConsumerTemplateSpec{
						Spec: ConsumerSpec{
							Delivery: &DeliverySpec{
								DeliverySpec: &eventingduck.DeliverySpec{},
							},
							Subscriber: duckv1.Destination{
								URI: &apis.URL{
									Scheme: "http",
									Host:   "127.0.0.1",
								},
							},
						},
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

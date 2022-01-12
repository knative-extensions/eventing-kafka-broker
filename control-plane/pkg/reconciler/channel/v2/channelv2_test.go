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

package v2

import (
	"context"
	"fmt"
	"testing"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgotesting "k8s.io/client-go/testing"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/system"

	messagingv1beta1 "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"

	fakeconsumergroupinformer "knative.dev/eventing-kafka-broker/control-plane/pkg/client/internals/kafka/injection/client/fake"
	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	messagingv1beta1kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-channel-channels-subscriptions",
	GeneralConfigMapName:        "kafka-channel-config",
}

func TestReconcileKind(t *testing.T) {

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(conditionSet)

	env := *DefaultEnv
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
		{
			Name: "Reconciled normal - no subscription - no auth",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantCreates: []runtime.Object{},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusChannelConsumerGroup(),
						StatusConfigMapUpdatedReady(&env),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
						WithSubscribers(Subscriber1()),
						StatusChannelConsumerGroup(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with single unready subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithUnreadySubscriber))),
				NewService(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
						WithSubscribers(Subscriber1()),
						StatusChannelConsumerGroup(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with single ready subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1())),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
						WithSubscribers(Subscriber1()),
						StatusChannelConsumerGroup(),
					),
				},
			},
		},
		{
			Name: "Reconciled normal - with two fresh subscribers - no auth",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber)),
					WithSubscribers(Subscriber2(WithFreshSubscriber)),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
						WithSubscribers(Subscriber1(), Subscriber2()),
						StatusChannelConsumerGroup(),
					),
				},
			},
		},
		{
			Name: "channel configmap not resolved",
			Objects: []runtime.Object{
				NewChannel(),
			},
			Key:     testKey,
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, system.Namespace(), DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName)),
					),
				},
			},
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf(`failed to get contract configuration: failed to get configmap %s/%s: configmap %q not found`, system.Namespace(), DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName),
				),
			},
		},
		{
			Name: "channel configmap does not have bootstrap servers",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					"foo": "bar",
				}),
			},
			Key:     testKey,
			WantErr: true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneAvailable,
						StatusConfigNotParsed("unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]"),
					),
				},
			},
			WantEvents: []string{
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]",
				),
			},
		},
		{
			Name: "Channel spec is used properly",
			Objects: []runtime.Object{
				NewChannel(
					WithNumPartitions(3),
					WithReplicationFactor(4),
					WithRetentionDuration("1000"),
				),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithNumPartitions(3),
						WithReplicationFactor(4),
						WithRetentionDuration("1000"),
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
					),
				},
			},
		},
		{
			Name: "Create contract configmap when it does not exist",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
					),
				},
			},
		},
		{
			Name: "Do not create contract configmap when it exists",
			Objects: []runtime.Object{
				NewChannel(),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(&env, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(&env, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
							},
						},
					},
				}),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigMapUpdatedReady(&env),
					),
				},
			},
		},
	}

	table.Test(t, NewFactory(&env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {
		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
			},
			Env:                 env,
			ConfigMapLister:     listers.GetConfigMapLister(),
			ConsumerGroupLister: listers.GetConsumerGroupLister(),
			InternalsClient:     fakeconsumergroupinformer.Get(ctx),
		}

		r := messagingv1beta1kafkachannelreconciler.NewReconciler(
			ctx,
			logging.FromContext(ctx),
			fakeeventingkafkaclient.Get(ctx),
			listers.GetKafkaChannelLister(),
			controller.GetEventRecorder(ctx),
			reconciler,
		)
		return r
	}))
}

func StatusChannelConsumerGroup() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.GetConditionSet().Manage(ch.GetStatus()).MarkTrue(KafkaChannelConditionConsumerGroup)
	}
}

func StatusChannelConsumerGroupFailed() KRShapedOption {
	return func(obj duckv1.KRShaped) {
		ch := obj.(*messagingv1beta1.KafkaChannel)
		ch.GetConditionSet().Manage(ch.GetStatus()).MarkFalse(KafkaChannelConditionConsumerGroup, "failed to reconcile consumer group", "")
	}
}

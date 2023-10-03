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

package channel_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"text/template"

	apisconfig "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/sarama"
	"k8s.io/utils/pointer"
	eventingduck "knative.dev/eventing/pkg/apis/duck/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/eventing/pkg/eventingtls/eventingtlstesting"
	eventingrekttesting "knative.dev/eventing/pkg/reconciler/testing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/network"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/kafka/testing"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client/fake"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"
	"knative.dev/pkg/tracker"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/config"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober/probertesting"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/receiver"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/base"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/channel"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	"github.com/rickb777/date/period"
	messagingv1beta "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client/fake"
	messagingv1beta1kafkachannelreconciler "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
)

const (
	testProber = "testProber"

	finalizerName                 = "kafkachannels.messaging.knative.dev"
	TestExpectedDataNumPartitions = "TestExpectedDataNumPartitions"
	TestExpectedReplicationFactor = "TestExpectedReplicationFactor"
	TestExpectedRetentionDuration = "TestExpectedRetentionDuration"

	kafkaFeatureFlags = "kafka-feature-flags"
)

var (
	testCaCerts = string(eventingtlstesting.CA)

	kafkaChannelTLSSecretName = "kafka-channel-ingress-server-tls"
)

var finalizerUpdatedEvent = Eventf(
	corev1.EventTypeNormal,
	"FinalizerUpdate",
	fmt.Sprintf(`Updated %q finalizers`, ChannelName),
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	ContractConfigMapName:       "kafka-channel-channels-subscriptions",
	GeneralConfigMapName:        "kafka-channel-config",
	IngressName:                 "kafka-channel-ingress",
	SystemNamespace:             "knative-eventing",
	ContractConfigMapFormat:     base.Json,
}

var customChannelTopicTemplate = customTemplate()

func TestReconcileKind(t *testing.T) {

	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(base.IngressConditionSet)

	env := *DefaultEnv
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
		{
			Name: "bad workqueue key",
			// Make sure Reconcile handles bad keys.
			Key: "too/many/parts",
		},
		{
			Name: "key not found",
			// Make sure Reconcile handles good keys that don't exist.
			Key: "foo/not-found",
		},
		{
			Name: "Channel not found",
			Key:  testKey,
		},
		{
			Name: "Channel is being deleted, probe not ready",
			Key:  testKey,
			Objects: []runtime.Object{
				NewChannel(
					WithChannelTopicStatusAnnotation(defaultTopicName()),
					WithInitKafkaChannelConditions,
					WithDeletedTimeStamp),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Channel is being deleted, probe ready",
			Key:  testKey,
			Objects: []runtime.Object{
				NewChannel(
					WithInitKafkaChannelConditions,
					WithDeletedTimeStamp),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantErr: true,
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusReady),
			},
		},
		{
			Name: "Reconciled normal - no subscription - no auth",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(DefaultEnv),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with delivery",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
								Retry:      5,
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithChannelDelivery(&eventingduck.DeliverySpec{
							DeadLetterSink: ServiceDestination,
							Retry:          pointer.Int32(5),
						}),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithChannelDeadLetterSinkURI(ServiceURL),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled failed - probe " + prober.StatusNotReady.String(),
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						StatusProbeFailed(prober.StatusNotReady),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
		{
			Name: "Reconciled failed - probe " + prober.StatusUnknown.String(),
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(&env),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						StatusProbeFailed(prober.StatusUnknown),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusUnknown),
			},
		},
		{
			Name: "Reconciled failed - with single fresh subscriber without URI",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber, WithNoSubscriberURI))),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						WithSubscribers(Subscriber1(WithFreshSubscriber, WithNoSubscriberURI)),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to resolve subscriber config: failed to resolve Subscription.Spec.Subscriber: empty subscriber URI",
				),
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - no auth - no subscription",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single unready subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithUnreadySubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single ready subscriber - no auth",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1())),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with two fresh subscribers - no auth",
			Objects: []runtime.Object{
				NewChannel(
					WithSubscribers(Subscriber1(WithFreshSubscriber)),
					WithSubscribers(Subscriber2(WithFreshSubscriber)),
				),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
				eventingrekttesting.NewSubscription(Subscription2Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription2UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}, {
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription2UUID,
								Destination:   "http://" + Subscription2URI,
								Uid:           Subscription2UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_DiscardReply{},
								Reference: &contract.Reference{
									Name:      Subscription2Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription2UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1(), Subscriber2()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Data plane not available - no receiver",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key:                     testKey,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantErr:                 true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneNotAvailable,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf("%s: %s", base.ReasonDataPlaneNotAvailable, base.MessageDataPlaneNotAvailable),
				),
			},
		},
		{
			Name: "Data plane not available - no dispatcher",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key:                     testKey,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantErr:                 true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneNotAvailable,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					fmt.Sprintf("%s: %s", base.ReasonDataPlaneNotAvailable, base.MessageDataPlaneNotAvailable),
				),
			},
		},
		{
			Name: "channel configmap not resolved",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneAvailable,
						StatusConfigNotParsed(fmt.Sprintf(`failed to get configmap %s/%s: configmap %q not found`, system.Namespace(), DefaultEnv.GeneralConfigMapName, DefaultEnv.GeneralConfigMapName)),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
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
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					"foo": "bar",
				}),
			},
			Key:                     testKey,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantErr:                 true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneAvailable,
						StatusConfigNotParsed("unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]"),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[foo:bar]",
				),
			},
		},
		{
			Name: "channel configmap has blank bootstrap servers",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: "",
				}),
			},
			Key:                     testKey,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantErr:                 true,
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneAvailable,
						StatusConfigNotParsed("unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[bootstrap.servers:]"),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get contract configuration: unable to get bootstrapServers from configmap: invalid configuration bootstrapServers: [] - ConfigMap data: map[bootstrap.servers:]",
				),
			},
		},
		{
			Name: "Channel spec is used properly",
			Objects: []runtime.Object{
				NewChannel(
					WithNumPartitions(3),
					WithReplicationFactor(4),
					WithRetentionDuration("PT10M"),
				),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			OtherTestData: map[string]interface{}{
				TestExpectedDataNumPartitions: int32(3),
				TestExpectedReplicationFactor: int16(4),
				TestExpectedRetentionDuration: "PT10M",
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithNumPartitions(3),
						WithReplicationFactor(4),
						WithRetentionDuration("PT10M"),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - with auth - SSL",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySSLSecret("ns-1", "secret-1"),
				NewSSLSecret(system.Namespace(), "secret-1"),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SSL,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "user.key", Field: contract.SecretField_USER_KEY},
											{SecretKey: "user.crt", Field: contract.SecretField_USER_CRT},
											{SecretKey: "ca.crt", Field: contract.SecretField_CA_CRT},
										},
									}},
								},
							},
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - with auth - SASL SSL",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySASLSSLSecret("ns-1", "secret-1"),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SASL_SSL,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "user.key", Field: contract.SecretField_USER_KEY},
											{SecretKey: "user.crt", Field: contract.SecretField_USER_CRT},
											{SecretKey: "ca.crt", Field: contract.SecretField_CA_CRT},
											{SecretKey: "password", Field: contract.SecretField_PASSWORD},
											{SecretKey: "username", Field: contract.SecretField_USER},
											{SecretKey: "saslType", Field: contract.SecretField_SASL_MECHANISM},
										},
									}},
								},
							},
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - with single fresh subscriber - with auth - SASL",
			Objects: []runtime.Object{
				NewChannel(WithSubscribers(Subscriber1(WithFreshSubscriber))),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
					security.AuthSecretNameKey:         "secret-1",
					security.AuthSecretNamespaceKey:    "ns-1",
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewLegacySASLSecret("ns-1", "secret-1"),
				eventingrekttesting.NewSubscription(Subscription1Name, ChannelNamespace,
					eventingrekttesting.WithSubscriptionUID(Subscription1UUID),
				),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Auth: &contract.Resource_MultiAuthSecret{
								MultiAuthSecret: &contract.MultiSecretReference{
									Protocol: contract.Protocol_SASL_PLAINTEXT,
									References: []*contract.SecretReference{{
										Reference: &contract.Reference{
											Uuid:      SecretUUID,
											Namespace: "ns-1",
											Name:      "secret-1",
											Version:   SecretResourceVersion,
										},
										KeyFieldReferences: []*contract.KeyFieldReference{
											{SecretKey: "password", Field: contract.SecretField_PASSWORD},
											{SecretKey: "username", Field: contract.SecretField_USER},
											{SecretKey: "saslType", Field: contract.SecretField_SASL_MECHANISM},
										},
									}},
								},
							},
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							Egresses: []*contract.Egress{{
								ConsumerGroup: "kafka." + ChannelNamespace + "." + ChannelName + "." + Subscription1UUID,
								Destination:   "http://" + Subscription1URI,
								Uid:           Subscription1UUID,
								DeliveryOrder: contract.DeliveryOrder_ORDERED,
								ReplyStrategy: &contract.Egress_ReplyUrl{ReplyUrl: "http://" + Subscription1ReplyURI},
								Reference: &contract.Reference{
									Name:      Subscription1Name,
									Namespace: ChannelNamespace,
									Uuid:      Subscription1UUID,
								},
							}},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						WithSubscribers(Subscriber1()),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Create contract configmap when it does not exist",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Do not create contract configmap when it exists",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Corrupt contract in configmap",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, []byte("corrupt")),
			},
			Key:                     testKey,
			WantErr:                 true,
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusDataPlaneAvailable,
						StatusConfigParsed,
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusConfigMapNotUpdatedReady(
							"Failed to get contract data from ConfigMap: knative-eventing/kafka-channel-channels-subscriptions",
							"failed to unmarshal contract: 'corrupt'",
						),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
				Eventf(
					corev1.EventTypeWarning,
					"InternalError",
					"failed to get broker and triggers data from config map knative-eventing/kafka-channel-channels-subscriptions: failed to unmarshal contract: 'corrupt'",
				),
			},
		},
		{
			Name: "Reconciled normal - custom topic template",
			Objects: []runtime.Object{
				NewChannel(),
				NewService(),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{CustomTopic(customChannelTopicTemplate)},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
				NewPerChannelService(DefaultEnv),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(CustomTopic(customChannelTopicTemplate)),
						StatusTopicReadyWithName(CustomTopic(customChannelTopicTemplate)),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
			OtherTestData: map[string]interface{}{
				kafkaFeatureFlags: newKafkaFeaturesConfigFromMap(&corev1.ConfigMap{
					Data: map[string]string{
						"channels.topic.template": "custom-channel-template.{{ .Namespace }}.{{ .Name }}",
					},
				}),
			},
		},
		{
			Name: "Reconciled normal - TLS Permissive",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				makeTLSSecret(),
			},
			Key: testKey,
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.TransportEncryption: feature.Permissive,
			}),
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
								Retry:      5,
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithChannelDelivery(&eventingduck.DeliverySpec{
							DeadLetterSink: ServiceDestination,
							Retry:          pointer.Int32(5),
						}),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithChannelDeadLetterSinkURI(ServiceURL),
						WithChannelAddresses([]duckv1.Addressable{
							{
								Name:    pointer.String("https"),
								URL:     httpsURL(ChannelServiceName, ChannelNamespace),
								CACerts: pointer.String(testCaCerts),
							},
							{
								Name: pointer.String("http"),
								URL:  ChannelAddress(),
							},
						}),
						WithChannelAddress(duckv1.Addressable{
							Name: pointer.String("http"),
							URL:  ChannelAddress(),
						}),
						WithChannelAddessable(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
		{
			Name: "Reconciled normal - TLS Strict",
			Objects: []runtime.Object{
				NewChannel(
					WithChannelDelivery(&eventingduck.DeliverySpec{
						DeadLetterSink: ServiceDestination,
						Retry:          pointer.Int32(5),
					}),
				),
				NewService(),
				NewPerChannelService(DefaultEnv),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				makeTLSSecret(),
			},
			Key: testKey,
			Ctx: feature.ToContext(context.Background(), feature.Flags{
				feature.TransportEncryption: feature.Strict,
			}),
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
							EgressConfig: &contract.EgressConfig{
								DeadLetter: ServiceURL,
								Retry:      5,
							},
						},
					},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					"annotation_to_preserve":           "value_to_preserve",
					base.VolumeGenerationAnnotationKey: "1",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the channel namespace with configmap namespace, so skip it
			WantCreates: []runtime.Object{
				NewConfigMapWithBinaryData(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithChannelDelivery(&eventingduck.DeliverySpec{
							DeadLetterSink: ServiceDestination,
							Retry:          pointer.Int32(5),
						}),
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						WithChannelTopicStatusAnnotation(ChannelTopic()),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						ChannelAddressable(&env),
						StatusProbeSucceeded,
						WithChannelDeadLetterSinkURI(ServiceURL),
						WithChannelAddresses([]duckv1.Addressable{
							{
								Name:    pointer.String("https"),
								URL:     httpsURL(ChannelServiceName, ChannelNamespace),
								CACerts: pointer.String(testCaCerts),
							},
						}),
						WithChannelAddress(duckv1.Addressable{
							Name:    pointer.String("https"),
							URL:     httpsURL(ChannelServiceName, ChannelNamespace),
							CACerts: pointer.String(testCaCerts),
						}),
						WithChannelAddessable(),
					),
				},
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchFinalizers(),
			},
			WantEvents: []string{
				finalizerUpdatedEvent,
			},
		},
	}

	useTable(t, table, env)
}

func TestFinalizeKind(t *testing.T) {

	t.Setenv("SYSTEM_NAMESPACE", "knative-eventing")

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(base.IngressConditionSet)

	env := *DefaultEnv
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
		{
			Name: "Finalize normal - no auth",
			Objects: []runtime.Object{
				NewDeletedChannel(WithChannelTopicStatusAnnotation(defaultTopicName())),
				NewConfigMapFromContract(&contract.Contract{
					Generation: 1,
					Resources: []*contract.Resource{
						{
							Uid:              ChannelUUID,
							Topics:           []string{ChannelTopic()},
							BootstrapServers: ChannelBootstrapServers,
							Reference:        ChannelReference(),
							Ingress: &contract.Ingress{
								Host: receiver.Host(ChannelNamespace, ChannelName),
							},
						},
					},
				}, env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat),
				NewConfigMapWithTextData(system.Namespace(), DefaultEnv.GeneralConfigMapName, map[string]string{
					kafka.BootstrapServersConfigMapKey: ChannelBootstrapServers,
				}),
				ChannelReceiverPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPod(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "0",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			Key: testKey,
			WantUpdates: []clientgotesting.UpdateActionImpl{
				ConfigMapUpdate(env.DataPlaneConfigMapNamespace, env.ContractConfigMapName, env.ContractConfigMapFormat, &contract.Contract{
					Generation: 2,
					Resources:  []*contract.Resource{},
				}),
				ChannelReceiverPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
					"annotation_to_preserve":           "value_to_preserve",
				}),
				ChannelDispatcherPodUpdate(env.SystemNamespace, map[string]string{
					base.VolumeGenerationAnnotationKey: "2",
					"annotation_to_preserve":           "value_to_preserve",
				}),
			},
			SkipNamespaceValidation: true, // WantCreates compare the source namespace with configmap namespace, so skip it
			OtherTestData: map[string]interface{}{
				testProber: probertesting.MockProber(prober.StatusNotReady),
			},
		},
	}

	useTable(t, table, env)
}

func useTable(t *testing.T, table TableTest, env config.Env) {
	table.Test(t, NewFactory(&env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {
		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		var featureFlags *apisconfig.KafkaFeatureFlags
		if v, ok := row.OtherTestData[kafkaFeatureFlags]; ok {
			featureFlags = v.(*apisconfig.KafkaFeatureFlags)
		} else {
			featureFlags = apisconfig.DefaultFeaturesConfig()
		}

		numPartitions := int32(1)
		if v, ok := row.OtherTestData[TestExpectedDataNumPartitions]; ok {
			numPartitions = v.(int32)
		}

		replicationFactor := int16(1)
		if v, ok := row.OtherTestData[TestExpectedReplicationFactor]; ok {
			replicationFactor = v.(int16)
		}

		retentionDuration := messagingv1beta.DefaultRetentionDuration
		if v, ok := row.OtherTestData[TestExpectedRetentionDuration]; ok {
			retentionPeriod, err := period.Parse(v.(string))
			if err != nil {
				t.Errorf("couldn't parse retention duration: %s", err.Error())
			}
			retentionDuration, _ = retentionPeriod.Duration()
		}

		retentionMillisString := strconv.FormatInt(retentionDuration.Milliseconds(), 10)

		expectedTopicName, err := featureFlags.ExecuteChannelsTopicTemplate(metav1.ObjectMeta{Name: ChannelName, Namespace: ChannelNamespace, UID: ChannelUUID})
		if err != nil {
			panic("failed to create expected topic name")
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				ContractConfigMapName:       env.ContractConfigMapName,
				ContractConfigMapFormat:     env.ContractConfigMapFormat,
				DataPlaneNamespace:          env.SystemNamespace,
				DispatcherLabel:             base.ChannelDispatcherLabel,
				ReceiverLabel:               base.ChannelReceiverLabel,
			},
			Env:                env,
			ConfigMapLister:    listers.GetConfigMapLister(),
			ServiceLister:      listers.GetServiceLister(),
			SubscriptionLister: listers.GetSubscriptionLister(),
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: expectedTopicName,
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     numPartitions,
						ReplicationFactor: replicationFactor,
						ConfigEntries: map[string]*string{
							messagingv1beta.KafkaTopicConfigRetentionMs: &retentionMillisString,
						},
					},
					T: t,
				}, nil
			},
			Prober:            proberMock,
			IngressHost:       network.GetServiceHostname(env.IngressName, env.SystemNamespace),
			KafkaFeatureFlags: featureFlags,
		}

		reconciler.Tracker = &FakeTracker{}
		reconciler.Tracker = &FakeTracker{}

		reconciler.Resolver = resolver.NewURIResolverFromTracker(ctx, tracker.New(func(name types.NamespacedName) {}, 0))

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

func patchFinalizers() clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = ChannelName
	action.Namespace = ChannelNamespace
	patch := `{"metadata":{"finalizers":["` + finalizerName + `"],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}

func defaultTopicName() string {
	topicName, err := apisconfig.DefaultFeaturesConfig().ExecuteChannelsTopicTemplate(metav1.ObjectMeta{Name: ChannelName, Namespace: ChannelNamespace})
	if err != nil {
		panic("failed to create default channel topic name")
	}
	return topicName
}

func customTemplate() *template.Template {
	channelsTemplate, _ := template.New("channels.topic.template").Parse("custom-channel-template.{{ .Namespace }}.{{ .Name }}")
	return channelsTemplate
}

func newKafkaFeaturesConfigFromMap(cm *corev1.ConfigMap) *apisconfig.KafkaFeatureFlags {
	featureFlags, err := apisconfig.NewFeaturesConfigFromMap(cm)
	if err != nil {
		panic("failed to create kafka features from config map")
	}
	return featureFlags
}

func makeTLSSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: DefaultEnv.SystemNamespace,
			Name:      kafkaChannelTLSSecretName,
		},
		Data: map[string][]byte{
			"ca.crt": []byte(testCaCerts),
		},
		Type: corev1.SecretTypeTLS,
	}
}

func httpsURL(name string, namespace string) *apis.URL {
	return &apis.URL{
		Scheme: "https",
		Host:   network.GetServiceHostname(DefaultEnv.IngressName, DefaultEnv.SystemNamespace),
		Path:   fmt.Sprintf("/%s/%s", namespace, name),
	}
}

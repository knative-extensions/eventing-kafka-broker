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
	"testing"

	"github.com/Shopify/sarama"
	"knative.dev/pkg/network"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgotesting "k8s.io/client-go/testing"

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
	kafkatesting "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka/testing"
	. "knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/testing"

	messagingv1beta "knative.dev/eventing-kafka/pkg/apis/messaging/v1beta1"
	fakeeventingkafkaclient "knative.dev/eventing-kafka/pkg/client/injection/client/fake"
	messagingv1beta1kafkachannelreconciler "knative.dev/eventing-kafka/pkg/client/injection/reconciler/messaging/v1beta1/kafkachannel"
	"knative.dev/eventing-kafka/pkg/common/constants"
)

// TODO: are we using all of these?
var configKafka = map[string]string{
	"version": "1.0.0",
	"sarama": `
enableLogging: false
config: |
  Version: 2.0.0 # Kafka Version Compatibility From Sarama's Supported List (Major.Minor.Patch)
  Admin:
    Timeout: 10000000000  # 10 seconds
  Net:
    KeepAlive: 30000000000  # 30 seconds
    MaxOpenRequests: 1 # Set to 1 for use with Idempotent Producer
    TLS:
      Enable: true
    SASL:
      Enable: true
      Version: 1
  Metadata:
    RefreshFrequency: 300000000000  # 5 minutes
`,
	"eventing-kafka": `
kafka:
  authSecretName: kafka-cluster
  authSecretNamespace: knative-eventing
  brokers: kafka:9092
`,
}

const (
	testProber = "testProber"

	finalizerName = "kafkachannels.messaging.knative.dev"
)

var finalizerUpdatedEvent = Eventf(
	corev1.EventTypeNormal,
	"FinalizerUpdate",
	fmt.Sprintf(`Updated %q finalizers`, ChannelName),
)

var DefaultEnv = &config.Env{
	DataPlaneConfigMapNamespace: "knative-eventing",
	DataPlaneConfigMapName:      "kafka-channel-channels-subscriptions",
	GeneralConfigMapName:        "kafka-broker-config",
	IngressName:                 "kafka-channel-ingress",
	SystemNamespace:             "knative-eventing",
	DataPlaneConfigFormat:       base.Json,
}

// TODO: tests with and without subscriptions
// TODO: tests for things from config-kafka
// TODO: are we setting a InitialOffsetsCommitted status?
// TODO: test if things from spec is used properly?

func TestReconcileKind(t *testing.T) {

	messagingv1beta.RegisterAlternateKafkaChannelConditionSet(base.IngressConditionSet)

	env := *DefaultEnv
	testKey := fmt.Sprintf("%s/%s", ChannelNamespace, ChannelName)

	table := TableTest{
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
				NewConfigMapWithTextData(system.Namespace(), constants.SettingsConfigMapName, configKafka),
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
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
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
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						//StatusInitialOffsetsCommitted,
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
				NewConfigMapWithTextData(system.Namespace(), constants.SettingsConfigMapName, configKafka),
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
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
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
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						//StatusInitialOffsetsCommitted,
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
				NewConfigMapWithTextData(system.Namespace(), constants.SettingsConfigMapName, configKafka),
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
							Ingress: &contract.Ingress{
								IngressType: &contract.Ingress_Path{
									Path: receiver.Path(ChannelNamespace, ChannelName),
								},
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
				NewConfigMapWithBinaryData(&env, nil),
			},
			WantStatusUpdates: []clientgotesting.UpdateActionImpl{
				{
					Object: NewChannel(
						WithInitKafkaChannelConditions,
						StatusConfigParsed,
						StatusConfigMapUpdatedReady(&env),
						StatusTopicReadyWithName(ChannelTopic()),
						StatusDataPlaneAvailable,
						//StatusInitialOffsetsCommitted,
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
	}

	useTable(t, table, env)
}

func useTable(t *testing.T, table TableTest, env config.Env) {
	table.Test(t, NewFactory(&env, func(ctx context.Context, listers *Listers, env *config.Env, row *TableRow) controller.Reconciler {

		proberMock := probertesting.MockProber(prober.StatusReady)
		if p, ok := row.OtherTestData[testProber]; ok {
			proberMock = p.(prober.Prober)
		}

		reconciler := &Reconciler{
			Reconciler: &base.Reconciler{
				KubeClient:                  kubeclient.Get(ctx),
				PodLister:                   listers.GetPodLister(),
				SecretLister:                listers.GetSecretLister(),
				DataPlaneConfigMapNamespace: env.DataPlaneConfigMapNamespace,
				DataPlaneConfigMapName:      env.DataPlaneConfigMapName,
				DataPlaneConfigFormat:       env.DataPlaneConfigFormat,
				SystemNamespace:             env.SystemNamespace,
				DispatcherLabel:             base.ChannelDispatcherLabel,
				ReceiverLabel:               base.ChannelReceiverLabel,
			},
			Env:             env,
			ConfigMapLister: listers.GetConfigMapLister(),
			InitOffsetsFunc: func(ctx context.Context, kafkaClient sarama.Client, kafkaAdminClient sarama.ClusterAdmin, topics []string, consumerGroup string) (int32, error) {
				return 1, nil
			},
			NewKafkaClient: func(addrs []string, config *sarama.Config) (sarama.Client, error) {
				return &kafkatesting.MockKafkaClient{}, nil
			},
			NewKafkaClusterAdminClient: func(_ []string, _ *sarama.Config) (sarama.ClusterAdmin, error) {
				return &kafkatesting.MockKafkaClusterAdmin{
					ExpectedTopicName: ChannelTopic(),
					ExpectedTopicDetail: sarama.TopicDetail{
						NumPartitions:     1,
						ReplicationFactor: 1,
					},
					T: t,
				}, nil
			},
			Prober:      proberMock,
			IngressHost: network.GetServiceHostname(env.IngressName, env.SystemNamespace),
		}

		reconciler.ConfigMapTracker = &FakeTracker{}
		reconciler.SecretTracker = &FakeTracker{}

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

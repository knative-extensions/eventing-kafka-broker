/*
 * Copyright 2024 The Knative Authors
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

package features

import (
	"context"
	"fmt"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler/keda"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	brokerconfigmap "knative.dev/eventing-kafka-broker/test/rekt/resources/configmap/broker"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkaauthsecret"

	"knative.dev/eventing/test/rekt/resources/trigger"

	"github.com/cloudevents/sdk-go/v2/test"
	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	"knative.dev/eventing/test/rekt/resources/broker"
	subscriptionresources "knative.dev/eventing/test/rekt/resources/subscription"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/reconciler-test/pkg/environment"
	"knative.dev/reconciler-test/pkg/eventshub"
	"knative.dev/reconciler-test/pkg/eventshub/assert"
	"knative.dev/reconciler-test/pkg/feature"
	"knative.dev/reconciler-test/pkg/manifest"
	"knative.dev/reconciler-test/pkg/resources/service"

	consumergroupclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	sourcesclient "knative.dev/eventing-kafka-broker/control-plane/pkg/client/injection/client"
	testingpkg "knative.dev/eventing-kafka-broker/test/pkg"
	testpkg "knative.dev/eventing-kafka-broker/test/pkg"
	"knative.dev/eventing-kafka-broker/test/rekt/features/kafkafeatureflags"
	kafkachannelresources "knative.dev/eventing-kafka-broker/test/rekt/resources/kafkachannel"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasink"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkasource"
	"knative.dev/eventing-kafka-broker/test/rekt/resources/kafkatopic"
	kedaclient "knative.dev/eventing-kafka-broker/third_party/pkg/client/injection/client"
)

func KafkaSourceScaledObjectHasNoEmptyAuthRef() *feature.Feature {
	f := feature.NewFeature()

	// we need to ensure that autoscaling is enabled for the rest of the feature to work
	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	kafkaSource := feature.MakeRandomK8sName("kafka-source")
	topic := feature.MakeRandomK8sName("topic")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	receiver := feature.MakeRandomK8sName("eventshub-receiver")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install kafkasink", kafkasink.Install(kafkaSink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSink))

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsDestinationRef(receiver)),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSource, kafkaSourceOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	// after the event is sent, the source should scale down to zero replicas
	f.Alpha("kafka source consumergroup scaled object").MustNot("have an authentication ref set on the trigger", verifyScaledObjectTriggerRef(getKafkaSourceCg(kafkaSource)))

	return f
}

func KafkaSourceScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	// we need to ensure that autoscaling is enabled for the rest of the feature to work
	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	kafkaSource := feature.MakeRandomK8sName("kafka-source")
	topic := feature.MakeRandomK8sName("topic")
	kafkaSink := feature.MakeRandomK8sName("kafkaSink")
	receiver := feature.MakeRandomK8sName("eventshub-receiver")
	sender := feature.MakeRandomK8sName("eventshub-sender")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	f.Setup("install kafka topic", kafkatopic.Install(topic))
	f.Setup("topic is ready", kafkatopic.IsReady(topic))

	// Binary content mode is default for Kafka Sink.
	f.Setup("install kafkasink", kafkasink.Install(kafkaSink, topic, testpkg.BootstrapServersPlaintextArr))
	f.Setup("kafkasink is ready", kafkasink.IsReady(kafkaSink))

	f.Setup("install eventshub receiver", eventshub.Install(receiver, eventshub.StartReceiver))

	kafkaSourceOpts := []manifest.CfgFn{
		kafkasource.WithSink(service.AsDestinationRef(receiver)),
		kafkasource.WithTopics([]string{topic}),
		kafkasource.WithBootstrapServers(testingpkg.BootstrapServersPlaintextArr),
	}

	f.Setup("install kafka source", kafkasource.Install(kafkaSource, kafkaSourceOpts...))
	f.Setup("kafka source is ready", kafkasource.IsReady(kafkaSource))

	// check that the source initially has replicas = 0
	f.Setup("Source should start with replicas = 0", verifyConsumerGroupReplicas(getKafkaSourceCg(kafkaSource), 0, true))

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), kafkaSink),
		eventshub.InputEvent(event),
	}
	f.Requirement("install eventshub sender", eventshub.Install(sender, options...))

	f.Requirement("eventshub receiver gets event", assert.OnStore(receiver).MatchEvent(test.HasId(event.ID())).Exact(1))

	// after the event is sent, the source should scale down to zero replicas
	f.Alpha("KafkaSource").Must("Scale down to zero", verifyConsumerGroupReplicas(getKafkaSourceCg(kafkaSource), 0, false))

	return f
}

func KafkaSourceSASLScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	// we need to ensure that autoscaling is enabled for the rest of the feature to work
	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	sourceCfg := kafkaSourceConfig{
		sourceName: feature.MakeRandomK8sName("kafka-source"),
		authMech:   SASLMech,
		topic:      feature.MakeRandomK8sName("kafka-source-keda-sasl"),
	}
	sinkCfg := kafkaSinkConfig{
		sinkName: feature.MakeRandomK8sName("kafka-sink"),
	}
	sinkName, receiver := KafkaSourceFeatureSetup(f, sourceCfg, sinkCfg)

	sender := feature.MakeRandomK8sName("eventshub-sender")

	event := cetest.FullEvent()
	event.SetID(uuid.New().String())

	// check that the source initially has replicas = 0
	f.Setup("Source should start with replicas = 0", verifyConsumerGroupReplicas(getKafkaSourceCg(sourceCfg.sourceName), 0, true))

	options := []eventshub.EventsHubOption{
		eventshub.StartSenderToResource(kafkasink.GVR(), sinkName),
		eventshub.InputEvent(event),
	}
	f.Requirement("install eventshub sender", eventshub.Install(sender, options...))

	f.Requirement("eventshub receiver gets event", assert.OnStore(receiver).MatchEvent(test.HasId(event.ID())).Exact(1))

	// after the event is sent, the source should scale down to zero replicas
	f.Alpha("KafkaSource").Must("Scale down to zero", verifyConsumerGroupReplicas(getKafkaSourceCg(sourceCfg.sourceName), 0, false))

	return f
}

func TriggerScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	event := cetest.FullEvent()

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")

	// check that the trigger initially has replicas = 0
	f.Setup("Trigger should start with replicas = 0", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, true))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))
	f.Setup("install broker", broker.Install(brokerName))
	f.Setup("install trigger", trigger.Install(triggerName, trigger.WithBrokerName(brokerName), trigger.WithSubscriber(service.AsKReference(sinkName), "")))

	f.Requirement("install source", eventshub.Install(sourceName, eventshub.StartSenderToResource(broker.GVR(), brokerName), eventshub.InputEvent(event)))

	f.Requirement("sink receives event", assert.OnStore(sinkName).MatchEvent(test.HasId(event.ID())).Exact(1))

	//after the event is sent, the trigger should scale down to zero replicas
	f.Alpha("Trigger").Must("Scale down to zero", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, false))

	return f
}

func TriggerSASLScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	event := cetest.FullEvent()

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	brokerConfigName := feature.MakeRandomK8sName("brokercfg")
	authSecretName := feature.MakeRandomK8sName("kafkaauth")

	// check that the trigger initially has replicas = 0
	f.Setup("Trigger should start with replicas = 0", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, true))

	f.Setup("Create auth secret", func(ctx context.Context, t feature.T) {
		kafkaauthsecret.Install(authSecretName, kafkaauthsecret.WithSslSaslScram512Data(ctx))(ctx, t)
	})

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithNumPartitions(3),
		brokerconfigmap.WithReplicationFactor(3),
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersSslSaslScram),
		brokerconfigmap.WithAuthSecret(authSecretName)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))
	f.Setup("install trigger", trigger.Install(triggerName, trigger.WithBrokerName(brokerName), trigger.WithSubscriber(service.AsKReference(sinkName), "")))

	f.Requirement("install source", eventshub.Install(sourceName, eventshub.StartSenderToResource(broker.GVR(), brokerName), eventshub.InputEvent(event)))

	f.Requirement("sink receives event", assert.OnStore(sinkName).MatchEvent(test.HasId(event.ID())).Exact(1))

	//after the event is sent, the trigger should scale down to zero replicas
	f.Alpha("Trigger").Must("Scale down to zero", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, false))

	return f
}

func TriggerSSLScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	event := cetest.FullEvent()

	brokerName := feature.MakeRandomK8sName("broker")
	triggerName := feature.MakeRandomK8sName("trigger")
	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")
	brokerConfigName := feature.MakeRandomK8sName("brokercfg")
	authSecretName := feature.MakeRandomK8sName("kafkaauth")

	// check that the trigger initially has replicas = 0
	f.Setup("Trigger should start with replicas = 0", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, true))

	f.Setup("Create auth secret", func(ctx context.Context, t feature.T) {
		kafkaauthsecret.Install(authSecretName, kafkaauthsecret.WithSslData(ctx))(ctx, t)
	})

	f.Setup("Create broker config", brokerconfigmap.Install(brokerConfigName,
		brokerconfigmap.WithNumPartitions(3),
		brokerconfigmap.WithReplicationFactor(3),
		brokerconfigmap.WithBootstrapServer(testpkg.BootstrapServersSsl),
		brokerconfigmap.WithAuthSecret(authSecretName)))

	f.Setup("Install broker", broker.Install(brokerName, append(
		broker.WithEnvConfig(),
		broker.WithConfig(brokerConfigName))...,
	))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))
	f.Setup("install trigger", trigger.Install(triggerName, trigger.WithBrokerName(brokerName), trigger.WithSubscriber(service.AsKReference(sinkName), "")))

	f.Requirement("install source", eventshub.Install(sourceName, eventshub.StartSenderToResource(broker.GVR(), brokerName), eventshub.InputEvent(event)))

	f.Requirement("sink receives event", assert.OnStore(sinkName).MatchEvent(test.HasId(event.ID())).Exact(1))

	//after the event is sent, the trigger should scale down to zero replicas
	f.Alpha("Trigger").Must("Scale down to zero", verifyConsumerGroupReplicas(getTriggerCg(triggerName), 0, false))

	return f
}

func ChannelScalesToZeroWithKeda() *feature.Feature {
	f := feature.NewFeature()

	f.Prerequisite("Autoscaling is enabled", kafkafeatureflags.AutoscalingEnabled())

	event := cetest.FullEvent()

	channelName := feature.MakeRandomK8sName("channel")
	subscriptionName := feature.MakeRandomK8sName("subscription")
	sourceName := feature.MakeRandomK8sName("source")
	sinkName := feature.MakeRandomK8sName("sink")

	// check that the trigger initially has replicas = 0
	f.Setup("Subscription should start with replicas = 0", verifyConsumerGroupReplicas(getSubscriptionCg(subscriptionName), 0, true))

	f.Setup("install sink", eventshub.Install(sinkName, eventshub.StartReceiver))
	f.Setup("install channel", kafkachannelresources.Install(channelName,
		kafkachannelresources.WithNumPartitions("3"),
		kafkachannelresources.WithReplicationFactor("1"),
		kafkachannelresources.WithRetentionDuration("P1D"),
	))

	f.Setup("install subscription", subscriptionresources.Install(subscriptionName,
		subscriptionresources.WithChannel(&duckv1.KReference{
			Kind:       "KafkaChannel",
			APIVersion: "messaging.knative.dev/v1beta1",
			Name:       channelName,
		}),
		subscriptionresources.WithSubscriber(service.AsKReference(sinkName), "", ""),
	))

	f.Requirement("install source", eventshub.Install(sourceName, eventshub.StartSenderToResource(kafkachannelresources.GVR(), channelName), eventshub.InputEvent(event)))

	f.Requirement("sink receives event", assert.OnStore(sinkName).MatchEvent(test.HasId(event.ID())).Exact(1))

	//after the event is sent, the subscription should scale down to zero replicas
	f.Alpha("Subscription").Must("Scale down to zero", verifyConsumerGroupReplicas(getSubscriptionCg(subscriptionName), 0, false))

	return f
}

type getCgName func(ctx context.Context) (string, error)

func getKafkaSourceCg(source string) getCgName {
	return func(ctx context.Context) (string, error) {
		ns := environment.FromContext(ctx).Namespace()

		ks, err := sourcesclient.Get(ctx).
			SourcesV1().
			KafkaSources(ns).
			Get(ctx, source, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		return string(ks.UID), nil
	}
}

func getSubscriptionCg(subscriptionName string) getCgName {
	return func(ctx context.Context) (string, error) {
		ns := environment.FromContext(ctx).Namespace()

		sub, err := eventingclient.Get(ctx).
			MessagingV1().
			Subscriptions(ns).
			Get(ctx, subscriptionName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		return string(sub.UID), nil
	}
}

func getTriggerCg(triggerName string) getCgName {
	return func(ctx context.Context) (string, error) {
		ns := environment.FromContext(ctx).Namespace()

		trig, err := eventingclient.Get(ctx).
			EventingV1().
			Triggers(ns).
			Get(ctx, triggerName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		groupId, ok := trig.Status.Annotations[kafka.GroupIdAnnotation]
		if !ok {
			return "", fmt.Errorf("no group id annotation on the trigger")
		}

		return groupId, nil
	}
}

func verifyConsumerGroupReplicas(getConsumerGroupName getCgName, expectedReplicas int32, allowNotFound bool) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		var seenReplicas int32
		interval, timeout := environment.PollTimingsFromContext(ctx)
		err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
			ns := environment.FromContext(ctx).Namespace()

			cgName, err := getConsumerGroupName(ctx)
			if err != nil {
				if allowNotFound {
					return false, nil
				}
				t.Fatal(err)
			}

			InternalsClient := consumergroupclient.Get(ctx)
			cg, err := InternalsClient.InternalV1alpha1().
				ConsumerGroups(ns).
				Get(ctx, cgName, metav1.GetOptions{})

			if err != nil {
				if allowNotFound {
					return false, nil
				}
				t.Fatal(err)
			}

			if *cg.Spec.Replicas != expectedReplicas {
				seenReplicas = *cg.Spec.Replicas
				return false, nil
			}
			return true, nil
		})

		if err != nil {
			t.Errorf("failed to verify consumergroup replicas. Expected %d, final value was %d", expectedReplicas, seenReplicas)
		}
	}
}

func verifyScaledObjectTriggerRef(getConsumerGroupName getCgName) feature.StepFn {
	return func(ctx context.Context, t feature.T) {
		kedaClient := kedaclient.Get(ctx)
		internalsClient := consumergroupclient.Get(ctx)
		ns := environment.FromContext(ctx).Namespace()

		cgName, err := getConsumerGroupName(ctx)
		if err != nil {
			t.Fatal(err)
		}

		cg, err := internalsClient.InternalV1alpha1().ConsumerGroups(ns).Get(ctx, cgName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		so, err := kedaClient.KedaV1alpha1().ScaledObjects(ns).Get(ctx, keda.GenerateScaledObjectName(cg), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}

		if so.Spec.Triggers != nil {
			for _, trig := range so.Spec.Triggers {
				if trig.AuthenticationRef != nil {
					t.Fatal("trigger on scaled object should have no authentication ref but there is an authentication ref")
				}
			}
		}

	}
}

/*
 * Copyright 2022 The Knative Authors
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

package config

import (
	"context"
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/feature"
	cm "knative.dev/pkg/configmap/testing"
	_ "knative.dev/pkg/system/testing"
)

func TestFlags_IsDispatcherRateLimiterEnabled(t *testing.T) {
	nc := DefaultFeaturesConfig()
	require.False(t, nc.features.DispatcherRateLimiter == feature.Enabled)
	require.False(t, nc.features.DispatcherOrderedExecutorMetrics == feature.Enabled)
	require.False(t, nc.features.ControllerAutoscaler == feature.Enabled)
}

func TestFlags_IsEnabled_ContainingFlag(t *testing.T) {
	nc := DefaultFeaturesConfig()
	nc.Reset(&KafkaFeatureFlags{
		features: features{
			DispatcherRateLimiter:            feature.Enabled,
			DispatcherOrderedExecutorMetrics: feature.Enabled,
			ControllerAutoscaler:             feature.Enabled,
		},
	})
	require.True(t, nc.features.DispatcherRateLimiter == feature.Enabled)
	require.True(t, nc.features.DispatcherOrderedExecutorMetrics == feature.Enabled)
	require.True(t, nc.features.ControllerAutoscaler == feature.Enabled)
}

func TestGetFlags(t *testing.T) {
	_, example := cm.ConfigMapsFromTestFile(t, FlagsConfigName)
	flags, err := newFeaturesConfigFromMap(example)
	require.NoError(t, err)

	require.True(t, flags.IsDispatcherRateLimiterEnabled())
	require.True(t, flags.IsDispatcherOrderedExecutorMetricsEnabled())
	require.True(t, flags.IsControllerAutoscalerEnabled())
	require.True(t, flags.IsControllerAutoscalerEnabled())
	require.Len(t, flags.features.TriggersConsumerGroupTemplate.Tree.Root.Nodes, 4)
	require.Equal(t, flags.features.TriggersConsumerGroupTemplate.Name(), "triggers.consumergroup.template")
	require.Len(t, flags.features.BrokersTopicTemplate.Tree.Root.Nodes, 4)
	require.Equal(t, flags.features.BrokersTopicTemplate.Name(), "brokers.topic.template")
	require.Len(t, flags.features.ChannelsTopicTemplate.Tree.Root.Nodes, 4)
	require.Equal(t, flags.features.ChannelsTopicTemplate.Name(), "channels.topic.template")

}

func TestStoreLoadWithConfigMap(t *testing.T) {
	store := NewStore(context.Background())

	_, exampleConfig := cm.ConfigMapsFromTestFile(t, FlagsConfigName)
	store.OnConfigChanged(exampleConfig)

	have := FromContext(store.ToContext(context.Background()))
	expected, _ := newFeaturesConfigFromMap(exampleConfig)

	require.Equal(t, expected.IsDispatcherRateLimiterEnabled(), have.IsDispatcherRateLimiterEnabled())
	require.Equal(t, expected.IsDispatcherOrderedExecutorMetricsEnabled(), have.IsDispatcherOrderedExecutorMetricsEnabled())
	require.Equal(t, expected.IsControllerAutoscalerEnabled(), have.IsControllerAutoscalerEnabled())
	require.Equal(t, expected.features.TriggersConsumerGroupTemplate.Name(), have.features.TriggersConsumerGroupTemplate.Name())
	require.Equal(t, expected.features.BrokersTopicTemplate.Name(), have.features.BrokersTopicTemplate.Name())
	require.Equal(t, expected.features.ChannelsTopicTemplate.Name(), have.features.ChannelsTopicTemplate.Name())
}

func TestStoreLoadWithContext(t *testing.T) {
	have := FromContext(context.Background())

	require.False(t, have.IsDispatcherRateLimiterEnabled())
	require.False(t, have.IsDispatcherOrderedExecutorMetricsEnabled())
	require.False(t, have.IsControllerAutoscalerEnabled())
	require.Equal(t, have.features.TriggersConsumerGroupTemplate.Name(), "triggers.consumergroup.template")
	require.Equal(t, have.features.BrokersTopicTemplate.Name(), "brokers.topic.template")
	require.Equal(t, have.features.ChannelsTopicTemplate.Name(), "channels.topic.template")
}

func TestExecuteTriggersConsumerGroupTemplateDefault(t *testing.T) {
	nc := DefaultFeaturesConfig()
	result, err := nc.ExecuteTriggersConsumerGroupTemplate(v1.ObjectMeta{
		Name:      "trigger",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-trigger-namespace-trigger")
}

func TestExecuteTriggersConsumerGroupTemplateOverride(t *testing.T) {
	nc := DefaultFeaturesConfig()
	nc.features.TriggersConsumerGroupTemplate, _ = template.New("custom-template").Parse("knative-trigger-{{ .Namespace }}-{{ .Name }}-{{ .UID }}")

	result, err := nc.ExecuteTriggersConsumerGroupTemplate(v1.ObjectMeta{
		Name:      "trigger",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-trigger-namespace-trigger-138ac0ec-2694-4747-900d-45be3da5c9a9")
}

func TestExecuteBrokersTopicTemplateDefault(t *testing.T) {
	nc := DefaultFeaturesConfig()
	result, err := nc.ExecuteBrokersTopicTemplate(v1.ObjectMeta{
		Name:      "topic",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-broker-namespace-topic")
}

func TestExecuteBrokersTopicTemplateOverride(t *testing.T) {
	nc := DefaultFeaturesConfig()
	nc.features.BrokersTopicTemplate, _ = template.New("custom-template").Parse("knative-broker-{{ .Namespace }}-{{ .Name }}-{{ .UID }}")

	result, err := nc.ExecuteBrokersTopicTemplate(v1.ObjectMeta{
		Name:      "topic",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-broker-namespace-topic-138ac0ec-2694-4747-900d-45be3da5c9a9")
}

func TestExecuteChannelsTopicTemplateDefault(t *testing.T) {
	nc := DefaultFeaturesConfig()
	result, err := nc.ExecuteChannelsTopicTemplate(v1.ObjectMeta{
		Name:      "topic",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-channel-namespace-topic")
}

func TestExecuteChannelsTopicTemplateOverride(t *testing.T) {
	nc := DefaultFeaturesConfig()
	nc.features.ChannelsTopicTemplate, _ = template.New("custom-template").Parse("knative-channel-{{ .Namespace }}-{{ .Name }}-{{ .UID }}")

	result, err := nc.ExecuteChannelsTopicTemplate(v1.ObjectMeta{
		Name:      "topic",
		Namespace: "namespace",
		UID:       "138ac0ec-2694-4747-900d-45be3da5c9a9",
	})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, result, "knative-channel-namespace-topic-138ac0ec-2694-4747-900d-45be3da5c9a9")
}

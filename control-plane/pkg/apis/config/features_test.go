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

	"github.com/stretchr/testify/require"
	"knative.dev/eventing/pkg/apis/feature"
	cm "knative.dev/pkg/configmap/testing"
	_ "knative.dev/pkg/system/testing"
)

func TestFlags_IsDispatcherRateLimiterEnabled(t *testing.T) {
	nc := DefaultFeaturesConfig()
	require.False(t, nc.features.DispatcherRateLimiter == feature.Enabled)
}

func TestFlags_IsEnabled_ContainingFlag(t *testing.T) {
	nc := DefaultFeaturesConfig()
	nc.Reset(&KafkaFeatureFlags{
		features: features{
			DispatcherRateLimiter: feature.Enabled,
		},
	})
	require.True(t, nc.features.DispatcherRateLimiter == feature.Enabled)
}

func TestGetFlags(t *testing.T) {
	_, example := cm.ConfigMapsFromTestFile(t, FlagsConfigName)
	flags, err := newFeaturesConfigFromMap(example)
	require.NoError(t, err)

	require.True(t, flags.IsDispatcherRateLimiterEnabled())

}

func TestStoreLoadWithConfigMap(t *testing.T) {
	store := NewStore(context.Background())

	_, exampleConfig := cm.ConfigMapsFromTestFile(t, FlagsConfigName)
	store.OnConfigChanged(exampleConfig)

	have := FromContext(store.ToContext(context.Background()))
	expected, _ := newFeaturesConfigFromMap(exampleConfig)

	require.Equal(t, expected.IsDispatcherRateLimiterEnabled(), have.IsDispatcherRateLimiterEnabled())
}

func TestStoreLoadWithContext(t *testing.T) {
	have := FromContext(context.Background())

	require.False(t, have.IsDispatcherRateLimiterEnabled())
}

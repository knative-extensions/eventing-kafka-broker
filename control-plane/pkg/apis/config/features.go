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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/feature"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/logging"
)

const (
	FlagsConfigName = "config-kafka-features"
)

type features struct {
	DispatcherRateLimiter            feature.Flag
	DispatcherOrderedExecutorMetrics feature.Flag
	ControllerAutoscaler             feature.Flag
	TriggersConsumerGroupTemplate    *template.Template
}

type KafkaFeatureFlags struct {
	features features
	m        sync.RWMutex
}

var DefaultTriggersConsumerGroupTemplate *template.Template

func init() {
	DefaultTriggersConsumerGroupTemplate, _ = template.New("triggers.consumergroup.template").Parse("knative-trigger-{{ .Namespace }}-{{ .Name }}")
}

func DefaultFeaturesConfig() *KafkaFeatureFlags {
	return &KafkaFeatureFlags{
		features: features{
			DispatcherRateLimiter:            feature.Disabled,
			DispatcherOrderedExecutorMetrics: feature.Disabled,
			ControllerAutoscaler:             feature.Disabled,
			TriggersConsumerGroupTemplate:    DefaultTriggersConsumerGroupTemplate,
		},
	}
}

// newFeaturesConfigFromMap creates a Features from the supplied Map
func newFeaturesConfigFromMap(cm *corev1.ConfigMap) (*KafkaFeatureFlags, error) {
	nc := DefaultFeaturesConfig()
	err := configmap.Parse(cm.Data,
		asFlag("dispatcher.rate-limiter", &nc.features.DispatcherRateLimiter),
		asFlag("dispatcher.ordered-executor-metrics", &nc.features.DispatcherOrderedExecutorMetrics),
		asFlag("controller.autoscaler", &nc.features.ControllerAutoscaler),
		asTemplate("triggers.consumergroup.template", nc.features.TriggersConsumerGroupTemplate),
	)
	return nc, err
}

func (f *KafkaFeatureFlags) Reset(ff *KafkaFeatureFlags) {
	f.m.Lock()
	defer f.m.Unlock()
	f.features = ff.features
}

func (f *KafkaFeatureFlags) IsDispatcherRateLimiterEnabled() bool {
	return f.features.DispatcherRateLimiter == feature.Enabled
}

func (f *KafkaFeatureFlags) IsDispatcherOrderedExecutorMetricsEnabled() bool {
	return f.features.DispatcherOrderedExecutorMetrics == feature.Enabled
}

func (f *KafkaFeatureFlags) IsControllerAutoscalerEnabled() bool {
	return f.features.ControllerAutoscaler == feature.Enabled
}

func (f *KafkaFeatureFlags) ExecuteTriggersConsumerGroupTemplate(triggerMetadata v1.ObjectMeta) (string, error) {
	var result bytes.Buffer
	err := f.features.TriggersConsumerGroupTemplate.Execute(&result, triggerMetadata)
	if err != nil {
		return "", fmt.Errorf("unable to execute triggers consumergroup template: %w", err)
	}

	return result.String(), nil
}

// Store is a typed wrapper around configmap.Untyped store to handle our configmaps.
// +k8s:deepcopy-gen=false
type Store struct {
	*configmap.UntypedStore
}

// NewStore creates a new store of Configs and optionally calls functions when ConfigMaps are updated.
func NewStore(ctx context.Context, onAfterStore ...func(name string, value *KafkaFeatureFlags)) *Store {
	store := &Store{
		UntypedStore: configmap.NewUntypedStore(
			"config-kafka-features",
			logging.FromContext(ctx).Named("config-kafka-features"),
			configmap.Constructors{
				FlagsConfigName: newFeaturesConfigFromMap,
			},
			func(name string, value interface{}) {
				for _, f := range onAfterStore {
					f(name, value.(*KafkaFeatureFlags))
				}
			},
		),
	}

	return store
}

// ToContext attaches the current Config state to the provided context.
func (s *Store) ToContext(ctx context.Context) context.Context {
	return ToContext(ctx, s.Load())
}

type cfgKey struct{}

func ToContext(ctx context.Context, features *KafkaFeatureFlags) context.Context {
	return context.WithValue(ctx, cfgKey{}, features)
}

func FromContext(ctx context.Context) *KafkaFeatureFlags {
	if v := ctx.Value(cfgKey{}); v != nil {
		return v.(*KafkaFeatureFlags)
	}
	return DefaultFeaturesConfig()
}

// Load creates a Config from the current config state of the Store.
func (s *Store) Load() *KafkaFeatureFlags {
	loaded := s.UntypedLoad(FlagsConfigName)
	if loaded == nil {
		return DefaultFeaturesConfig()
	}
	return loaded.(*KafkaFeatureFlags)
}

// asFlag parses the value at key as a Flag into the target, if it exists.
func asFlag(key string, target *feature.Flag) configmap.ParseFunc {
	return func(data map[string]string) error {
		if raw, ok := data[key]; ok {
			for _, flag := range []feature.Flag{feature.Enabled, feature.Allowed, feature.Disabled} {
				if strings.EqualFold(raw, string(flag)) {
					*target = flag
					return nil
				}
			}
		}
		return nil
	}
}

// asTemplate parses the value at key as a go text template into the target, if it exists.
func asTemplate(key string, target *template.Template) configmap.ParseFunc {
	return func(data map[string]string) error {
		if raw, ok := data[key]; ok {
			tmlp, err := template.New(key).Parse(raw)
			if err != nil {
				return err
			}

			*target = *tmlp
		}
		return nil
	}
}

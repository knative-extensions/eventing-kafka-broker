/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
)

const (
	// KafkaDefaultsConfigName is the name of config map for the default
	// configs that KafkaSource should use.
	KafkaDefaultsConfigName = "config-kafka-source-defaults"

	// DefaultAutoscalingClassKey is the name of autoscaler class annotation
	DefaultAutoscalingClassKey = "autoscalingClass"

	// DefaultMinScaleKey is the name of the key corresponding to the KEDA minScale annotation
	DefaultMinScaleKey = "minScale"

	// DefaultMaxScaleKey is the name of the KEDA maxScale annotation
	DefaultMaxScaleKey = "maxScale"

	// DefaultPollingIntervalKey is the name of the KEDA pollingInterval annotation
	DefaultPollingIntervalKey = "pollingInterval"

	// DefaultCooldownPeriodKey is the name of the KEDA cooldownPeriod annotation
	DefaultCooldownPeriodKey = "cooldownPeriod"

	// DefaultKafkaLagThresholdKey is the name of the KEDA kafkaLagThreshold annotation
	DefaultKafkaLagThresholdKey = "kafkaLagThreshold"

	// KedaAutoscalingClass is the class name for KEDA
	KedaAutoscalingClass = "keda.autoscaling.knative.dev"

	// DefaultMinScaleValue is the default value for DefaultMinScaleKey
	DefaultMinScaleValue = int64(1)

	// DefaultMaxScaleValue is  the default value for DefaultMaxScaleKey
	DefaultMaxScaleValue = int64(1)

	// DefaultPollingIntervalValue is the default value for DefaultPollingIntervalKey
	DefaultPollingIntervalValue = int64(30)

	// DefaultCooldownPeriodValue is the default value for DefaultCooldownPeriodKey
	DefaultCooldownPeriodValue = int64(300)

	// DefaultKafkaLagThresholdValue is the default value for DefaultKafkaLagThresholdKey
	DefaultKafkaLagThresholdValue = int64(10)
)

// NewKafkaDefaultsConfigFromMap creates a KafkaSourceDefaults from the supplied Map
func NewKafkaDefaultsConfigFromMap(data map[string]string) (*KafkaSourceDefaults, error) {
	nc := &KafkaSourceDefaults{}

	value, present := data[DefaultAutoscalingClassKey]
	if !present || value == "" {
		return nc, nil
	}
	if value != "keda.autoscaling.knative.dev" {
		return nil, fmt.Errorf("invalid value %q for %s. Only keda.autoscaling.knative.dev is allowed", value, DefaultAutoscalingClassKey)
	}
	nc.AutoscalingClass = value

	int64Value, err := parseInt64Entry(data, DefaultMinScaleKey, DefaultMinScaleValue)
	if err != nil {
		return nil, err
	}
	nc.MinScale = int64Value

	int64Value, err = parseInt64Entry(data, DefaultMaxScaleKey, DefaultMaxScaleValue)
	if err != nil {
		return nil, err
	}
	nc.MaxScale = int64Value

	int64Value, err = parseInt64Entry(data, DefaultPollingIntervalKey, DefaultPollingIntervalValue)
	if err != nil {
		return nil, err
	}
	nc.PollingInterval = int64Value

	int64Value, err = parseInt64Entry(data, DefaultCooldownPeriodKey, DefaultCooldownPeriodValue)
	if err != nil {
		return nil, err
	}
	nc.CooldownPeriod = int64Value

	int64Value, err = parseInt64Entry(data, DefaultKafkaLagThresholdKey, DefaultKafkaLagThresholdValue)
	if err != nil {
		return nil, err
	}
	nc.KafkaLagThreshold = int64Value

	return nc, nil
}

// NewKafkaDefaultsConfigFromConfigMap creates a KafkaSourceDefaults from the supplied configMap
func NewKafkaDefaultsConfigFromConfigMap(config *corev1.ConfigMap) (*KafkaSourceDefaults, error) {
	return NewKafkaDefaultsConfigFromMap(config.Data)
}

type KafkaSourceDefaults struct {
	AutoscalingClass  string `json:"autoscalingClass,omitempty"`
	MinScale          int64  `json:"minScale,omitempty"`
	MaxScale          int64  `json:"maxScale,omitempty"`
	PollingInterval   int64  `json:"pollingInterval,omitempty"`
	CooldownPeriod    int64  `json:"cooldownPeriod,omitempty"`
	KafkaLagThreshold int64  `json:"kafkaLagThreshold,omitempty"`
}

func (d *KafkaSourceDefaults) DeepCopy() *KafkaSourceDefaults {
	if d == nil {
		return nil
	}
	out := new(KafkaSourceDefaults)
	*out = *d
	return out
}

func parseInt64Entry(data map[string]string, key string, defaults int64) (int64, error) {
	value, present := data[key]
	if !present {
		return defaults, nil
	}
	return strconv.ParseInt(value, 0, 64)
}

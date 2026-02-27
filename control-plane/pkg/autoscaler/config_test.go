/*
 * Copyright 2025 The Knative Authors
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

package autoscaler

import (
	"reflect"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	config := defaultConfig()

	// Test AutoscalerClass defaults
	expectedClass := map[string]string{
		AutoscalingClassAnnotation: AutoscalingClassDisabledAnnotationValue,
	}
	if !reflect.DeepEqual(config.AutoscalerClass, expectedClass) {
		t.Errorf("Expected AutoscalerClass %v, got %v", expectedClass, config.AutoscalerClass)
	}

	// Test AutoscalerDefaults
	expectedDefaults := map[string]int32{
		AutoscalingMinScaleAnnotation:        DefaultMinReplicaCount,
		AutoscalingMaxScaleAnnotation:        DefaultMaxReplicaCount,
		AutoscalingPollingIntervalAnnotation: DefaultPollingInterval,
		AutoscalingCooldownPeriodAnnotation:  DefaultCooldownPeriod,
		AutoscalingLagThreshold:              DefaultLagThreshold,
	}
	if !reflect.DeepEqual(config.AutoscalerDefaults, expectedDefaults) {
		t.Errorf("Expected AutoscalerDefaults %v, got %v", expectedDefaults, config.AutoscalerDefaults)
	}

	// Test AutoscalerAuthentication defaults
	expectedAuth := map[string]string{
		AutoscalingAuthenticationName: DefaultAuthenticationName,
		AutoscalingAuthenticationKind: DefaultAuthenticationKind,
		AutoscalingAwsRegion:          DefaultAwsRegion,
	}
	if !reflect.DeepEqual(config.AutoscalerAuthentication, expectedAuth) {
		t.Errorf("Expected AutoscalerAuthentication %v, got %v", expectedAuth, config.AutoscalerAuthentication)
	}
}

func TestNewConfigFromMap_EmptyMap(t *testing.T) {
	config, err := NewConfigFromMap(map[string]string{})
	if err != nil {
		t.Errorf("Expected no error for empty map, got %v", err)
	}

	// Should return default config
	expectedConfig := defaultConfig()
	if !reflect.DeepEqual(config, expectedConfig) {
		t.Errorf("Expected default config for empty map, got %v", config)
	}
}

func TestNewConfigFromMap_DisabledClass(t *testing.T) {
	data := map[string]string{
		"class":            "disabled",
		"min-scale":        "5",
		"max-scale":        "100",
		"polling-interval": "20",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for disabled class, got %v", err)
	}

	// When class is disabled, should return default config and ignore other values
	expectedConfig := defaultConfig()
	if !reflect.DeepEqual(config, expectedConfig) {
		t.Errorf("Expected default config for disabled class, got %v", config)
	}
}

func TestNewConfigFromMap_ValidIntegerValues(t *testing.T) {
	data := map[string]string{
		"class":            "keda",
		"min-scale":        "2",
		"max-scale":        "20",
		"polling-interval": "15",
		"cooldown-period":  "45",
		"lag-threshold":    "200",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for valid config, got %v", err)
	}

	// Test class annotation
	expectedClass := "keda"
	if config.AutoscalerClass[AutoscalingClassAnnotation] != expectedClass {
		t.Errorf("Expected class %s, got %s", expectedClass, config.AutoscalerClass[AutoscalingClassAnnotation])
	}

	// Test integer values
	expectedValues := map[string]int32{
		AutoscalingMinScaleAnnotation:        2,
		AutoscalingMaxScaleAnnotation:        20,
		AutoscalingPollingIntervalAnnotation: 15,
		AutoscalingCooldownPeriodAnnotation:  45,
		AutoscalingLagThreshold:              200,
	}

	for key, expected := range expectedValues {
		if config.AutoscalerDefaults[key] != expected {
			t.Errorf("Expected %s = %d, got %d", key, expected, config.AutoscalerDefaults[key])
		}
	}
}

func TestNewConfigFromMap_AuthenticationValues(t *testing.T) {
	data := map[string]string{
		"authentication-name": "my-auth",
		"authentication-kind": "TriggerAuthentication",
		"aws-region":          "us-west-2",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for authentication config, got %v", err)
	}

	expectedAuth := map[string]string{
		AutoscalingAuthenticationName: "my-auth",
		AutoscalingAuthenticationKind: "TriggerAuthentication",
		AutoscalingAwsRegion:          "us-west-2",
	}

	for key, expected := range expectedAuth {
		if config.AutoscalerAuthentication[key] != expected {
			t.Errorf("Expected %s = %s, got %s", key, expected, config.AutoscalerAuthentication[key])
		}
	}
}

func TestNewConfigFromMap_MixedValues(t *testing.T) {
	data := map[string]string{
		"class":               "keda",
		"min-scale":           "1",
		"max-scale":           "10",
		"authentication-name": "kafka-auth",
		"authentication-kind": "ClusterTriggerAuthentication",
		"aws-region":          "eu-central-1",
		"polling-interval":    "5",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for mixed config, got %v", err)
	}

	// Verify class
	if config.AutoscalerClass[AutoscalingClassAnnotation] != "keda" {
		t.Errorf("Expected class keda, got %s", config.AutoscalerClass[AutoscalingClassAnnotation])
	}

	// Verify integer defaults
	if config.AutoscalerDefaults[AutoscalingMinScaleAnnotation] != 1 {
		t.Errorf("Expected min-scale 1, got %d", config.AutoscalerDefaults[AutoscalingMinScaleAnnotation])
	}
	if config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation] != 10 {
		t.Errorf("Expected max-scale 10, got %d", config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation])
	}
	if config.AutoscalerDefaults[AutoscalingPollingIntervalAnnotation] != 5 {
		t.Errorf("Expected polling-interval 5, got %d", config.AutoscalerDefaults[AutoscalingPollingIntervalAnnotation])
	}

	// Verify authentication
	if config.AutoscalerAuthentication[AutoscalingAuthenticationName] != "kafka-auth" {
		t.Errorf("Expected authentication-name kafka-auth, got %s", config.AutoscalerAuthentication[AutoscalingAuthenticationName])
	}
	if config.AutoscalerAuthentication[AutoscalingAuthenticationKind] != "ClusterTriggerAuthentication" {
		t.Errorf("Expected authentication-kind ClusterTriggerAuthentication, got %s", config.AutoscalerAuthentication[AutoscalingAuthenticationKind])
	}
	if config.AutoscalerAuthentication[AutoscalingAwsRegion] != "eu-central-1" {
		t.Errorf("Expected aws-region eu-central-1, got %s", config.AutoscalerAuthentication[AutoscalingAwsRegion])
	}
}

func TestNewConfigFromMap_InvalidIntegerValue(t *testing.T) {
	data := map[string]string{
		"min-scale": "invalid",
	}

	_, err := NewConfigFromMap(data)
	if err == nil {
		t.Error("Expected error for invalid integer value, got nil")
	}

	// Error should not be empty
	if err != nil && err.Error() == "" {
		t.Error("Error message should not be empty")
	}
}

func TestNewConfigFromMap_NegativeIntegerValue(t *testing.T) {
	data := map[string]string{
		"min-scale": "-5",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for negative integer, got %v", err)
	}

	// Should accept negative values as they are valid int32
	if config.AutoscalerDefaults[AutoscalingMinScaleAnnotation] != -5 {
		t.Errorf("Expected min-scale -5, got %d", config.AutoscalerDefaults[AutoscalingMinScaleAnnotation])
	}
}

func TestNewConfigFromMap_ZeroValues(t *testing.T) {
	data := map[string]string{
		"min-scale":        "0",
		"max-scale":        "0",
		"polling-interval": "0",
		"cooldown-period":  "0",
		"lag-threshold":    "0",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for zero values, got %v", err)
	}

	expectedValues := map[string]int32{
		AutoscalingMinScaleAnnotation:        0,
		AutoscalingMaxScaleAnnotation:        0,
		AutoscalingPollingIntervalAnnotation: 0,
		AutoscalingCooldownPeriodAnnotation:  0,
		AutoscalingLagThreshold:              0,
	}

	for key, expected := range expectedValues {
		if config.AutoscalerDefaults[key] != expected {
			t.Errorf("Expected %s = %d, got %d", key, expected, config.AutoscalerDefaults[key])
		}
	}
}

func TestNewConfigFromMap_LargeIntegerValues(t *testing.T) {
	data := map[string]string{
		"max-scale": "2147483647", // Max int32 value
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for max int32 value, got %v", err)
	}

	if config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation] != 2147483647 {
		t.Errorf("Expected max-scale 2147483647, got %d", config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation])
	}
}

func TestNewConfigFromMap_IntegerOverflow(t *testing.T) {
	data := map[string]string{
		"max-scale": "9223372036854775808", // Greater than max int64
	}

	_, err := NewConfigFromMap(data)
	if err == nil {
		t.Error("Expected error for integer overflow, got nil")
	}
}

func TestNewConfigFromMap_UnknownKey(t *testing.T) {
	data := map[string]string{
		"unknown-key": "123",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for unknown key, got %v", err)
	}

	// Unknown keys should be treated as integer defaults
	unknownAnnotation := convertConfigKeyToAutoscalingAnnotation("unknown-key")
	if config.AutoscalerDefaults[unknownAnnotation] != 123 {
		t.Errorf("Expected unknown-key to be stored as integer 123, got %d", config.AutoscalerDefaults[unknownAnnotation])
	}
}

func TestNewConfigFromMap_EmptyStringValues(t *testing.T) {
	data := map[string]string{
		"authentication-name": "",
		"authentication-kind": "",
		"aws-region":          "",
		"class":               "",
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error for empty string values, got %v", err)
	}

	// Empty strings should be accepted for authentication fields and class
	if config.AutoscalerAuthentication[AutoscalingAuthenticationName] != "" {
		t.Errorf("Expected empty authentication-name, got %s", config.AutoscalerAuthentication[AutoscalingAuthenticationName])
	}
	if config.AutoscalerAuthentication[AutoscalingAuthenticationKind] != "" {
		t.Errorf("Expected empty authentication-kind, got %s", config.AutoscalerAuthentication[AutoscalingAuthenticationKind])
	}
	if config.AutoscalerAuthentication[AutoscalingAwsRegion] != "" {
		t.Errorf("Expected empty aws-region, got %s", config.AutoscalerAuthentication[AutoscalingAwsRegion])
	}
	if config.AutoscalerClass[AutoscalingClassAnnotation] != "" {
		t.Errorf("Expected empty class, got %s", config.AutoscalerClass[AutoscalingClassAnnotation])
	}
}

func TestConvertConfigKeyToAutoscalingAnnotation(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"class", "autoscaling.eventing.knative.dev/class"},
		{"min-scale", "autoscaling.eventing.knative.dev/min-scale"},
		{"max-scale", "autoscaling.eventing.knative.dev/max-scale"},
		{"polling-interval", "autoscaling.eventing.knative.dev/polling-interval"},
		{"cooldown-period", "autoscaling.eventing.knative.dev/cooldown-period"},
		{"lag-threshold", "autoscaling.eventing.knative.dev/lag-threshold"},
		{"authentication-name", "autoscaling.eventing.knative.dev/authentication-name"},
		{"authentication-kind", "autoscaling.eventing.knative.dev/authentication-kind"},
		{"aws-region", "autoscaling.eventing.knative.dev/aws-region"},
		{"custom-key", "autoscaling.eventing.knative.dev/custom-key"},
		{"", "autoscaling.eventing.knative.dev/"},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			result := convertConfigKeyToAutoscalingAnnotation(test.input)
			if result != test.expected {
				t.Errorf("Expected %s, got %s", test.expected, result)
			}
		})
	}
}

func TestNewConfigFromMap_PreservesUnmodifiedDefaults(t *testing.T) {
	data := map[string]string{
		"min-scale": "5",
		// Only set min-scale, others should remain default
	}

	config, err := NewConfigFromMap(data)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Modified value
	if config.AutoscalerDefaults[AutoscalingMinScaleAnnotation] != 5 {
		t.Errorf("Expected min-scale 5, got %d", config.AutoscalerDefaults[AutoscalingMinScaleAnnotation])
	}

	// Unmodified defaults should remain
	if config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation] != DefaultMaxReplicaCount {
		t.Errorf("Expected default max-scale %d, got %d", DefaultMaxReplicaCount, config.AutoscalerDefaults[AutoscalingMaxScaleAnnotation])
	}
	if config.AutoscalerDefaults[AutoscalingPollingIntervalAnnotation] != DefaultPollingInterval {
		t.Errorf("Expected default polling-interval %d, got %d", DefaultPollingInterval, config.AutoscalerDefaults[AutoscalingPollingIntervalAnnotation])
	}
	if config.AutoscalerDefaults[AutoscalingCooldownPeriodAnnotation] != DefaultCooldownPeriod {
		t.Errorf("Expected default cooldown-period %d, got %d", DefaultCooldownPeriod, config.AutoscalerDefaults[AutoscalingCooldownPeriodAnnotation])
	}
	if config.AutoscalerDefaults[AutoscalingLagThreshold] != DefaultLagThreshold {
		t.Errorf("Expected default lag-threshold %d, got %d", DefaultLagThreshold, config.AutoscalerDefaults[AutoscalingLagThreshold])
	}
}

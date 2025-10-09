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

package keda

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingduckv1alpha1 "knative.dev/eventing/pkg/apis/duck/v1alpha1"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/autoscaler"
	kedav1alpha1 "knative.dev/eventing-kafka-broker/third_party/pkg/apis/keda/v1alpha1"
)

// Helper function to create a base ConsumerGroup with default values
func createTestConsumerGroup(name, namespace string, annotations map[string]string, topics []string) *kafkainternals.ConsumerGroup {
	if annotations == nil {
		annotations = map[string]string{}
	}
	if topics == nil {
		topics = []string{"test-topic"}
	}

	return &kafkainternals.ConsumerGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: kafkainternals.ConsumerGroupSpec{
			Template: kafkainternals.ConsumerTemplateSpec{
				Spec: kafkainternals.ConsumerSpec{
					Topics: topics,
					Configs: kafkainternals.ConsumerConfigs{
						Configs: map[string]string{
							"bootstrap.servers": "localhost:9092",
							"group.id":          "test-group",
						},
					},
				},
			},
		},
	}
}

// Helper function to create a ConsumerGroup with custom configs
func createTestConsumerGroupWithConfigs(name, namespace string, annotations map[string]string, topics []string, configs map[string]string) *kafkainternals.ConsumerGroup {
	cg := createTestConsumerGroup(name, namespace, annotations, topics)
	cg.Spec.Template.Spec.Configs.Configs = configs
	return cg
}

// Helper function to create a default autoscaler config
func createDefaultAutoscalerConfig() autoscaler.AutoscalerConfig {
	return autoscaler.AutoscalerConfig{
		AutoscalerDefaults: map[string]int32{
			autoscaler.AutoscalingLagThreshold: 100,
		},
		AutoscalerAuthentication: map[string]string{
			autoscaler.AutoscalingAuthenticationName: "",
			autoscaler.AutoscalingAuthenticationKind: "",
			autoscaler.AutoscalingAwsRegion:          "",
		},
	}
}

// Helper function to create expected trigger with default metadata
func createExpectedTrigger(topic string, metadata map[string]string, authRef *kedav1alpha1.ScaledObjectAuthRef) kedav1alpha1.ScaleTriggers {
	defaultMetadata := map[string]string{
		"bootstrapServers":   "localhost:9092",
		"consumerGroup":      "test-group",
		"topic":              topic,
		"lagThreshold":       "100",
		"allowIdleConsumers": "false",
	}

	// Merge custom metadata with defaults
	for key, value := range metadata {
		defaultMetadata[key] = value
	}

	trigger := kedav1alpha1.ScaleTriggers{
		Type:     "kafka",
		Metadata: defaultMetadata,
	}

	if authRef != nil {
		trigger.AuthenticationRef = authRef
	}

	return trigger
}

// Helper function to create multiple expected triggers for multiple topics
func createExpectedTriggersForTopics(topics []string, metadata map[string]string, authRef *kedav1alpha1.ScaledObjectAuthRef) []kedav1alpha1.ScaleTriggers {
	triggers := make([]kedav1alpha1.ScaleTriggers, len(topics))
	for i, topic := range topics {
		triggers[i] = createExpectedTrigger(topic, metadata, authRef)
	}
	return triggers
}

func TestGenerateScaleTriggers(t *testing.T) {
	tests := []struct {
		name                   string
		consumerGroup          *kafkainternals.ConsumerGroup
		triggerAuthentication  *kedav1alpha1.TriggerAuthentication
		autoscalerConfig       autoscaler.AutoscalerConfig
		expectedTriggers       []kedav1alpha1.ScaleTriggers
		expectedError          bool
		expectedErrorSubstring string
	}{
		{
			name:                  "single topic with default values",
			consumerGroup:         createTestConsumerGroup("test-cg", "test-ns", nil, nil),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers:      []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", nil, nil)},
			expectedError:         false,
		},
		{
			name: "multiple topics with default values",
			consumerGroup: createTestConsumerGroupWithConfigs("test-cg", "test-ns", nil, []string{"topic1", "topic2", "topic3"}, map[string]string{
				"bootstrap.servers": "broker1:9092,broker2:9092",
				"group.id":          "multi-topic-group",
			}),
			triggerAuthentication: nil,
			autoscalerConfig: autoscaler.AutoscalerConfig{
				AutoscalerDefaults: map[string]int32{
					autoscaler.AutoscalingLagThreshold: 50,
				},
				AutoscalerAuthentication: map[string]string{
					autoscaler.AutoscalingAuthenticationName: "",
					autoscaler.AutoscalingAuthenticationKind: "",
					autoscaler.AutoscalingAwsRegion:          "",
				},
			},
			expectedTriggers: createExpectedTriggersForTopics([]string{"topic1", "topic2", "topic3"}, map[string]string{
				"bootstrapServers": "broker1:9092,broker2:9092",
				"consumerGroup":    "multi-topic-group",
				"lagThreshold":     "50",
			}, nil),
			expectedError: false,
		},
		{
			name: "with custom lag threshold annotation",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingLagThreshold: "200",
			}, nil),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", map[string]string{
				"lagThreshold": "200",
			}, nil)},
			expectedError: false,
		},
		{
			name: "with placements (allowIdleConsumers true)",
			consumerGroup: func() *kafkainternals.ConsumerGroup {
				cg := createTestConsumerGroup("test-cg", "test-ns", nil, nil)
				cg.Status = kafkainternals.ConsumerGroupStatus{
					PlaceableStatus: eventingduckv1alpha1.PlaceableStatus{
						Placeable: eventingduckv1alpha1.Placeable{
							Placements: []eventingduckv1alpha1.Placement{
								{
									PodName:   "pod1",
									VReplicas: 1,
								},
							},
						},
					},
				}
				return cg
			}(),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", map[string]string{
				"allowIdleConsumers": "true",
			}, nil)},
			expectedError: false,
		},
		{
			name:          "with trigger authentication provided",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", nil, nil),
			triggerAuthentication: &kedav1alpha1.TriggerAuthentication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-trigger-auth",
					Namespace: "test-ns",
				},
			},
			autoscalerConfig: createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", nil, &kedav1alpha1.ScaledObjectAuthRef{
				Name: "test-trigger-auth",
			})},
			expectedError: false,
		},
		{
			name: "with authentication name annotation",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
			}, nil),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", nil, &kedav1alpha1.ScaledObjectAuthRef{
				Name: "my-auth",
			})},
			expectedError: false,
		},
		{
			name: "with authentication name and kind annotations",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-cluster-auth",
				autoscaler.AutoscalingAuthenticationKind: "ClusterTriggerAuthentication",
			}, nil),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", nil, &kedav1alpha1.ScaledObjectAuthRef{
				Name: "my-cluster-auth",
				Kind: "ClusterTriggerAuthentication",
			})},
			expectedError: false,
		},
		{
			name: "with aws region annotation",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingAwsRegion: "us-west-2",
			}, nil),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", map[string]string{
				"awsRegion": "us-west-2",
			}, nil)},
			expectedError: false,
		},
		{
			name:                  "with aws region from config default",
			consumerGroup:         createTestConsumerGroup("test-cg", "test-ns", nil, nil),
			triggerAuthentication: nil,
			autoscalerConfig: autoscaler.AutoscalerConfig{
				AutoscalerDefaults: map[string]int32{
					autoscaler.AutoscalingLagThreshold: 100,
				},
				AutoscalerAuthentication: map[string]string{
					autoscaler.AutoscalingAuthenticationName: "",
					autoscaler.AutoscalingAuthenticationKind: "",
					autoscaler.AutoscalingAwsRegion:          "eu-central-1",
				},
			},
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", map[string]string{
				"awsRegion": "eu-central-1",
			}, nil)},
			expectedError: false,
		},
		{
			name:                  "empty topics slice",
			consumerGroup:         createTestConsumerGroup("test-cg", "test-ns", nil, []string{}),
			triggerAuthentication: nil,
			autoscalerConfig:      createDefaultAutoscalerConfig(),
			expectedTriggers:      []kedav1alpha1.ScaleTriggers{},
			expectedError:         false,
		},
		{
			name: "invalid lag threshold annotation",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingLagThreshold: "invalid",
			}, nil),
			triggerAuthentication:  nil,
			autoscalerConfig:       createDefaultAutoscalerConfig(),
			expectedTriggers:       nil,
			expectedError:          true,
			expectedErrorSubstring: "Expected value for annotation",
		},
		{
			name: "trigger authentication takes precedence over annotations",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingAuthenticationName: "annotation-auth",
				autoscaler.AutoscalingAuthenticationKind: "TriggerAuthentication",
			}, nil),
			triggerAuthentication: &kedav1alpha1.TriggerAuthentication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "priority-auth",
					Namespace: "test-ns",
				},
			},
			autoscalerConfig: createDefaultAutoscalerConfig(),
			expectedTriggers: []kedav1alpha1.ScaleTriggers{createExpectedTrigger("test-topic", nil, &kedav1alpha1.ScaledObjectAuthRef{
				Name: "priority-auth",
			})},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			triggers, err := GenerateScaleTriggers(tt.consumerGroup, tt.triggerAuthentication, tt.autoscalerConfig)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.expectedErrorSubstring != "" && !contains(err.Error(), tt.expectedErrorSubstring) {
					t.Errorf("Expected error to contain '%s', but got: %v", tt.expectedErrorSubstring, err)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(triggers) != len(tt.expectedTriggers) {
				t.Errorf("Expected %d triggers, got %d", len(tt.expectedTriggers), len(triggers))
				return
			}

			for i, expectedTrigger := range tt.expectedTriggers {
				if i >= len(triggers) {
					t.Errorf("Missing trigger at index %d", i)
					continue
				}

				actualTrigger := triggers[i]

				// Check type
				if actualTrigger.Type != expectedTrigger.Type {
					t.Errorf("Trigger %d: expected type %s, got %s", i, expectedTrigger.Type, actualTrigger.Type)
				}

				// Check metadata
				if !reflect.DeepEqual(actualTrigger.Metadata, expectedTrigger.Metadata) {
					t.Errorf("Trigger %d: expected metadata %v, got %v", i, expectedTrigger.Metadata, actualTrigger.Metadata)
				}

				// Check authentication ref
				if !reflect.DeepEqual(actualTrigger.AuthenticationRef, expectedTrigger.AuthenticationRef) {
					t.Errorf("Trigger %d: expected AuthenticationRef %v, got %v", i, expectedTrigger.AuthenticationRef, actualTrigger.AuthenticationRef)
				}
			}
		})
	}
}

func TestGenerateTriggerAuthentication(t *testing.T) {
	tests := []struct {
		name                   string
		consumerGroup          *kafkainternals.ConsumerGroup
		secretData             map[string][]byte
		autoscalerConfig       autoscaler.AutoscalerConfig
		expectedTriggerAuth    *kedav1alpha1.TriggerAuthentication
		expectedSecret         *corev1.Secret
		expectedError          bool
		expectedErrorSubstring string
	}{
		{
			name: "authenticationName annotation has non-empty value",
			consumerGroup: createTestConsumerGroup("test-cg", "test-ns", map[string]string{
				autoscaler.AutoscalingAuthenticationName: "existing-auth",
			}, nil),
			secretData:          nil,
			autoscalerConfig:    createDefaultAutoscalerConfig(),
			expectedTriggerAuth: nil,
			expectedSecret:      nil,
			expectedError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			triggerAuth, secret, err := GenerateTriggerAuthentication(tt.consumerGroup, tt.secretData, tt.autoscalerConfig)

			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.expectedErrorSubstring != "" && !contains(err.Error(), tt.expectedErrorSubstring) {
					t.Errorf("Expected error to contain '%s', but got: %v", tt.expectedErrorSubstring, err)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(triggerAuth, tt.expectedTriggerAuth) {
				t.Errorf("Expected TriggerAuthentication %v, got %v", tt.expectedTriggerAuth, triggerAuth)
			}

			if !reflect.DeepEqual(secret, tt.expectedSecret) {
				t.Errorf("Expected Secret %v, got %v", tt.expectedSecret, secret)
			}
		})
	}
}

func TestSetAutoscalingAnnotations(t *testing.T) {
	tests := []struct {
		name                string
		objAnnotations      map[string]string
		expectedAnnotations map[string]string
	}{
		{
			name: "with authentication name annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
			},
		},
		{
			name: "with authentication kind annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationKind: "ClusterTriggerAuthentication",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationKind: "ClusterTriggerAuthentication",
			},
		},
		{
			name: "with aws region annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAwsRegion: "us-west-2",
				"some.other.annotation":         "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAwsRegion: "us-west-2",
			},
		},
		{
			name: "with all three authentication annotations",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
				autoscaler.AutoscalingAuthenticationKind: "TriggerAuthentication",
				autoscaler.AutoscalingAwsRegion:          "eu-central-1",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
				autoscaler.AutoscalingAuthenticationKind: "TriggerAuthentication",
				autoscaler.AutoscalingAwsRegion:          "eu-central-1",
			},
		},
		{
			name: "with empty authentication name annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "",
				autoscaler.AutoscalingAwsRegion:          "us-east-1",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAwsRegion: "us-east-1",
			},
		},
		{
			name: "with empty authentication kind annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
				autoscaler.AutoscalingAuthenticationKind: "",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
			},
		},
		{
			name: "with empty aws region annotation",
			objAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
				autoscaler.AutoscalingAwsRegion:          "",
				"some.other.annotation":                  "other-value",
			},
			expectedAnnotations: map[string]string{
				autoscaler.AutoscalingAuthenticationName: "my-auth",
			},
		},
		{
			name: "with no target annotations",
			objAnnotations: map[string]string{
				"some.other.annotation": "other-value",
				"another.annotation":    "another-value",
			},
			expectedAnnotations: map[string]string{},
		},
		{
			name:                "with nil annotations",
			objAnnotations:      nil,
			expectedAnnotations: nil,
		},
		{
			name:                "with empty annotations map",
			objAnnotations:      map[string]string{},
			expectedAnnotations: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SetAutoscalingAnnotations(tt.objAnnotations)

			if tt.expectedAnnotations == nil {
				if result != nil {
					t.Errorf("Expected nil result, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("Expected non-nil result, got nil")
				return
			}

			// Check that only the expected annotations are present
			if len(result) != len(tt.expectedAnnotations) {
				t.Errorf("Expected %d annotations, got %d", len(tt.expectedAnnotations), len(result))
			}

			// Check each expected annotation
			for key, expectedValue := range tt.expectedAnnotations {
				actualValue, ok := result[key]
				if !ok {
					t.Errorf("Expected annotation %s not found in result", key)
					continue
				}
				if actualValue != expectedValue {
					t.Errorf("For annotation %s, expected value %s, got %s", key, expectedValue, actualValue)
				}
			}

			// Check that no unexpected annotations are present
			for key := range result {
				if _, ok := tt.expectedAnnotations[key]; !ok {
					t.Errorf("Unexpected annotation %s found in result", key)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

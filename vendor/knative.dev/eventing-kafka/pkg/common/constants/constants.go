/*
Copyright 2020 The Knative Authors

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

package constants

import "time"

const (

	// DefaultNumPartitions is the KafkaChannel Spec default for the number of partitions
	DefaultNumPartitions = 1

	// DefaultReplicationFactor is the KafkaChannel Spec default for the replication factor
	DefaultReplicationFactor = 1

	// DefaultRetentionISO8601Duration is the KafkaChannel Spec default for the retention duration as an ISO-8601 string
	DefaultRetentionISO8601Duration = "PT168H" // Precise 7 Days

	// DefaultRetentionDuration is the time.Duration equivalent of the DefaultRetentionISO8601Duration
	DefaultRetentionDuration = 7 * 24 * time.Hour // Precise 7 Days

	// SettingsConfigMapName is the name of the configmap used to hold eventing-kafka settings
	SettingsConfigMapName = "config-kafka"

	// SettingsConfigMapMountPath is the mount path of the configmap used to hold eventing-kafka settings
	SettingsConfigMapMountPath = "/etc/" + SettingsConfigMapName

	// EventingKafkaSettingsConfigKey is the field in the configmap used to hold eventing-kafka settings
	EventingKafkaSettingsConfigKey = "eventing-kafka"

	// SaramaSettingsConfigKey is the name of the field in the Data section of the eventing-kafka configmap that holds Sarama config YAML
	SaramaSettingsConfigKey = "sarama"

	// VersionConfigKey is the name of the field used to store the version of the configmap data, for upgrade purposes
	VersionConfigKey = "version"

	// DefaultMaxIdleConns is the default values for the cloud events connection argument "MaxIdleConns", if not overridden
	DefaultMaxIdleConns = 1000
	// DefaultMaxIdleConnsPerHost is the default values for the cloud events connection argument "MaxIdleConnsPerHost", if not overridden
	DefaultMaxIdleConnsPerHost = 100

	// ConfigMapHashAnnotationKey is an annotation is used by the controller to track updates
	// to config-kafka and apply them in the dispatcher deployment
	ConfigMapHashAnnotationKey = "kafka.eventing.knative.dev/configmap-hash"

	// CurrentConfigVersion is the current version which should be in the "version" field of the config-kafka configmap
	CurrentConfigVersion = "1.0.0"

	// KafkaSecretKeyNamespace is the username key in the Kafka Auth Config Secret
	KafkaSecretKeyNamespace = "namespace"
	// KafkaSecretKeyUsername is the username key in the Kafka Auth Config Secret
	KafkaSecretKeyUsername = "username"
	// KafkaSecretKeyPassword is the password key in the Kafka Auth Config Secret
	KafkaSecretKeyPassword = "password"
	// KafkaSecretKeySaslType is the SASL type key in the Kafka Auth Config Secret
	KafkaSecretKeySaslType = "sasltype"

	// KnativeLoggingConfigMapNameEnvVarKey Is The Environment Variable Used For Knative Logging Configuration
	KnativeLoggingConfigMapNameEnvVarKey = "CONFIG_LOGGING_NAME" // Note - Matches value of configMapNameEnv constant in Knative.dev/pkg/logging !

	// KafkaTopicConfigRetentionMs is the key in the Sarama TopicDetail ConfigEntries map for retention time (in ms)
	KafkaTopicConfigRetentionMs = "retention.ms"
)

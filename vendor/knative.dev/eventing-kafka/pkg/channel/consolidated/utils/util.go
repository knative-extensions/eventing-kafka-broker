/*
Copyright 2019 The Knative Authors

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

package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
	"knative.dev/eventing-kafka/pkg/common/kafka/sarama"
)

const (
	BrokerConfigMapKey           = "bootstrapServers"
	AuthSecretName               = "authSecretName"
	AuthSecretNamespace          = "authSecretNamespace"
	MaxIdleConnectionsKey        = "maxIdleConns"
	MaxIdleConnectionsPerHostKey = "maxIdleConnsPerHost"

	KafkaChannelSeparator = "."

	knativeKafkaTopicPrefix = "knative-messaging-kafka"
)

type KafkaConfig struct {
	Brokers       []string
	EventingKafka *config.EventingKafkaConfig
}

// GetKafkaConfig returns the details of the Kafka cluster.
func GetKafkaConfig(ctx context.Context, clientId string, configMap map[string]string, getAuth sarama.GetAuth) (*KafkaConfig, error) {
	if len(configMap) == 0 {
		return nil, fmt.Errorf("missing configuration")
	}

	// Set some default values; will be overwritten by anything that exists in the configMap
	eventingKafkaConfig := &config.EventingKafkaConfig{
		CloudEvents: config.EKCloudEventConfig{
			MaxIdleConns:        constants.DefaultMaxIdleConns,
			MaxIdleConnsPerHost: constants.DefaultMaxIdleConnsPerHost,
		},
		Kafka: config.EKKafkaConfig{
			AuthSecretName:      sarama.DefaultAuthSecretName,
			AuthSecretNamespace: system.Namespace(),
		},
	}
	var err error

	if configMap[constants.VersionConfigKey] != constants.CurrentConfigVersion {
		// Backwards-compatibility: Support old configmap format
		err = configmap.Parse(configMap,
			configmap.AsString(BrokerConfigMapKey, &eventingKafkaConfig.Kafka.Brokers),
			configmap.AsString(AuthSecretName, &eventingKafkaConfig.Kafka.AuthSecretName),
			configmap.AsString(AuthSecretNamespace, &eventingKafkaConfig.Kafka.AuthSecretNamespace),
			configmap.AsInt(MaxIdleConnectionsKey, &eventingKafkaConfig.CloudEvents.MaxIdleConns),
			configmap.AsInt(MaxIdleConnectionsPerHostKey, &eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost),
		)
		// Since LoadSettings isn't going to be called in this situation, we need to call getAuth explicitly
		eventingKafkaConfig.Auth = getAuth(ctx, eventingKafkaConfig.Kafka.AuthSecretName, eventingKafkaConfig.Kafka.AuthSecretNamespace)
		ekConfigFromOldFormat, err := sarama.LoadSettings(ctx, clientId, configMap, getAuth)
		if err != nil {
			return nil, err
		}
		eventingKafkaConfig.Sarama = ekConfigFromOldFormat.Sarama
	} else {
		eventingKafkaConfig, err = sarama.LoadSettings(ctx, clientId, configMap, getAuth)
	}
	if err != nil {
		return nil, err
	}

	// Enable Sarama logging if specified in the ConfigMap
	sarama.EnableSaramaLogging(eventingKafkaConfig.Sarama.EnableLogging)

	if eventingKafkaConfig.Kafka.Brokers == "" {
		return nil, errors.New("missing or empty brokers in configuration")
	}
	bootstrapServersSplitted := strings.Split(eventingKafkaConfig.Kafka.Brokers, ",")
	for _, s := range bootstrapServersSplitted {
		if len(s) == 0 {
			return nil, errors.New("empty brokers value in configuration")
		}
	}

	return &KafkaConfig{
		Brokers:       bootstrapServersSplitted,
		EventingKafka: eventingKafkaConfig,
	}, nil
}

func TopicName(separator, namespace, name string) string {
	topic := []string{knativeKafkaTopicPrefix, namespace, name}
	return strings.Join(topic, separator)
}

func FindContainer(d *appsv1.Deployment, containerName string) *corev1.Container {
	for i := range d.Spec.Template.Spec.Containers {
		if d.Spec.Template.Spec.Containers[i].Name == containerName {
			return &d.Spec.Template.Spec.Containers[i]
		}
	}

	return nil
}

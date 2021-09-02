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

package sarama

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"knative.dev/pkg/system"

	"knative.dev/eventing-kafka/pkg/common/client"
	"knative.dev/eventing-kafka/pkg/common/config"
	commonconfig "knative.dev/eventing-kafka/pkg/common/config"
	"knative.dev/eventing-kafka/pkg/common/constants"
)

const DefaultAuthSecretName = "kafka-cluster"

// EnableSaramaLogging Is A Utility Function For Enabling Sarama Logging (Debugging)
func EnableSaramaLogging(enable bool) {
	if enable {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	} else {
		sarama.Logger = log.New(ioutil.Discard, "[Sarama] ", log.LstdFlags)
	}
}

// GetAuth Is The Function Type Used To Delay Loading Auth Config Until The Secret Name/Namespace Are Known
type GetAuth func(ctx context.Context, authSecretName string, authSecretNamespace string) *client.KafkaAuthConfig

// LoadAuthConfig Creates A Sarama-Safe KafkaAuthConfig From The Specified Secret Name/Namespace
func LoadAuthConfig(ctx context.Context, name string, namespace string) *client.KafkaAuthConfig {
	// Extract The Relevant Data From The Kafka Secret And Create A Kafka Auth Config
	kafkaAuthCfg := config.GetAuthConfigFromKubernetes(ctx, name, namespace)
	if kafkaAuthCfg != nil &&
		kafkaAuthCfg.SASL != nil &&
		kafkaAuthCfg.SASL.User == "" {
		if kafkaAuthCfg.TLS != nil {
			// backwards-compatibility requires leaving the TLS auth config alone if it exists
			kafkaAuthCfg.SASL = nil
		} else {
			kafkaAuthCfg = nil // The Sarama builder expects a nil KafkaAuthConfig if no authentication is desired
		}
	}
	return kafkaAuthCfg
}

// LoadSettings Loads The Sarama & EventingKafka Configuration From The ConfigMap
// The Provided Context Must Have A Kubernetes Client Associated With It
func LoadSettings(ctx context.Context, clientId string, configMap map[string]string, getAuthConfig GetAuth) (*commonconfig.EventingKafkaConfig, error) {
	// Validate The ConfigMap Data
	if configMap == nil {
		return nil, fmt.Errorf("attempted to merge sarama settings with empty configmap")
	}

	ekConfig, err := LoadEventingKafkaSettings(configMap)
	if err != nil {
		return nil, err
	}

	ekConfig.Auth = getAuthConfig(ctx, ekConfig.Kafka.AuthSecretName, ekConfig.Kafka.AuthSecretNamespace)

	// Merge The ConfigMap Settings Into The Provided Config
	saramaShell := &struct {
		EnableLogging bool   `json:"enableLogging"`
		Config        string `json:"config"`
	}{}
	var saramaConfigString string

	if configMap[constants.VersionConfigKey] != constants.CurrentConfigVersion {
		// In the old version, the sarama config string was the entire field
		saramaConfigString = configMap[constants.SaramaSettingsConfigKey]
	} else {
		err = yaml.Unmarshal([]byte(configMap[constants.SaramaSettingsConfigKey]), &saramaShell)
		if err != nil {
			return nil, err
		}
		if saramaShell == nil {
			// An empty or missing sarama field will create a default sarama.Config
			ekConfig.Sarama.EnableLogging = false
		} else {
			ekConfig.Sarama.EnableLogging = saramaShell.EnableLogging
			saramaConfigString = saramaShell.Config
		}
	}

	// Merge The Sarama Settings In The ConfigMap Into A New Base Sarama Config
	ekConfig.Sarama.Config, err = client.NewConfigBuilder().
		WithDefaults().
		FromYaml(saramaConfigString).
		WithAuth(ekConfig.Auth).
		WithClientId(clientId).
		Build(ctx)

	return ekConfig, err
}

// upgradeConfig converts an old configmap into a new EventingKafkaConfig
// Returns nil if an upgrade is either unnecessary or impossible
func upgradeConfig(data map[string]string) *commonconfig.EventingKafkaConfig {

	// If the map is nil or missing the top values, it can't be upgraded
	if data == nil {
		return nil
	}
	if _, ok := data[constants.EventingKafkaSettingsConfigKey]; !ok {
		return nil
	}

	version, ok := data[constants.VersionConfigKey]
	if ok && version == constants.CurrentConfigVersion {
		// Current version; no changes necessary
		return nil
	}

	// Pre-1.0.0 structs
	type oldKafkaConfig struct {
		Brokers             string `json:"brokers,omitempty"`
		EnableSaramaLogging bool   `json:"enableSaramaLogging,omitempty"`
		AdminType           string `json:"adminType,omitempty"`
		AuthSecretName      string `json:"authSecretName,omitempty"`
		AuthSecretNamespace string `json:"authSecretNamespace,omitempty"`
	}
	type oldEventingKafkaConfig struct {
		Receiver    commonconfig.EKReceiverConfig   `json:"receiver,omitempty"`
		Dispatcher  commonconfig.EKDispatcherConfig `json:"dispatcher,omitempty"`
		CloudEvents commonconfig.EKCloudEventConfig `json:"cloudevents,omitempty"`
		Kafka       oldKafkaConfig                  `json:"kafka,omitempty"`
	}

	oldConfig := &oldEventingKafkaConfig{}
	err := yaml.Unmarshal([]byte(data[constants.EventingKafkaSettingsConfigKey]), &oldConfig)
	if err != nil || oldConfig == nil {
		return nil // Can't be upgraded
	}

	// Upgrade the eventing-kafka config by placing old values into the new struct
	return &commonconfig.EventingKafkaConfig{
		Channel: commonconfig.EKChannelConfig{
			Dispatcher: oldConfig.Dispatcher,
			Receiver:   oldConfig.Receiver,
			AdminType:  oldConfig.Kafka.AdminType,
		},
		CloudEvents: oldConfig.CloudEvents,
		Kafka: commonconfig.EKKafkaConfig{
			Brokers:             oldConfig.Kafka.Brokers,
			AuthSecretName:      oldConfig.Kafka.AuthSecretName,
			AuthSecretNamespace: oldConfig.Kafka.AuthSecretNamespace,
		},
		Sarama: commonconfig.EKSaramaConfig{
			EnableLogging: oldConfig.Kafka.EnableSaramaLogging,
			Config:        nil,
		},
	}
}

func LoadEventingKafkaSettings(configMap map[string]string) (*commonconfig.EventingKafkaConfig, error) {
	eventingKafkaConfig := upgradeConfig(configMap)
	if eventingKafkaConfig == nil {
		// Upgrade didn't produce an EventingKafkaConfig, so assume it is the current version

		// Unmarshal The Eventing-Kafka ConfigMap YAML Into A EventingKafkaSettings Struct
		eventingKafkaConfig = &commonconfig.EventingKafkaConfig{}
		err := yaml.Unmarshal([]byte(configMap[constants.EventingKafkaSettingsConfigKey]), &eventingKafkaConfig)
		if err != nil {
			return nil, fmt.Errorf("ConfigMap's eventing-kafka value could not be converted to an EventingKafkaConfig struct: %s : %v", err, configMap[constants.EventingKafkaSettingsConfigKey])
		}
	}

	if eventingKafkaConfig == nil {
		// A nil config is not valid; create an empty config so that defaults may be set
		eventingKafkaConfig = &commonconfig.EventingKafkaConfig{}
	}

	// Set Some Default Values If Missing

	// Increase The Idle Connection Limits From Transport Defaults If Not Provided (see net/http/DefaultTransport)
	if eventingKafkaConfig.CloudEvents.MaxIdleConns == 0 {
		eventingKafkaConfig.CloudEvents.MaxIdleConns = constants.DefaultMaxIdleConns
	}
	if eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost == 0 {
		eventingKafkaConfig.CloudEvents.MaxIdleConnsPerHost = constants.DefaultMaxIdleConnsPerHost
	}

	// Default Replicas To 1 If Not Specified
	if eventingKafkaConfig.Channel.Receiver.Replicas < 1 {
		eventingKafkaConfig.Channel.Receiver.Replicas = 1
	}
	if eventingKafkaConfig.Channel.Dispatcher.Replicas < 1 {
		eventingKafkaConfig.Channel.Dispatcher.Replicas = 1
	}

	// Set Default Values For Secret
	if len(eventingKafkaConfig.Kafka.AuthSecretNamespace) == 0 {
		eventingKafkaConfig.Kafka.AuthSecretNamespace = system.Namespace()
	}
	if len(eventingKafkaConfig.Kafka.AuthSecretName) == 0 {
		eventingKafkaConfig.Kafka.AuthSecretName = DefaultAuthSecretName
	}

	return eventingKafkaConfig, nil
}

// AuthFromSarama creates a KafkaAuthConfig using the SASL settings from
// a given Sarama config, or nil if there is no SASL user in that config
func AuthFromSarama(config *sarama.Config) *client.KafkaAuthConfig {
	// Use the SASL settings from the provided Sarama config only if the user is non-empty
	if config.Net.SASL.User != "" {
		return &client.KafkaAuthConfig{
			SASL: &client.KafkaSaslConfig{
				User:     config.Net.SASL.User,
				Password: config.Net.SASL.Password,
				SaslType: string(config.Net.SASL.Mechanism),
			},
		}
	} else {
		// If the user is empty, return explicitly nil authentication
		return nil
	}
}

// StringifyHeaders Is A Utility function to convert []byte headers to string ones for logging purposes
func StringifyHeaders(headers []sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}

// StringifyHeaderPtrs Is A Pointer-version of the StringifyHeaders function
func StringifyHeaderPtrs(headers []*sarama.RecordHeader) map[string][]string {
	stringHeaders := make(map[string][]string)
	for _, header := range headers {
		key := string(header.Key)
		stringHeaders[key] = append(stringHeaders[key], string(header.Value))
	}
	return stringHeaders
}

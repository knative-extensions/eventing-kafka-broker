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

package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ghodss/yaml"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
	"knative.dev/eventing-kafka/pkg/channel/distributed/common/kafka/constants"
	"knative.dev/pkg/logging"
)

type KafkaAuthConfig struct {
	TLS  *KafkaTlsConfig
	SASL *KafkaSaslConfig
}

type KafkaTlsConfig struct {
	Cacert   string
	Usercert string
	Userkey  string
}

type KafkaSaslConfig struct {
	User     string
	Password string
	SaslType string
}

// HasSameSettings returns true if all of the SASL settings in the provided config are the same as in this struct
func (c *KafkaSaslConfig) HasSameSettings(saramaConfig *sarama.Config) bool {
	return saramaConfig.Net.SASL.User == c.User &&
		saramaConfig.Net.SASL.Password == c.Password &&
		string(saramaConfig.Net.SASL.Mechanism) == c.SaslType
}

// HasSameBrokers returns true if all of the brokers in the slice are present and in the same order as
// the ones in the consolidated str
func HasSameBrokers(brokerString string, brokers []string) bool {

	// Special case for empty string and slice, because Split() always returns at least one element
	if brokerString == "" && len(brokers) == 0 {
		return true
	}

	splitBrokers := strings.Split(brokerString, ",")

	// Not the same if there aren't the same number of brokers
	if len(splitBrokers) != len(brokers) {
		return false
	}

	// Not the same if any of the individual brokers are different
	// Note that brokers must be in the same order in the string and the slice in order to be considered equal
	for index := range brokers {
		if splitBrokers[index] != brokers[index] {
			return false
		}
	}

	// Same number and values of brokers
	return true
}

// ConfigBuilder builds the Sarama config using multiple options.
// Precedence for the options is following:
// - get existing config if provided, create a new one otherwise
// - apply defaults
// - apply settings from YAML string
// - apply settings from KafkaAuthConfig
// - apply individual settings like version, clientId
type ConfigBuilder interface {
	// WithExisting makes the builder use an existing Sarama
	// config as a base.
	WithExisting(existing *sarama.Config) ConfigBuilder

	// WithDefaults makes the builder apply
	// some defaults that we always use in eventing-kafka.
	WithDefaults() ConfigBuilder

	// FromYaml makes the builder apply the settings
	// in a YAML-string for Sarama.
	FromYaml(saramaYaml string) ConfigBuilder

	// WithAuth makes the builder apply the TLS/SASL settings
	// on the config for the given KafkaAuthConfig
	WithAuth(kafkaAuthConfig *KafkaAuthConfig) ConfigBuilder

	// WithVersion makes the builder set the version
	// explicitly, regardless what's set in the existing config
	// (if provided) or in the YAML-string
	WithVersion(version *sarama.KafkaVersion) ConfigBuilder

	// WithClientId makes the builder set the clientId
	// explicitly, regardless what's set in the existing config
	// (if provided) or in the YAML-string
	WithClientId(clientId string) ConfigBuilder

	// WithInitialOffset sets the initial offset to use
	// if no offset was previously committed
	WithInitialOffset(offset v1beta1.Offset) ConfigBuilder

	// Build builds the Sarama config with the given context.
	// Context is used for getting the config at the moment.
	Build(ctx context.Context) (*sarama.Config, error)
}

func NewConfigBuilder() ConfigBuilder {
	return &configBuilder{}
}

type configBuilder struct {
	existing      *sarama.Config
	defaults      bool
	version       *sarama.KafkaVersion
	clientId      string
	yaml          string
	auth          *KafkaAuthConfig
	initialOffset v1beta1.Offset
}

func (b *configBuilder) WithExisting(existing *sarama.Config) ConfigBuilder {
	b.existing = existing
	return b
}

func (b *configBuilder) WithDefaults() ConfigBuilder {
	b.defaults = true
	return b
}

func (b *configBuilder) WithVersion(version *sarama.KafkaVersion) ConfigBuilder {
	b.version = version
	return b
}

func (b *configBuilder) WithClientId(clientId string) ConfigBuilder {
	b.clientId = clientId
	return b
}

func (b *configBuilder) FromYaml(saramaSettingsYamlString string) ConfigBuilder {
	b.yaml = saramaSettingsYamlString
	return b
}

func (b *configBuilder) WithAuth(kafkaAuthCfg *KafkaAuthConfig) ConfigBuilder {
	b.auth = kafkaAuthCfg
	return b
}

func (b *configBuilder) WithInitialOffset(offset v1beta1.Offset) ConfigBuilder {
	b.initialOffset = offset
	return b
}

// Build builds the Sarama config.
func (b *configBuilder) Build(ctx context.Context) (*sarama.Config, error) {
	var config *sarama.Config

	// check if there's existing first
	if b.existing != nil {
		config = b.existing
	} else {
		config = sarama.NewConfig()
	}

	// then apply defaults, if requested
	if b.defaults {
		// Use Our Default Minimum Version
		config.Version = constants.ConfigKafkaVersionDefault

		// We Always Want To Know About Consumer Errors
		config.Consumer.Return.Errors = true

		// We Always Want Success Messages From Producer
		config.Producer.Return.Successes = true
	}

	// then apply stuff from YAML
	if b.yaml != "" {
		// Extract (Remove) The KafkaVersion From The Sarama Config YAML as we can't marshal it regularly
		saramaSettingsYamlString, kafkaVersionInYaml, err := extractKafkaVersion(b.yaml)
		if err != nil {
			return nil, fmt.Errorf("failed to extract KafkaVersion from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
		}

		// Extract (Remove) Any TLS.Config RootCAs & Set In Sarama.Config
		saramaSettingsYamlString, certPool, err := extractRootCerts(saramaSettingsYamlString)
		if err != nil {
			return nil, fmt.Errorf("failed to extract RootPEMs from Sarama Config YAML: err=%s : config=%+v", err, saramaSettingsYamlString)
		}

		// Unmarshall The Sarama Config Yaml Into The Provided Sarama.Config Object
		err = yaml.Unmarshal([]byte(saramaSettingsYamlString), &config)
		if err != nil {
			return nil, fmt.Errorf("ConfigMap's sarama value could not be converted to a Sarama.Config struct: %s : %v", err, saramaSettingsYamlString)
		}

		// Override The Custom Parsed KafkaVersion, if it is specified in the YAML
		if kafkaVersionInYaml != nil {
			config.Version = *kafkaVersionInYaml
		}

		// Override Any Custom Parsed TLS.Config.RootCAs
		if certPool != nil && len(certPool.Subjects()) > 0 {
			config.Net.TLS.Config = &tls.Config{RootCAs: certPool}
		}
	}

	// then apply auth settings
	if b.auth != nil {
		// TLS
		if b.auth.TLS != nil {
			config.Net.TLS.Enable = true

			// if we have TLS, we might want to use the certs for self-signed CERTs
			if b.auth.TLS.Cacert != "" {
				tlsConfig, err := newTLSConfig(b.auth.TLS.Usercert, b.auth.TLS.Userkey, b.auth.TLS.Cacert)
				if err != nil {
					return nil, fmt.Errorf("Error creating TLS config: %w", err)
				}
				config.Net.TLS.Config = tlsConfig
			}
		}
		// SASL
		if b.auth.SASL != nil {
			config.Net.SASL.Enable = true
			config.Net.SASL.Handshake = true

			// if SaslType is not provided we are defaulting to PLAIN
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

			if b.auth.SASL.SaslType == sarama.SASLTypeSCRAMSHA256 {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			}

			if b.auth.SASL.SaslType == sarama.SASLTypeSCRAMSHA512 {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			}
			config.Net.SASL.User = b.auth.SASL.User
		}
	}

	// finally, apply individual fields
	if b.version != nil {
		config.Version = *b.version
	}
	if b.clientId != "" {
		config.ClientID = b.clientId
	}

	if b.initialOffset != "" {
		switch b.initialOffset {
		case v1beta1.OffsetEarliest:
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
		case v1beta1.OffsetLatest:
			config.Consumer.Offsets.Initial = sarama.OffsetNewest
		}
	}

	logger := logging.FromContext(ctx)
	logger.Infof("Built Sarama config: %+v", config)

	if b.auth != nil && b.auth.SASL != nil {
		config.Net.SASL.Password = b.auth.SASL.Password
	}

	return config, nil
}

// ConfigEqual is a convenience function to determine if two given sarama.Config structs are identical aside
// from unserializable fields (e.g. function pointers).  To ignore parts of the sarama.Config struct, pass
// them in as the "ignore" parameter.
func ConfigEqual(config1, config2 *sarama.Config, ignore ...interface{}) bool {
	// If some of the types in the sarama.Config struct are not ignored, these kinds of errors will appear:
	// panic: cannot handle unexported field at {*sarama.Config}.Consumer.Group.Rebalance.Strategy.(*sarama.balanceStrategy).name

	// Note that using the types directly from config1 is convenient (it allows us to call IgnoreTypes instead of the
	// more complicated IgnoreInterfaces), but it will fail if, for example, config1.Consumer.Group.Rebalance is nil

	// However, the sarama.NewConfig() function sets all of these values to a non-nil default, so the risk
	// is minimal and should be caught by one of the several unit tests for this function if the sarama vendor
	// code is updated and these defaults become something invalid at that time)

	ignoreTypeList := append([]interface{}{
		config1.Consumer.Group.Rebalance.Strategy,
		config1.MetricRegistry,
		config1.Producer.Partitioner},
		ignore...)
	ignoredTypes := cmpopts.IgnoreTypes(ignoreTypeList...)

	// If some interfaces are not included in the "IgnoreUnexported" list, these kinds of errors will appear:
	// panic: cannot handle unexported field at {*sarama.Config}.Net.TLS.Config.mutex: "crypto/tls".Config

	// Note that x509.CertPool and tls.Config are created here explicitly because config1/config2 may not
	// have those fields, and results in a nil pointer panic if used in the IgnoreUnexported list indirectly
	// like config1.Version is (Version is required to be present in a sarama.Config struct).

	ignoredUnexported := cmpopts.IgnoreUnexported(config1.Version, x509.CertPool{}, tls.Config{})

	// Compare the two sarama config structs, ignoring types and unexported fields as specified
	return cmp.Equal(config1, config2, ignoredTypes, ignoredUnexported)
}

// verifyCertSkipHostname verifies certificates in the same way that the
// default TLS handshake does, except it skips hostname verification. It must
// be used with InsecureSkipVerify.
func verifyCertSkipHostname(roots *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(certs [][]byte, _ [][]*x509.Certificate) error {
		opts := x509.VerifyOptions{
			Roots:         roots,
			CurrentTime:   time.Now(),
			Intermediates: x509.NewCertPool(),
		}

		leaf, err := x509.ParseCertificate(certs[0])
		if err != nil {
			return err
		}

		for _, asn1Data := range certs[1:] {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return err
			}

			opts.Intermediates.AddCert(cert)
		}

		_, err = leaf.Verify(opts)
		return err
	}
}

// NewTLSConfig returns a *tls.Config using the given ceClient cert, ceClient key,
// and CA certificate. If none are appropriate, a nil *tls.Config is returned.
func newTLSConfig(clientCert, clientKey, caCert string) (*tls.Config, error) {
	valid := false

	config := &tls.Config{}

	if clientCert != "" && clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
		valid = true
	}

	if caCert != "" {
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM([]byte(caCert))
		config.RootCAs = caCertPool
		// The CN of Heroku Kafka certs do not match the hostname of the
		// broker, but Go's default TLS behavior requires that they do.
		config.VerifyPeerCertificate = verifyCertSkipHostname(caCertPool)
		config.InsecureSkipVerify = true
		valid = true
	}

	if !valid {
		config = nil
	}

	return config, nil
}

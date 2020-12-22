/*
 * Copyright 2020 The Knative Authors
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

package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/Shopify/sarama"
)

const (
	ProtocolKey = "protocol"

	CaCertificateKey = "ca.crt"

	UserCertificate = "user.crt"
	UserKey         = "user.key"

	SaslMechanismKey = "sasl.mechanism"
	SaslUserKey      = "user"
	SaslPasswordKey  = "password"

	ProtocolPlaintext     = "PLAINTEXT"
	ProtocolSASLPlaintext = "SASL_PLAINTEXT"
	ProtocolSSL           = "SSL"
	ProtocolSASLSSL       = "SASL_SSL"

	SaslPlain       = "PLAIN"
	SaslScramSha256 = "SCRAM-SHA-256"
	SaslScramSha512 = "SCRAM-SHA-512"
)

var supportedProtocols = fmt.Sprintf("%v", []string{
	ProtocolPlaintext,
	ProtocolSASLPlaintext,
	ProtocolSSL,
	ProtocolSASLSSL,
})

type ConfigOption func(config *sarama.Config) error

func options(config *sarama.Config, options ...ConfigOption) error {
	for _, opt := range options {
		if err := opt(config); err != nil {
			return err
		}
	}
	return nil
}

func secretData(data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		protocolBytes, ok := data[ProtocolKey]
		if !ok {
			return fmt.Errorf("protocol required (key: %s) supported protocols: %s", ProtocolKey, supportedProtocols)
		}

		protocol := string(protocolBytes)
		if protocol == ProtocolPlaintext {
			return nil
		}

		if protocol == ProtocolSASLPlaintext {
			return options(config,
				saslConfig(protocol, data),
			)
		}

		if protocol == ProtocolSSL {
			return options(config,
				sslConfig(protocol, data),
			)
		}

		if protocol == ProtocolSASLSSL {
			return options(config,
				saslConfig(protocol, data),
				sslConfig(protocol, data),
			)
		}

		return fmt.Errorf("protocol %s unsupported (key: %s), supported protocols: %s", protocol, ProtocolKey, supportedProtocols)
	}
}

func saslConfig(protocol string, data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		// Supported mechanism SASL/PLAIN or SASL/SCRAM.
		givenSASLMechanism, ok := data[SaslMechanismKey]
		if !ok {
			return fmt.Errorf("[protocol %s] SASL mechanism required (key: %s)", protocol, SaslMechanismKey)
		}
		saslMechanism := string(givenSASLMechanism)

		user, ok := data[SaslUserKey]
		if !ok || len(user) == 0 {
			return fmt.Errorf("[protocol %s] SASL user required (key: %s)", protocol, SaslUserKey)
		}

		password, ok := data[SaslPasswordKey]
		if !ok || len(password) == 0 {
			return fmt.Errorf("[protocol %s] SASL password required (key: %s)", protocol, SaslPasswordKey)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = string(user)
		config.Net.SASL.Password = string(password)

		if saslMechanism == SaslPlain {
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			return nil
		}

		if saslMechanism == SaslScramSha512 {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = sha512ScramClientGeneratorFunc
			return nil
		}

		if saslMechanism == SaslScramSha256 {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = sha256ScramClientGeneratorFunc
			return nil
		}

		return fmt.Errorf("[protocol %s] unsupported SASL mechanism (key: %s)", protocol, SaslMechanismKey)
	}
}

func sslConfig(protocol string, data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		caCert, ok := data[CaCertificateKey]
		if !ok {
			return fmt.Errorf("[protocol %s] required CA certificate (key: %s)", protocol, CaCertificateKey)
		}

		var tlsCerts []tls.Certificate
		if protocol == ProtocolSSL {
			userKeyCert, ok := data[UserKey]
			if !ok {
				return fmt.Errorf("[protocol %s] required user key (key: %s)", protocol, UserKey)
			}

			userCert, ok := data[UserCertificate]
			if !ok {
				return fmt.Errorf("[protocol %s] required user key (key: %s)", protocol, UserCertificate)
			}

			tlsCert, err := tls.X509KeyPair(userCert, userKeyCert)
			if err != nil {
				return fmt.Errorf("[protocol %s] failed to load x.509 key pair: %w", protocol, err)
			}
			tlsCerts = []tls.Certificate{tlsCert}
		}

		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			Certificates: tlsCerts,
			RootCAs:      certPool,
		}

		return nil
	}
}

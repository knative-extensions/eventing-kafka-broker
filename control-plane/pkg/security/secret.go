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
	"strconv"

	"github.com/Shopify/sarama"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/reconciler/kafka"
)

const (
	ProtocolKey = "protocol"

	CaCertificateKey = "ca.crt"

	UserCertificate = "user.crt"
	UserKey         = "user.key"
	UserSkip        = "user.skip" // default: false

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

func secretData(data map[string][]byte) kafka.ConfigOption {
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
			return kafka.Options(config,
				saslConfig(protocol, data),
			)
		}

		if protocol == ProtocolSSL {
			return kafka.Options(config,
				sslConfig(protocol, data),
			)
		}

		if protocol == ProtocolSASLSSL {
			return kafka.Options(config,
				saslConfig(protocol, data),
				sslConfig(protocol, data),
			)
		}

		return fmt.Errorf("protocol %s unsupported (key: %s), supported protocols: %s", protocol, ProtocolKey, supportedProtocols)
	}
}

func saslConfig(protocol string, data map[string][]byte) kafka.ConfigOption {
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

func sslConfig(protocol string, data map[string][]byte) kafka.ConfigOption {
	return func(config *sarama.Config) error {

		// If there is no CaCertificateKey, we assume that we can use system's root CA set.
		var certPool *x509.CertPool
		if caCert, ok := data[CaCertificateKey]; ok {
			certPool = x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caCert) {
				return fmt.Errorf(
					"[protocol %s] failed to parse CA certificates (key: %s)",
					protocol, CaCertificateKey,
				)
			}
		}

		var tlsCerts []tls.Certificate
		if protocol == ProtocolSSL {

			// When protocol=SSL we might have 2 options:
			//	- client auth is required on the broker side (mTLS)
			//	- client auth is not required on the broker side

			skipClientAuth, err := skipClientAuthCheck(data)
			if err != nil {
				return fmt.Errorf("[protocol %s] %w", protocol, err)
			}

			if !skipClientAuth {
				userKeyCert, ok := data[UserKey]
				if !ok {
					return fmt.Errorf(
						`[protocol %s] required user key (key: %s) - use "%s: true" to disable client auth`,
						protocol, UserKey, UserSkip,
					)
				}

				userCert, ok := data[UserCertificate]
				if !ok {
					return fmt.Errorf(
						`[protocol %s] required user key (key: %s) - use "%s: true" to disable client auth`,
						protocol, UserCertificate, UserSkip,
					)
				}

				tlsCert, err := tls.X509KeyPair(userCert, userKeyCert)
				if err != nil {
					return fmt.Errorf("[protocol %s] failed to load x.509 key pair: %w", protocol, err)
				}
				tlsCerts = []tls.Certificate{tlsCert}
			}
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			MaxVersion:   tls.VersionTLS13,
			Certificates: tlsCerts,
			RootCAs:      certPool,
		}

		return nil
	}
}

func skipClientAuthCheck(data map[string][]byte) (bool, error) {
	clientAuth, ok := data[UserSkip]
	if !ok {
		return false, nil // Client auth is required by default.
	}
	enabled, err := strconv.ParseBool(string(clientAuth))
	if err != nil {
		return false, fmt.Errorf("failed to parse client auth flag (key: %s): %w", UserSkip, err)
	}
	return enabled, nil
}

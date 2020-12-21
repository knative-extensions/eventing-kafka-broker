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
	"encoding/pem"
	"fmt"

	"github.com/Shopify/sarama"
	"golang.org/x/crypto/pkcs12"
	truststorepkcs12 "software.sslmate.com/src/go-pkcs12"
)

const (
	protocolKey           = "protocol"
	truststoreKey         = "ca.p12"
	truststorePasswordKey = "ca.password"
	caCertificateKey      = "ca.crt"
	keystoreKey           = "user.p12"
	keystorePasswordKey   = "user.password"
	userCertificate       = "user.crt"
	userKey               = "user.key"
	saslMechanismKey      = "sasl.mechanism"
	saslUserKey           = "user"
	saslPasswordKey       = "password"

	protocolPlaintext     = "PLAINTEXT"
	protocolSASLPlaintext = "SASL_PLAINTEXT"
	protocolSSL           = "SSL"
	protocolSASLSSL       = "SASL_SSL"

	saslPlain       = "PLAIN"
	saslScramSha256 = "SCRAM-SHA-256"
	saslScramSha512 = "SCRAM-SHA-512"
)

var supportedProtocols = fmt.Sprintf("%v", []string{
	protocolPlaintext,
	protocolSASLPlaintext,
	protocolSSL,
	protocolSASLSSL,
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

		protocolBytes, ok := data[protocolKey]
		if !ok {
			return fmt.Errorf("protocol required (key: %s) supported protocols: %s", protocolKey, supportedProtocols)
		}

		protocol := string(protocolBytes)
		if protocol == protocolPlaintext {
			return nil
		}

		if protocol == protocolSASLPlaintext {
			return options(config,
				saslConfig(protocol, data),
			)
		}

		if protocol == protocolSSL {
			return options(config,
				sslConfig(protocol, data),
			)
		}

		if protocol == protocolSASLSSL {
			return options(config,
				saslConfig(protocol, data),
				sslConfig(protocol, data),
			)
		}

		return fmt.Errorf("protocol %s unsupported (key: %s), supported protocols: %s", protocol, protocolKey, supportedProtocols)
	}
}

func saslConfig(protocol string, data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		// Supported mechanism SASL/PLAIN or SASL/SCRAM.
		givenSASLMechanism, ok := data[saslMechanismKey]
		if !ok {
			return fmt.Errorf("[protocol %s] SASL mechanism required (key: %s)", protocol, saslMechanismKey)
		}
		saslMechanism := string(givenSASLMechanism)

		user, ok := data[saslUserKey]
		if !ok || len(user) == 0 {
			return fmt.Errorf("[protocol %s] SASL user required (key: %s)", protocol, saslUserKey)
		}

		password, ok := data[saslPasswordKey]
		if !ok || len(password) == 0 {
			return fmt.Errorf("[protocol %s] SASL password required (key: %s)", protocol, saslPasswordKey)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = string(user)
		config.Net.SASL.Password = string(password)

		if saslMechanism == saslPlain {
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			return nil
		}

		if saslMechanism == saslScramSha512 {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			config.Net.SASL.SCRAMClientGeneratorFunc = sha512ScramClientGeneratorFunc
			return nil
		}

		if saslMechanism == saslScramSha256 {
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			config.Net.SASL.SCRAMClientGeneratorFunc = sha256ScramClientGeneratorFunc
			return nil
		}

		return fmt.Errorf("[protocol %s] unsupported SASL mechanism (key: %s)", protocol, saslMechanismKey)
	}
}

func sslConfig(protocol string, data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		var certificates []tls.Certificate
		if protocol == protocolSSL {
			keystoreCerts, err := tlsCertsFromStore(data, protocol, /* what */ "keystore", keystoreKey, keystorePasswordKey)
			if err != nil {
				return err
			}
			certificates = keystoreCerts
		}
		truststoreCerts, err := x509CertsFromStore(data, protocol, /* what */ "truststore", truststoreKey, truststorePasswordKey)
		if err != nil {
			return err
		}

		certPool := x509.NewCertPool()
		for _, cert := range truststoreCerts {
			certPool.AddCert(cert)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		}

		return nil
	}
}

func tlsCertsFromStore(data map[string][]byte, protocol, what, storeKey, storePasswordKey string) ([]tls.Certificate, error) {

	store, ok := data[storeKey]
	if !ok || len(store) == 0 {
		return nil, fmt.Errorf("[protocol %s] %s archive required (key: %s)", protocol, what, storeKey)
	}
	storePassword, ok := data[storePasswordKey]
	if !ok {
		return nil, fmt.Errorf("[protocol %s] %s password required (key: %s)", protocol, what, storePasswordKey)
	}

	blocks, err := pkcs12.ToPEM(store, string(storePassword))
	if err != nil {
		return nil, conversionError(protocol, what, fmt.Errorf("failed to convert store to PEM: %w", err))
	}

	var pemData []byte
	for _, b := range blocks {
		pemData = append(pemData, pem.EncodeToMemory(b)...)
	}

	// then use PEM data for tls to construct tls certificate:
	cert, err := tls.X509KeyPair(pemData, pemData)

	return []tls.Certificate{cert}, nil
}

func x509CertsFromStore(data map[string][]byte, protocol, what, storeKey, storePasswordKey string) ([]*x509.Certificate, error) {

	store, ok := data[storeKey]
	if !ok || len(store) == 0 {
		return nil, fmt.Errorf("[protocol %s] %s archive required (key: %s)", protocol, what, storeKey)
	}
	storePassword, ok := data[storePasswordKey]
	if !ok {
		return nil, fmt.Errorf("[protocol %s] %s password required (key: %s)", protocol, what, storePasswordKey)
	}

	certs, err := truststorepkcs12.DecodeTrustStore(store, string(storePassword))
	if err != nil {
		return nil, conversionError(protocol, what, fmt.Errorf("pkcs12.Decode: %w", err))
	}

	return certs, nil
}

func conversionError(protocol, what string, err error) error {
	return fmt.Errorf("[protocol %s] failed to convert %s (key: %s) using password (key: %s) to PEM: %w", protocol, what, truststoreKey, truststorePasswordKey, err)
}

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
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
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

type ConfigOption func(config *sarama.Config) error

func Options(config *sarama.Config, options ...ConfigOption) error {
	for _, opt := range options {
		if err := opt(config); err != nil {
			return err
		}
	}
	return nil
}

func SecretData(data map[string][]byte) ConfigOption {
	return func(config *sarama.Config) error {

		protocolBytes, ok := data[protocolKey]
		if !ok {
			return fmt.Errorf("protocol required (key: %s)", protocolKey)
		}

		protocol := string(protocolBytes)
		if protocol == protocolPlaintext {
			return nil
		}

		if protocol == protocolSASLPlaintext {
			return Options(config,
				saslConfig(protocol, data),
			)
		}

		if protocol == protocolSSL {
			return Options(config,
				sslConfig(protocol, data),
			)
		}

		if protocol == protocolSASLSSL {
			return Options(config,
				saslConfig(protocol, data),
				sslConfig(protocol, data),
			)
		}

		return fmt.Errorf("protocol %s unsupported (key: %s)", protocol, protocolKey)
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

		privateKeys, keystoreCert, err := tlsCertFromStore(data, protocol, "keystore", keystoreKey, keystorePasswordKey)
		if err != nil {
			return err
		}
		truststoreCerts, err := x509CertsFromStore(data, protocol, "truststore", truststoreKey, truststorePasswordKey)
		if err != nil {
			return err
		}

		certPool := x509.NewCertPool()
		for _, cert := range truststoreCerts {
			certPool.AddCert(cert)
		}

		certs := make([]tls.Certificate, 0, len(keystoreCert))
		for i := range keystoreCert {
			certs = append(certs, tls.Certificate{
				PrivateKey: privateKeys[i],
				Leaf:       keystoreCert[i],
			})
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			Certificates: certs,
			RootCAs:      certPool,
		}

		return nil
	}
}

func tlsCertFromStore(data map[string][]byte, protocol, what, storeKey, storePasswordKey string) ([]*rsa.PrivateKey, []*x509.Certificate, error) {

	store, ok := data[storeKey]
	if !ok || len(store) == 0 {
		return nil, nil, fmt.Errorf("[protocol %s] %s archive required (key: %s)", protocol, what, storeKey)
	}
	storePassword, ok := data[storePasswordKey]
	if !ok {
		return nil, nil, fmt.Errorf("[protocol %s] %s password required (key: %s)", protocol, what, storePasswordKey)
	}

	blocks, err := pkcs12.ToPEM(store, string(storePassword))
	if err != nil {
		return nil, nil, conversionError(protocol, what, fmt.Errorf("failed to convert store to PEM: %w", err))
	}

	var privateKeys []*rsa.PrivateKey
	var certificates []*x509.Certificate
	for _, b := range blocks {
		if b.Type == "CERTIFICATE" {
			certs, err := x509.ParseCertificates(b.Bytes)
			if err != nil {
				return nil, nil, conversionError(protocol, what, fmt.Errorf("failed to parse certificates: %w", err))
			}
			certificates = append(certificates, certs...)

		} else if b.Type == "PRIVATE KEY" {
			privateKey, err := x509.ParsePKCS1PrivateKey(b.Bytes)
			if err != nil {
				return nil, nil, conversionError(protocol, what, fmt.Errorf("failed to parse PKCS1 private key: %w", err))
			}
			privateKeys = append(privateKeys, privateKey)
		}

	}

	return privateKeys, certificates, nil
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

	/* privateKey */ certs, err := truststorepkcs12.DecodeTrustStore(store, string(storePassword))
	if err != nil {
		return nil, conversionError(protocol, what, fmt.Errorf("pkcs12.Decode: %w", err))
	}

	return certs, nil
}

func conversionError(protocol, what string, err error) error {
	return fmt.Errorf("[protocol %s] failed to convert %s (key: %s) using password (key: %s) to PEM: %w", protocol, what, truststoreKey, truststorePasswordKey, err)
}

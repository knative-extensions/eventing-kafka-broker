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
	"io/ioutil"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestNoProtocol(t *testing.T) {
	config := sarama.NewConfig()

	err := Options(config, SecretData(map[string][]byte{}))

	assert.NotNil(t, err)
}

func TestUnsupportedProtocol(t *testing.T) {
	secret := map[string][]byte{
		"protocol": []byte("PLAIN"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.NotNil(t, err)
}

func TestPlaintext(t *testing.T) {
	secret := map[string][]byte{
		"protocol": []byte("PLAINTEXT"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.Nil(t, err)
}

func TestSASLPlain(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("PLAIN"),
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), config.Net.SASL.Mechanism)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

func TestSASLPlainLSCRAM256(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-256"),
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256), config.Net.SASL.Mechanism)
	assert.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

func TestSASLPlainLSCRAM512(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512), config.Net.SASL.Mechanism)
	assert.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

func TestSASLPlainSCRAM513(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-513"),
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.NotNil(t, err)
}

func TestSASLPlainLSCRAM512NoUser(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user":           []byte(""),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.NotNil(t, err)
}

func TestSASLPlainLSCRAM512NoPassword(t *testing.T) {
	secret := map[string][]byte{
		"protocol":       []byte("SASL_PLAINTEXT"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user":           []byte("my-user-name"),
		"password":       []byte(""),
	}
	config := sarama.NewConfig()

	err := Options(config, SecretData(secret))

	assert.NotNil(t, err)
}

func TestSSL(t *testing.T) {
	cap12, err := ioutil.ReadFile("testdata/ca.p12")
	assert.Nil(t, err)

	caPassword, err := ioutil.ReadFile("testdata/ca.password")
	assert.Nil(t, err)

	userp12, err := ioutil.ReadFile("testdata/user.p12")
	assert.Nil(t, err)

	userPassword, err := ioutil.ReadFile("testdata/user.password")
	assert.Nil(t, err)

	secret := map[string][]byte{
		"protocol":      []byte("SSL"),
		"user.p12":      userp12,
		"user.password": userPassword,
		"ca.p12":        cap12,
		"ca.password":   caPassword,
	}
	config := sarama.NewConfig()

	err = Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Enable)
	assert.Greater(t, len(config.Net.TLS.Config.Certificates), 0)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)
	assert.Greater(t, len(config.Net.TLS.Config.RootCAs.Subjects()), 0)
}

func TestSASLPLainSSL(t *testing.T) {
	cap12, err := ioutil.ReadFile("testdata/ca.p12")
	assert.Nil(t, err)

	caPassword, err := ioutil.ReadFile("testdata/ca.password")
	assert.Nil(t, err)

	userp12, err := ioutil.ReadFile("testdata/user.p12")
	assert.Nil(t, err)

	userPassword, err := ioutil.ReadFile("testdata/user.password")
	assert.Nil(t, err)

	secret := map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("PLAIN"),
		"user.p12":       userp12,
		"user.password":  userPassword,
		"ca.p12":         cap12,
		"ca.password":    caPassword,
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err = Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Enable)
	assert.Greater(t, len(config.Net.TLS.Config.Certificates), 0)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)
	assert.Greater(t, len(config.Net.TLS.Config.RootCAs.Subjects()), 0)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), config.Net.SASL.Mechanism)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

func TestSASLSCRAM256SSL(t *testing.T) {
	cap12, err := ioutil.ReadFile("testdata/ca.p12")
	assert.Nil(t, err)

	caPassword, err := ioutil.ReadFile("testdata/ca.password")
	assert.Nil(t, err)

	userp12, err := ioutil.ReadFile("testdata/user.p12")
	assert.Nil(t, err)

	userPassword, err := ioutil.ReadFile("testdata/user.password")
	assert.Nil(t, err)

	secret := map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("SCRAM-SHA-256"),
		"user.p12":       userp12,
		"user.password":  userPassword,
		"ca.p12":         cap12,
		"ca.password":    caPassword,
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err = Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Enable)
	assert.Greater(t, len(config.Net.TLS.Config.Certificates), 0)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)
	assert.Greater(t, len(config.Net.TLS.Config.RootCAs.Subjects()), 0)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256), config.Net.SASL.Mechanism)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

func TestSASLSCRAM512SSL(t *testing.T) {
	cap12, err := ioutil.ReadFile("testdata/ca.p12")
	assert.Nil(t, err)

	caPassword, err := ioutil.ReadFile("testdata/ca.password")
	assert.Nil(t, err)

	userp12, err := ioutil.ReadFile("testdata/user.p12")
	assert.Nil(t, err)

	userPassword, err := ioutil.ReadFile("testdata/user.password")
	assert.Nil(t, err)

	secret := map[string][]byte{
		"protocol":       []byte("SASL_SSL"),
		"sasl.mechanism": []byte("SCRAM-SHA-512"),
		"user.p12":       userp12,
		"user.password":  userPassword,
		"ca.p12":         cap12,
		"ca.password":    caPassword,
		"user":           []byte("my-user-name"),
		"password":       []byte("my-user-password"),
	}
	config := sarama.NewConfig()

	err = Options(config, SecretData(secret))

	assert.Nil(t, err)
	assert.True(t, config.Net.TLS.Enable)
	assert.Greater(t, len(config.Net.TLS.Config.Certificates), 0)
	assert.NotNil(t, config.Net.TLS.Config.RootCAs)
	assert.Greater(t, len(config.Net.TLS.Config.RootCAs.Subjects()), 0)
	assert.True(t, config.Net.SASL.Enable)
	assert.True(t, config.Net.SASL.Handshake)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512), config.Net.SASL.Mechanism)
	assert.Equal(t, "my-user-name", config.Net.SASL.User)
	assert.Equal(t, "my-user-password", config.Net.SASL.Password)
}

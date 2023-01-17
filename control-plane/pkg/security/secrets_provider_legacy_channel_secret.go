/*
 * Copyright 2021 The Knative Authors
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
	"strconv"

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

func ResolveAuthContextFromLegacySecret(s *corev1.Secret) (*NetSpecAuthContext, error) {
	if s == nil {
		return &NetSpecAuthContext{}, nil
	}

	legacyAuth := getAuthConfigFromSecret(s)
	protocolStr, protocolContract := extractProtocol(legacyAuth)

	virtualSecret := s.DeepCopy()
	virtualSecret.Data[ProtocolKey] = []byte(protocolStr)
	if v, ok := virtualSecret.Data["sasltype"]; ok {
		virtualSecret.Data[SaslMechanismKey] = v
	}
	if v, ok := virtualSecret.Data["saslType"]; ok {
		virtualSecret.Data[SaslMechanismKey] = v
	}
	if v, ok := virtualSecret.Data["username"]; ok {
		virtualSecret.Data[SaslUserKey] = v
	}
	if v, ok := virtualSecret.Data["user"]; ok {
		virtualSecret.Data[SaslUserKey] = v
	}

	return &NetSpecAuthContext{
		VirtualSecret: virtualSecret,
		MultiSecretReference: &contract.MultiSecretReference{
			Protocol:   protocolContract,
			References: resolveReferencesFromLegacyKafkaSecret(s),
		},
	}, nil
}

func resolveReferencesFromLegacyKafkaSecret(s *corev1.Secret) []*contract.SecretReference {
	sRef := &contract.SecretReference{
		Reference: &contract.Reference{
			Uuid:      string(s.UID),
			Namespace: s.GetNamespace(),
			Name:      s.GetName(),
			Version:   s.ResourceVersion,
		},
	}

	hasFields := maybeAddKeyFieldRef(s, UserKey, contract.SecretField_USER_KEY, sRef)
	hasFields = maybeAddKeyFieldRef(s, UserCertificate, contract.SecretField_USER_CRT, sRef) || hasFields
	hasFields = maybeAddKeyFieldRef(s, CaCertificateKey, contract.SecretField_CA_CRT, sRef) || hasFields
	hasFields = maybeAddKeyFieldRef(s, SaslPasswordKey, contract.SecretField_PASSWORD, sRef) || hasFields
	// We need to support `username` and `user` since in some cases one is used over the other
	hasFields = maybeAddKeyFieldRef(s, "user", contract.SecretField_USER, sRef) || hasFields
	if _, ok := s.Data["user"]; !ok {
		hasFields = maybeAddKeyFieldRef(s, "username", contract.SecretField_USER, sRef) || hasFields
	}
	// We need to support `saslType` and `sasltype` since in some cases one is used over the other
	hasFields = maybeAddKeyFieldRef(s, "saslType", contract.SecretField_SASL_MECHANISM, sRef) || hasFields
	if _, ok := s.Data["saslType"]; !ok {
		hasFields = maybeAddKeyFieldRef(s, "sasltype", contract.SecretField_SASL_MECHANISM, sRef) || hasFields
	}

	if !hasFields {
		return nil
	}

	return []*contract.SecretReference{sRef}
}

func maybeAddKeyFieldRef(s *corev1.Secret, key string, field contract.SecretField, reference *contract.SecretReference) bool {
	if s.Data == nil {
		return false
	}

	if v, ok := s.Data[key]; ok && len(v) > 0 {
		reference.KeyFieldReferences = append(reference.KeyFieldReferences, &contract.KeyFieldReference{
			SecretKey: key,
			Field:     field,
		})
		return true
	}
	return false
}

func extractProtocol(auth *KafkaAuthConfig) (string, contract.Protocol) {
	if hasTLSEnabled(auth) && hasSASLEnabled(auth) {
		return ProtocolSASLSSL, contract.Protocol_SASL_SSL
	}
	if hasTLSEnabled(auth) {
		return ProtocolSSL, contract.Protocol_SSL
	}
	if hasSASLEnabled(auth) {
		return ProtocolSASLPlaintext, contract.Protocol_SASL_PLAINTEXT
	}
	return ProtocolPlaintext, contract.Protocol_PLAINTEXT
}

func hasTLSEnabled(auth *KafkaAuthConfig) bool {
	return auth.TLS != nil && (auth.TLS.Userkey != "" || auth.TLS.Cacert != "" || auth.TLS.Usercert != "")
}

func hasSASLEnabled(auth *KafkaAuthConfig) bool {
	return auth.SASL != nil && auth.SASL.User != "" || auth.SASL.Password != ""
}

// getAuthConfigFromSecret Looks Up And Returns Kafka Auth Config And Brokers From Provided Secret
func getAuthConfigFromSecret(secret *corev1.Secret) *KafkaAuthConfig {
	if secret == nil || secret.Data == nil {
		return nil
	}

	username := string(secret.Data[SaslUsernameKey])
	saslType := string(secret.Data[SaslType])
	var authConfig KafkaAuthConfig
	// Backwards-compatibility - Support old consolidated secret fields if present
	// (TLS data is now in the configmap, e.g. sarama.Config.Net.TLS.Config.RootPEMs)
	_, hasTlsCaCert := secret.Data[CaCertificateKey]
	_, hasTlsEnabled := secret.Data[SSLLegacyEnabled]
	if hasTlsEnabled || hasTlsCaCert {
		parseTls(secret, &authConfig)
		username = string(secret.Data[SaslUserKey])
		saslType = string(secret.Data[SaslTypeLegacy]) // old "saslType" is different than new "sasltype"
	}

	// If we don't convert the empty string to the "PLAIN" default, the client.HasSameSettings()
	// function will assume that they should be treated as differences and needlessly reconfigure
	if saslType == "" {
		saslType = sarama.SASLTypePlaintext
	}

	authConfig.SASL = &KafkaSaslConfig{
		User:     username,
		Password: string(secret.Data[SaslPasswordKey]),
		SaslType: saslType,
	}

	return &authConfig
}

// parseTls allows backward-compatibility with older consolidated channel secrets
func parseTls(secret *corev1.Secret, kafkaAuthConfig *KafkaAuthConfig) {

	// self-signed CERTs we need CA CERT, USER CERT and KEY
	if string(secret.Data[CaCertificateKey]) != "" {
		// We have a self-signed TLS cert
		tls := &KafkaTlsConfig{
			Cacert:   string(secret.Data[CaCertificateKey]),
			Usercert: string(secret.Data[UserCertificate]),
			Userkey:  string(secret.Data[UserKey]),
		}
		kafkaAuthConfig.TLS = tls
	} else {
		// Public CERTS from a proper CA do not need this,
		// we can just say `tls.enabled: true`
		tlsEnabled, err := strconv.ParseBool(string(secret.Data[SSLLegacyEnabled]))
		if err != nil {
			tlsEnabled = false
		}
		if tlsEnabled {
			// Looks like TLS is desired/enabled:
			kafkaAuthConfig.TLS = &KafkaTlsConfig{}
		}
	}
}

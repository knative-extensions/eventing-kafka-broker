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
	corev1 "k8s.io/api/core/v1"
	"knative.dev/eventing-kafka/pkg/common/client"
	legacycommonconfig "knative.dev/eventing-kafka/pkg/common/config"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

func ResolveAuthContextFromLegacySecret(s *corev1.Secret) (*NetSpecAuthContext, error) {
	if s == nil {
		return &NetSpecAuthContext{}, nil
	}

	legacyAuth := legacycommonconfig.GetAuthConfigFromSecret(s)
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

func extractProtocol(auth *client.KafkaAuthConfig) (string, contract.Protocol) {
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

func hasTLSEnabled(auth *client.KafkaAuthConfig) bool {
	return auth.TLS != nil && (auth.TLS.Userkey != "" || auth.TLS.Cacert != "" || auth.TLS.Usercert != "")
}

func hasSASLEnabled(auth *client.KafkaAuthConfig) bool {
	return auth.SASL != nil && auth.SASL.User != "" || auth.SASL.Password != ""
}

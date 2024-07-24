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
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	bindings "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1beta1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/contract"
)

// NetSpecSecretProviderFunc creates a SecretProviderFunc that creates an in-memory (virtual) secret with the format
// expected by the NewSaramaSecurityOptionFromSecret function.
func NetSpecSecretProviderFunc(authContext *NetSpecAuthContext) SecretProviderFunc {
	return func(_ context.Context, _, _ string) (*corev1.Secret, error) { return authContext.VirtualSecret, nil }
}

type NetSpecAuthContext struct {
	VirtualSecret        *corev1.Secret
	MultiSecretReference *contract.MultiSecretReference
}

// ResolveAuthContextFromNetSpec creates a NetSpecAuthContext from a provided bindings.KafkaNetSpec.
func ResolveAuthContextFromNetSpec(lister corelisters.SecretLister, namespace string, netSpec bindings.KafkaNetSpec) (*NetSpecAuthContext, error) {
	securityFields := []*securityField{
		{ref: netSpec.TLS.Cert, field: contract.SecretField_USER_CRT, virtualSecretKey: UserCertificate},
		{ref: netSpec.TLS.Key, field: contract.SecretField_USER_KEY, virtualSecretKey: UserKey},
		{ref: netSpec.TLS.CACert, field: contract.SecretField_CA_CRT, virtualSecretKey: CaCertificateKey},
		{ref: netSpec.SASL.Type, field: contract.SecretField_SASL_MECHANISM, virtualSecretKey: SaslMechanismKey},
		{ref: netSpec.SASL.User, field: contract.SecretField_USER, virtualSecretKey: SaslUserKey},
		{ref: netSpec.SASL.Password, field: contract.SecretField_PASSWORD, virtualSecretKey: SaslPasswordKey},
	}
	for _, b := range securityFields {
		if err := b.resolveSecret(lister, namespace); err != nil {
			return nil, err
		}
	}
	references, virtualSecret := toContract(securityFields)
	multiSecretReference := &contract.MultiSecretReference{
		Protocol:   getProtocolContractFromNetSpec(netSpec),
		References: references,
	}
	virtualSecret.Data[ProtocolKey] = []byte(getProtocolFromNetSpec(netSpec))

	authContext := &NetSpecAuthContext{
		VirtualSecret:        &virtualSecret,
		MultiSecretReference: multiSecretReference,
	}
	return authContext, nil
}

func toContract(securityFields []*securityField) ([]*contract.SecretReference, corev1.Secret) {
	virtualSecretData := make(map[string][]byte)
	bySecretName := make(map[string][]securityField)
	for _, f := range securityFields {
		if f.secret == nil || f.value == nil || len(f.value) == 0 {
			continue
		}
		virtualSecretData[f.virtualSecretKey] = f.value
		bySecretName[f.secret.Name] = append(bySecretName[f.secret.Name], *f)
	}

	refs := make([]*contract.SecretReference, 0, 6 /* max number of secrets */)
	names := make([]string, 0, 6)
	namespaces := make([]string, 0, 6)
	for secretName, securityFields := range bySecretName {
		keyFieldReferences := make([]*contract.KeyFieldReference, 0, len(securityFields))
		for _, f := range securityFields {
			keyFieldReferences = append(keyFieldReferences, &contract.KeyFieldReference{
				SecretKey: f.ref.SecretKeyRef.Key,
				Field:     f.field,
			})
		}
		any := securityFields[0]
		refs = append(refs, &contract.SecretReference{
			Reference: &contract.Reference{
				Uuid:      string(any.secret.GetUID()),
				Namespace: any.secret.GetNamespace(),
				Name:      secretName,
				Version:   any.secret.GetResourceVersion(),
			},
			KeyFieldReferences: keyFieldReferences,
		})
		names = append(names, any.secret.Name)
		namespaces = append(namespaces, any.secret.Namespace)
	}
	return refs, corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stableConcat(names),
			Namespace: stableConcat(namespaces),
		},
		Data: virtualSecretData,
	}
}

type securityField struct {
	ref              bindings.SecretValueFromSource
	field            contract.SecretField
	virtualSecretKey string

	secret *corev1.Secret
	value  []byte
}

func (b *securityField) resolveSecret(lister corelisters.SecretLister, namespace string) error {
	value, secret, err := resolveSecret(lister, namespace, b.ref.SecretKeyRef)
	if err != nil {
		return err
	}
	b.value = value
	b.secret = secret
	return nil
}

func getProtocolFromNetSpec(netSpec bindings.KafkaNetSpec) string {
	protocol := ProtocolPlaintext
	if netSpec.SASL.Enable && netSpec.TLS.Enable {
		protocol = ProtocolSASLSSL
	} else if netSpec.SASL.Enable {
		protocol = ProtocolSASLPlaintext
	} else if netSpec.TLS.Enable {
		protocol = ProtocolSSL
	}
	return protocol
}

func getProtocolContractFromNetSpec(netSpec bindings.KafkaNetSpec) contract.Protocol {
	protocol := contract.Protocol_PLAINTEXT
	if netSpec.SASL.Enable && netSpec.TLS.Enable {
		protocol = contract.Protocol_SASL_SSL
	} else if netSpec.SASL.Enable {
		protocol = contract.Protocol_SASL_PLAINTEXT
	} else if netSpec.TLS.Enable {
		protocol = contract.Protocol_SSL
	}
	return protocol
}

// resolveSecret resolves the secret reference
func resolveSecret(lister corelisters.SecretLister, ns string, ref *corev1.SecretKeySelector) ([]byte, *corev1.Secret, error) {
	if ref == nil || ref.Name == "" {
		return nil, nil, nil
	}
	secret, err := lister.Secrets(ns).Get(ref.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read secret %s/%s: %w", ns, ref.Name, err)
	}

	value, ok := secret.Data[ref.Key]
	if !ok || len(value) == 0 {
		return nil, nil, fmt.Errorf("missing secret key or empty secret value (%s/%s.%s)", ns, ref.Name, ref.Key)
	}
	return value, secret, nil
}

func stableConcat(elements []string) string {
	sort.SliceStable(elements, func(i, j int) bool {
		return elements[i] < elements[j]
	})

	return strings.Join(elements, "")
}

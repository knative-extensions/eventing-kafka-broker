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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
)

const (
	AuthSecretNameKey      = "auth.secret.ref.name"      /* #nosec G101 */ /* Potential hardcoded credentials (gosec) */
	AuthSecretNamespaceKey = "auth.secret.ref.namespace" /* #nosec G101 */ /* Potential hardcoded credentials (gosec) */
)

// SecretLocator locates a secret in a cluster.
type SecretLocator interface {
	// SecretName returns the secret name.
	// It returns true if the name should be used and false if should be ignored.
	SecretName() (string, bool)

	// SecretNamespace returns the secret name.
	// It returns true if the namespace should be used and false if should be ignored.
	SecretNamespace() (string, bool)
}

// SecretProviderFunc provides a secret given a namespace/name pair.
type SecretProviderFunc func(ctx context.Context, namespace, name string) (*corev1.Secret, error)

func NewSaramaSecurityOptionFromSecret(secret *corev1.Secret) kafka.ConfigOption {
	if secret == nil {
		return kafka.NoOpConfigOption
	}
	return secretData(secret.Data)
}

func Secret(ctx context.Context, config SecretLocator, secretProviderFunc SecretProviderFunc) (*corev1.Secret, error) {
	name, ok := config.SecretName()
	if !ok {
		// No auth config, will later use a no-op config option.
		return nil, nil
	}
	ns, ok := config.SecretNamespace()
	if !ok {
		// No auth config, will later use a no-op config option.
		return nil, nil
	}

	secret, err := secretProviderFunc(ctx, ns, name)
	if err != nil {
		return nil, err
	}

	return secret, nil
}

// DefaultSecretProviderFunc is a secret provider that uses the local cache for getting the secret and when the secret
// is not found it uses the kube client to check if the secret doesn't actually exist.
func DefaultSecretProviderFunc(lister corelisters.SecretLister, kc kubernetes.Interface) SecretProviderFunc {
	return func(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
		secret, err := lister.Secrets(namespace).Get(name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Check if the secret actually doesn't exist.
				secret, err = kc.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
				if err != nil {
					return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
				}
				return secret, nil
			}
			return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
		}

		return secret, nil
	}
}

// MTConfigMapSecretLocator is a SecretLocator that locates a secret using a reference in a ConfigMap.
//
// The name is taken from the data field using the key: AuthSecretNameKey.
// The namespace is taken from the data field using the key: AuthSecretNamespaceKey but if it is not defined,
// namespace of the ConfigMap is returned.
type MTConfigMapSecretLocator struct {
	*corev1.ConfigMap
}

func (cmp *MTConfigMapSecretLocator) SecretName() (string, bool) {
	if cmp.ConfigMap == nil {
		return "", false
	}
	v, ok := cmp.Data[AuthSecretNameKey]
	return v, ok
}

func (cmp *MTConfigMapSecretLocator) SecretNamespace() (string, bool) {
	if cmp.ConfigMap == nil {
		return cmp.Namespace, true
	}

	if v, ok := cmp.Data[AuthSecretNamespaceKey]; ok {
		return v, true
	}

	return cmp.Namespace, true
}

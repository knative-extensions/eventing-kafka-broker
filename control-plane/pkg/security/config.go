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

	"github.com/Shopify/sarama"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	AuthSecretNameKey = "auth.secret.ref.name" /* #nosec G101 */ /* Potential hardcoded credentials (gosec) */
)

// SecretLocator locates a secret in a cluster.
type SecretLocator interface {
	// SecretName returns the secret name.
	// It returns true if the name should be used and false if should be ignored.
	SecretName() (string, bool, error)

	// SecretNamespace returns the secret name.
	// It returns true if the namespace should be used and false if should be ignored.
	SecretNamespace() (string, bool, error)
}

// SecretProviderFunc provides a secret given a namespace/name pair.
type SecretProviderFunc func(ctx context.Context, namespace, name string) (*corev1.Secret, error)

func NewOptionFromSecret(ctx context.Context, config SecretLocator, secretProviderFunc SecretProviderFunc) (ConfigOption, *corev1.Secret, error) {

	name, ok, err := config.SecretName()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		// No auth config, return a no-op config option.
		return NoOp, nil, nil
	}
	ns, ok, err := config.SecretNamespace()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		// No auth config, return a no-op config option.
		return NoOp, nil, nil
	}

	secret, err := secretProviderFunc(ctx, ns, name)
	if err != nil {
		return nil, nil, err
	}

	return secretData(secret.Data), secret, nil
}

// DefaultSecretProviderFunc is a secret provider that uses the local cache for getting the secret and when the secret
// is not found it uses the kube client check if the secret doesn't actually exist.
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

// NoOp is a no-op ConfigOption.
func NoOp(*sarama.Config) error {
	return nil
}

// MTConfigMapSecretLocator is a SecretLocator that locates a secret using a reference in a ConfigMap.
//
// The name is take from the data field using the key: AuthSecretNameKey.
// The namespace is the same namespace of the ConfigMap.
type MTConfigMapSecretLocator struct {
	*corev1.ConfigMap
}

func (cmp *MTConfigMapSecretLocator) SecretName() (string, bool, error) {
	if cmp.ConfigMap == nil {
		return "", false, nil
	}
	v, ok := cmp.Data[AuthSecretNameKey]
	return v, ok, nil
}

func (cmp *MTConfigMapSecretLocator) SecretNamespace() (string, bool, error) {
	return cmp.Namespace, true, nil
}

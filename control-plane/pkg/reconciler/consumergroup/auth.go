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

package consumergroup

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/kafka"
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

func (r Reconciler) newAuthConfigOption(ctx context.Context, cg *kafkainternals.ConsumerGroup) (kafka.ConfigOption, error) {
	var secret *corev1.Secret
	var err error

	if hasAuthSpecAuthConfig(cg.Spec.Template.Spec.Auth) {
		secret, err = security.Secret(ctx, &AuthSpecSecretLocator{cg}, security.DefaultSecretProviderFunc(r.SecretLister, r.KubeClient))
		if err != nil {
			return nil, err
		}
	} else if hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) {
		auth, err := security.ResolveAuthContextFromNetSpec(r.SecretLister, cg.GetNamespace(), *cg.Spec.Template.Spec.Auth.NetSpec)
		if err != nil {
			return nil, err
		}
		secret, err = security.Secret(ctx, &NetSpecSecretLocator{cg}, security.NetSpecSecretProviderFunc(auth))
		if err != nil {
			return nil, fmt.Errorf("failed to get secret: %w", err)
		}
	}

	return security.NewSaramaSecurityOptionFromSecret(secret), nil
}

type NetSpecSecretLocator struct {
	*kafkainternals.ConsumerGroup
}

func (sl *NetSpecSecretLocator) SecretName() (string, bool) {
	if !hasNetSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth) {
		return "", false
	}
	// KafkaSource uses a secret provider `security.NetSpecSecretProviderFunc` that ignores name and namespace.
	// so there is no need to return a name here.
	return "", hasNetSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

func (sl *NetSpecSecretLocator) SecretNamespace() (string, bool) {
	return sl.Namespace, hasNetSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

func hasAuthSpecAuthConfig(auth *kafkainternals.Auth) bool {
	return auth != nil &&
		auth.AuthSpec != nil &&
		auth.AuthSpec.Secret != nil &&
		auth.AuthSpec.Secret.Ref != nil &&
		auth.AuthSpec.Secret.Ref.Name != ""
}

func hasNetSpecAuthConfig(auth *kafkainternals.Auth) bool {
	return auth != nil && (auth.NetSpec.TLS.Enable || auth.NetSpec.SASL.Enable) &&
		(auth.NetSpec.TLS.Cert.SecretKeyRef != nil ||
			auth.NetSpec.TLS.CACert.SecretKeyRef != nil ||
			auth.NetSpec.TLS.Key.SecretKeyRef != nil ||
			auth.NetSpec.SASL.User.SecretKeyRef != nil ||
			auth.NetSpec.SASL.Password.SecretKeyRef != nil ||
			auth.NetSpec.SASL.Type.SecretKeyRef != nil)
}

type AuthSpecSecretLocator struct {
	*kafkainternals.ConsumerGroup
}

func (sl *AuthSpecSecretLocator) SecretNamespace() (string, bool) {
	if !hasAuthSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth) {
		return "", false
	}
	return sl.ConsumerGroup.GetNamespace(), hasAuthSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

func (sl *AuthSpecSecretLocator) SecretName() (string, bool) {
	if !hasAuthSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth) {
		return "", false
	}
	name := sl.ConsumerGroup.Spec.Template.Spec.Auth.AuthSpec.Secret.Ref.Name
	return name, hasAuthSpecAuthConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

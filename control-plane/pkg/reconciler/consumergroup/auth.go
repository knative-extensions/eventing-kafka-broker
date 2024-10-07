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
	"knative.dev/eventing-kafka-broker/control-plane/pkg/security"
)

func (r *Reconciler) newAuthSecret(ctx context.Context, cg *kafkainternals.ConsumerGroup) (*corev1.Secret, error) {
	if hasSecretSpecConfig(cg.Spec.Template.Spec.Auth) {
		secret, err := security.Secret(ctx, &SecretSpecSecretLocator{cg}, security.DefaultSecretProviderFunc(r.SecretLister, r.KubeClient))
		if err != nil {
			return nil, err
		}
		return secret, nil
	}

	if hasNetSpecAuthConfig(cg.Spec.Template.Spec.Auth) {
		auth, err := security.ResolveAuthContextFromNetSpec(r.SecretLister, cg.GetNamespace(), *cg.Spec.Template.Spec.Auth.NetSpec)
		if err != nil {
			return nil, err
		}
		secret, err := security.Secret(ctx, &NetSpecSecretLocator{cg}, security.NetSpecSecretProviderFunc(auth))
		if err != nil {
			return nil, fmt.Errorf("failed to get secret: %w", err)
		}
		return secret, nil
	}

	return nil, nil
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

func hasSecretSpecConfig(auth *kafkainternals.Auth) bool {
	return auth != nil &&
		auth.SecretSpec != nil &&
		auth.SecretSpec.Ref != nil &&
		auth.SecretSpec.Ref.Name != "" &&
		auth.SecretSpec.Ref.Namespace != ""
}

func hasNetSpecAuthConfig(auth *kafkainternals.Auth) bool {
	return auth != nil &&
		auth.NetSpec != nil &&
		(auth.NetSpec.TLS.Enable || auth.NetSpec.SASL.Enable) &&
		(auth.NetSpec.TLS.Cert.SecretKeyRef != nil ||
			auth.NetSpec.TLS.CACert.SecretKeyRef != nil ||
			auth.NetSpec.TLS.Key.SecretKeyRef != nil ||
			auth.NetSpec.SASL.User.SecretKeyRef != nil ||
			auth.NetSpec.SASL.Password.SecretKeyRef != nil ||
			auth.NetSpec.SASL.Type.SecretKeyRef != nil)
}

type SecretSpecSecretLocator struct {
	*kafkainternals.ConsumerGroup
}

func (sl *SecretSpecSecretLocator) SecretNamespace() (string, bool) {
	if !hasSecretSpecConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth) {
		return "", false
	}
	namespace := sl.ConsumerGroup.Spec.Template.Spec.Auth.SecretSpec.Ref.Namespace
	return namespace, hasSecretSpecConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

func (sl *SecretSpecSecretLocator) SecretName() (string, bool) {
	if !hasSecretSpecConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth) {
		return "", false
	}
	name := sl.ConsumerGroup.Spec.Template.Spec.Auth.SecretSpec.Ref.Name
	return name, hasSecretSpecConfig(sl.ConsumerGroup.Spec.Template.Spec.Auth)
}

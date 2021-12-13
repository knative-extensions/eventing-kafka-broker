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

package consumer

import (
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internals/kafka/eventing/v1alpha1"
)

type SecretLocator struct {
	*kafkainternals.Consumer
}

func (s *SecretLocator) SecretName() (string, bool) {
	if s.hasNoAuth() {
		return "", false
	}
	return s.Consumer.Spec.Auth.AuthSpec.Secret.Ref.Name, true
}

func (s *SecretLocator) hasNoAuth() bool {
	return s.Consumer.Spec.Auth == nil ||
		s.Consumer.Spec.Auth.AuthSpec == nil ||
		s.Consumer.Spec.Auth.AuthSpec.Secret == nil ||
		s.Consumer.Spec.Auth.AuthSpec.Secret.Ref == nil ||
		s.Consumer.Spec.Auth.AuthSpec.Secret.Ref.Name == ""
}

func (s *SecretLocator) SecretNamespace() (string, bool) {
	return s.GetNamespace(), !s.hasNoAuth()
}

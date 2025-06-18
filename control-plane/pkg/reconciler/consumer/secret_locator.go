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
	kafkainternals "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/internalskafkaeventing/v1alpha1"
)

type SecretLocator struct {
	*kafkainternals.Consumer
}

func (s *SecretLocator) SecretName() (string, bool) {
	if s.hasNoSecret() {
		return "", false
	}
	return s.Spec.Auth.SecretSpec.Ref.Name, true
}

func (s *SecretLocator) hasNoSecret() bool {
	return s.Spec.Auth == nil ||
		s.Spec.Auth.SecretSpec == nil ||
		s.Spec.Auth.SecretSpec.Ref == nil ||
		s.Spec.Auth.SecretSpec.Ref.Name == "" ||
		s.Spec.Auth.SecretSpec.Ref.Namespace == ""
}

func (s *SecretLocator) SecretNamespace() (string, bool) {
	if s.hasNoSecret() {
		return "", false
	}
	return s.Spec.Auth.SecretSpec.Ref.Namespace, true
}

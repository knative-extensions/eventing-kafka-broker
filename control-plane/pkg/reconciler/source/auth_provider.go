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

package source

import (
	sources "knative.dev/eventing-kafka/pkg/apis/sources/v1beta1"
)

type SecretLocator struct {
	*sources.KafkaSource
}

func (ks *SecretLocator) SecretName() (string, bool) {
	if !hasAuthConfig(ks.KafkaSource) {
		return "", false
	}
	return getName(ks.KafkaSource), hasAuthConfig(ks.KafkaSource)
}

func (ks *SecretLocator) SecretNamespace() (string, bool) {
	return ks.Namespace, hasAuthConfig(ks.KafkaSource)
}

func getName(ks *sources.KafkaSource) string {
	return ""
}

func hasAuthConfig(ks *sources.KafkaSource) bool {
	// TODO handle secrets
	//return (ks.Spec.KafkaAuthSpec.Net.TLS.Enable || ks.Spec.KafkaAuthSpec.Net.SASL.Enable) &&
	//	(ks.Spec.KafkaAuthSpec.Net.TLS.Cert.SecretKeyRef != nil ||
	//		ks.Spec.KafkaAuthSpec.Net.SASL.User.SecretKeyRef != nil ||
	//		ks.Spec.KafkaAuthSpec.Net.SASL.Password.SecretKeyRef != nil ||
	//		ks.Spec.KafkaAuthSpec.Net.SASL.Type.SecretKeyRef != nil)
	return false
}

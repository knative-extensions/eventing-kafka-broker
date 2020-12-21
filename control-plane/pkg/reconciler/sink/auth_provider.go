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

package sink

import (
	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

type SecretLocator struct {
	*eventing.KafkaSink
}

func (ks *SecretLocator) SecretName() (string, bool, error) {
	if ks.Spec.Auth == nil || ks.Spec.Auth.Ref == nil {
		return "", false, nil
	}
	return ks.Spec.Auth.Ref.Name, true, nil
}

func (ks *SecretLocator) SecretNamespace() (string, bool, error) {
	return ks.Namespace, true, nil
}

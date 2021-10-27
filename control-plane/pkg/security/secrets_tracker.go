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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bindings "knative.dev/eventing-kafka/pkg/apis/bindings/v1beta1"
	"knative.dev/pkg/tracker"
)

// TrackNetSpecSecrets tracks all secrets referenced by a provided bindings.KafkaNetSpec.
// parent is the object that is tracking changes to those secrets.
func TrackNetSpecSecrets(secretsTracker tracker.Interface, netSpec bindings.KafkaNetSpec, parent metav1.Object) error {
	secrets := []bindings.SecretValueFromSource{
		netSpec.TLS.Key,
		netSpec.TLS.Cert,
		netSpec.TLS.CACert,
		netSpec.SASL.Password,
		netSpec.SASL.User,
		netSpec.SASL.Type,
	}
	for _, s := range secrets {
		if s.SecretKeyRef != nil && s.SecretKeyRef.Name != "" {
			ref := tracker.Reference{
				APIVersion: "v1",
				Kind:       "Secret",
				Namespace:  parent.GetNamespace(),
				Name:       parent.GetName(),
			}
			if err := secretsTracker.TrackReference(ref, parent); err != nil {
				return err
			}
		}
	}
	return nil
}

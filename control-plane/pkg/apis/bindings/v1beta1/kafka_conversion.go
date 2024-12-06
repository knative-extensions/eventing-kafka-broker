/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1beta1

import (
	"context"

	v1 "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/bindings/v1"
)

// ConvertToV1 converts v1beta1 to v1.
func (source *KafkaAuthSpec) ConvertToV1(_ context.Context) *v1.KafkaAuthSpec {
	if source == nil {
		return nil
	}
	sink := &v1.KafkaAuthSpec{
		BootstrapServers: source.BootstrapServers,
		Net: v1.KafkaNetSpec{
			SASL: v1.KafkaSASLSpec{
				Enable: source.Net.SASL.Enable,
				User: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.SASL.User.SecretKeyRef,
				},
				Password: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.SASL.Password.SecretKeyRef,
				},
				Type: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.SASL.Type.SecretKeyRef,
				},
			},
			TLS: v1.KafkaTLSSpec{
				Enable: source.Net.TLS.Enable,
				Cert: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.TLS.Cert.SecretKeyRef,
				},
				Key: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.TLS.Key.SecretKeyRef,
				},
				CACert: v1.SecretValueFromSource{
					SecretKeyRef: source.Net.TLS.CACert.SecretKeyRef,
				},
			},
		},
	}
	return sink
}

// ConvertFromV1 converts v1 to v1beta1
func (sink *KafkaAuthSpec) ConvertFromV1(source *v1.KafkaAuthSpec) {
	if source == nil {
		return
	}
	sink.BootstrapServers = source.BootstrapServers

	sink.Net.SASL.Enable = source.Net.SASL.Enable
	sink.Net.SASL.Type.SecretKeyRef = source.Net.SASL.Type.SecretKeyRef
	sink.Net.SASL.User.SecretKeyRef = source.Net.SASL.User.SecretKeyRef
	sink.Net.SASL.Password.SecretKeyRef = source.Net.SASL.Password.SecretKeyRef

	sink.Net.TLS.Enable = source.Net.TLS.Enable
	sink.Net.TLS.Key.SecretKeyRef = source.Net.TLS.Key.SecretKeyRef
	sink.Net.TLS.Cert.SecretKeyRef = source.Net.TLS.Cert.SecretKeyRef
	sink.Net.TLS.CACert.SecretKeyRef = source.Net.TLS.CACert.SecretKeyRef

	return
}

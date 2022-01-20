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

package receiver

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	eventing "knative.dev/eventing-kafka-broker/control-plane/pkg/apis/eventing/v1alpha1"
)

func TestAddress(t *testing.T) {
	host := "example.com"
	ks := &eventing.KafkaSink{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns",
			Name:      "ks",
		},
	}
	url := Address(host, ks)

	require.Equal(t, host, url.Host)
	require.Equal(t, "http", url.Scheme)
	require.Contains(t, url.Path, ks.GetNamespace())
	require.Contains(t, url.Path, ks.GetName())
}

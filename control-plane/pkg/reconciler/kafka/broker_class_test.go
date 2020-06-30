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

package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventing "knative.dev/eventing/pkg/apis/eventing/v1beta1"
)

func TestBrokerClassFilter(t *testing.T) {
	pass := BrokerClassFilter()(&metav1.ObjectMeta{
		Annotations: map[string]string{
			eventing.BrokerClassAnnotationKey: BrokerClass,
		},
	})

	assert.True(t, pass, "expected to pass filter when broker class annotation is set to "+BrokerClass)
}

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

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestKindKafkaSink(t *testing.T) {
	gk := Kind("KafkaSink")
	assert.Equal(t, SchemeGroupVersion.WithKind("KafkaSink").GroupKind(), gk)
}

func TestResourceKafkaSink(t *testing.T) {
	gr := Resource("kafkasink")
	assert.Equal(t, SchemeGroupVersion.WithResource("kafkasink").GroupResource(), gr)
}

func TestAddKnownTypes(t *testing.T) {
	err := addKnownTypes(runtime.NewScheme())
	assert.Nil(t, err)
}

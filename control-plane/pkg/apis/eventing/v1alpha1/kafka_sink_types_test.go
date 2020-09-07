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
	"k8s.io/utils/pointer"
)

func TestGetGroupVersionKind(t *testing.T) {
	gvk := (&KafkaSink{}).GetGroupVersionKind()
	assert.Equal(t, SchemeGroupVersion.WithKind("KafkaSink"), gvk)
}

func TestGetUntypedSpec(t *testing.T) {
	spec := KafkaSinkSpec{
		Topic:             "topic-name-1",
		NumPartitions:     pointer.Int32Ptr(10),
		ReplicationFactor: func(rf int16) *int16 { return &rf }(1),
		BootstrapServers:  "broker:9092",
		ContentMode:       pointer.StringPtr(ModeBinary),
	}
	ks := &KafkaSink{Spec: spec}

	assert.Equal(t, ks.GetUntypedSpec(), spec)
}

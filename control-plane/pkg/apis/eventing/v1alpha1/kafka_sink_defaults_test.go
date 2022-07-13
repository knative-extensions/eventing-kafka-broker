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

package v1alpha1

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafkaSinkSpecSetDefaults(t *testing.T) {

	defaultMode := ModeBinary

	tests := []struct {
		name string
		spec *KafkaSinkSpec
		want *KafkaSinkSpec
	}{
		{
			name: "binary content mode by default",
			spec: &KafkaSinkSpec{},
			want: &KafkaSinkSpec{
				ContentMode: &defaultMode,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tt.spec.SetDefaults(context.Background())

			assert.Equal(t, *tt.want, *tt.spec)
		})
	}
}

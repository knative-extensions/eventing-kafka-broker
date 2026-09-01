/*
 * Copyright 2025 The Knative Authors
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

package oauth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasTokenProvider(t *testing.T) {
	tests := []struct {
		name string
		data map[string][]byte
		want bool
	}{
		{
			name: "tokenProvider present",
			data: map[string][]byte{saslTokenProviderKey: []byte(mskAccessTokenProvider)},
			want: true,
		},
		{
			name: "tokenProvider present but empty still counts as set",
			data: map[string][]byte{saslTokenProviderKey: []byte("")},
			want: true,
		},
		{
			name: "tokenProvider absent",
			data: map[string][]byte{saslAWSRegion: []byte(defaultAWSRegion)},
			want: false,
		},
		{
			name: "empty data",
			data: map[string][]byte{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasTokenProvider(tt.data))
		})
	}
}

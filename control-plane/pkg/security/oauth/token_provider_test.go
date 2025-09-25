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

package security

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTokenProvider(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string][]byte
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid MSKAccessTokenProvider",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte("MSKAccessTokenProvider"),
				SaslAWSRegion:        []byte("us-west-2"),
			},
			wantErr: false,
		},
		{
			name: "valid MSKRoleAccessTokenProvider",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte("MSKRoleAccessTokenProvider"),
				SaslRoleARNKey:       []byte("arn:aws:iam::123456789012:role/test-role"),
				SaslAWSRegion:        []byte("us-west-2"),
			},
			wantErr: false,
		},
		{
			name: "missing token provider",
			data: map[string][]byte{
				SaslAWSRegion: []byte("us-west-2"),
			},
			wantErr: true,
			errMsg:  "OAUTHBEARER token provider required",
		},
		{
			name: "empty token provider",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte(""),
				SaslAWSRegion:        []byte("us-west-2"),
			},
			wantErr: true,
			errMsg:  "OAUTHBEARER token provider required",
		},
		{
			name: "unsupported token provider",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte("UnsupportedProvider"),
				SaslAWSRegion:        []byte("us-west-2"),
			},
			wantErr: true,
			errMsg:  "unsupported OAUTHBEARER token provider",
		},
		{
			name: "MSKAccessTokenProvider with missing region",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte("MSKAccessTokenProvider"),
			},
			wantErr: false, // should not error as it will use default region
		},
		{
			name: "MSKRoleAccessTokenProvider with missing role ARN",
			data: map[string][]byte{
				SaslTokenProviderKey: []byte("MSKRoleAccessTokenProvider"),
				SaslAWSRegion:        []byte("us-west-2"),
			},
			wantErr: true,
			errMsg:  "TokenProvider MSKRoleAccessTokenProvider required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the provider
			provider, err := NewTokenProvider(tt.data)

			// Check error cases
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, provider)
				return
			}

			// Check success cases
			assert.NoError(t, err)
			assert.NotNil(t, provider)
			assert.NotNil(t, provider.tokenIssuer)

			// Check the type of the token issuer
			switch string(tt.data[SaslTokenProviderKey]) {
			case "MSKAccessTokenProvider":
				assert.IsType(t, &MSKAccessTokenIssuer{}, provider.tokenIssuer)
			case "MSKRoleAccessTokenProvider":
				assert.IsType(t, &MSKRoleAccessTokenIssuer{}, provider.tokenIssuer)
			}
		})
	}
}

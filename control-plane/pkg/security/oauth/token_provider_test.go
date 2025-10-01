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

const (
	// Test-specific constants
	unsupportedProvider = "UnsupportedProvider"

	// Error messages - specific to this test file
	errorMsgTokenProviderRequired     = "OAUTHBEARER token provider required"
	errorMsgUnsupportedTokenProvider  = "unsupported OAUTHBEARER token provider"
	errorMsgRoleTokenProviderRequired = "TokenProvider MSKRoleAccessTokenProvider required"

	// Test names - specific to this test file
	testNameValidMSKAccessToken              = "valid MSKAccessTokenProvider"
	testNameValidMSKRoleAccessToken          = "valid MSKRoleAccessTokenProvider"
	testNameMissingTokenProvider             = "missing token provider"
	testNameEmptyTokenProvider               = "empty token provider"
	testNameUnsupportedTokenProvider         = "unsupported token provider"
	testNameMSKAccessTokenMissingRegion      = "MSKAccessTokenProvider with missing region"
	testNameMSKRoleAccessTokenMissingRoleARN = "MSKRoleAccessTokenProvider with missing role ARN"
)

func TestNewTokenProvider(t *testing.T) {
	tests := []struct {
		name    string
		data    map[string][]byte
		wantErr bool
		errMsg  string
	}{
		{
			name: testNameValidMSKAccessToken,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(MSKAccessTokenProvider),
				saslAWSRegion:        []byte(testRegionUsWest2),
			},
			wantErr: false,
		},
		{
			name: testNameValidMSKRoleAccessToken,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(MSKRoleAccessTokenProvider),
				saslRoleARNKey:       []byte(testRoleARN),
				saslAWSRegion:        []byte(testRegionUsWest2),
			},
			wantErr: false,
		},
		{
			name: testNameMissingTokenProvider,
			data: map[string][]byte{
				saslAWSRegion: []byte(testRegionUsWest2),
			},
			wantErr: true,
			errMsg:  errorMsgTokenProviderRequired,
		},
		{
			name: testNameEmptyTokenProvider,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(""),
				saslAWSRegion:        []byte(testRegionUsWest2),
			},
			wantErr: true,
			errMsg:  errorMsgTokenProviderRequired,
		},
		{
			name: testNameUnsupportedTokenProvider,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(unsupportedProvider),
				saslAWSRegion:        []byte(testRegionUsWest2),
			},
			wantErr: true,
			errMsg:  errorMsgUnsupportedTokenProvider,
		},
		{
			name: testNameMSKAccessTokenMissingRegion,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(MSKAccessTokenProvider),
			},
			wantErr: false, // should not error as it will use default region
		},
		{
			name: testNameMSKRoleAccessTokenMissingRoleARN,
			data: map[string][]byte{
				saslTokenProviderKey: []byte(MSKRoleAccessTokenProvider),
				saslAWSRegion:        []byte(testRegionUsWest2),
			},
			wantErr: true,
			errMsg:  errorMsgRoleTokenProviderRequired,
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
			switch string(tt.data[saslTokenProviderKey]) {
			case MSKAccessTokenProvider:
				assert.IsType(t, &mskAccessTokenIssuer{}, provider.tokenIssuer)
			case MSKRoleAccessTokenProvider:
				assert.IsType(t, &mskRoleAccessTokenIssuer{}, provider.tokenIssuer)
			}
		})
	}
}

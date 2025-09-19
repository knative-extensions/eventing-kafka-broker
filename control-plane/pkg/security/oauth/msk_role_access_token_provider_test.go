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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMSKRoleAccessTokenIssuer(t *testing.T) {
	tests := []struct {
		name        string
		data        map[string][]byte
		envVars     map[string]string
		wantRegion  string
		wantRoleARN string
		wantErr     bool
		setupEnv    func()
		cleanupEnv  func()
	}{
		{
			name: "valid configuration with region in data",
			data: map[string][]byte{
				SaslRoleARNKey: []byte("arn:aws:iam::123456789012:role/test-role"),
				SaslAWSRegion:  []byte("eu-west-1"),
			},
			wantRegion:  "eu-west-1",
			wantRoleARN: "arn:aws:iam::123456789012:role/test-role",
			setupEnv:    func() {},
			cleanupEnv:  func() {},
		},
		{
			name: "valid configuration with AWS_REGION env var",
			data: map[string][]byte{
				SaslRoleARNKey: []byte("arn:aws:iam::123456789012:role/test-role"),
			},
			envVars: map[string]string{
				"AWS_REGION": "us-west-2",
			},
			wantRegion:  "us-west-2",
			wantRoleARN: "arn:aws:iam::123456789012:role/test-role",
			setupEnv: func() {
				os.Setenv("AWS_REGION", "us-west-2")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_REGION")
			},
		},
		{
			name: "missing role ARN",
			data: map[string][]byte{
				SaslAWSRegion: []byte("eu-west-1"),
			},
			wantErr:    true,
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: "empty role ARN",
			data: map[string][]byte{
				SaslRoleARNKey: []byte(""),
				SaslAWSRegion:  []byte("eu-west-1"),
			},
			wantErr:    true,
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: "default region with valid role ARN",
			data: map[string][]byte{
				SaslRoleARNKey: []byte("arn:aws:iam::123456789012:role/test-role"),
			},
			wantRegion:  "us-east-1", // default region
			wantRoleARN: "arn:aws:iam::123456789012:role/test-role",
			setupEnv: func() {
				os.Unsetenv("AWS_REGION")
				os.Unsetenv("AWS_DEFAULT_REGION")
			},
			cleanupEnv: func() {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			tt.setupEnv()
			defer tt.cleanupEnv()

			// Create the issuer
			issuer, err := NewMSKRoleAccessTokenIssuer(tt.data)

			// Check error
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, issuer)

			// Check region and role ARN
			assert.Equal(t, tt.wantRegion, issuer.region)
			assert.Equal(t, tt.wantRoleARN, issuer.roleARN)
		})
	}
}

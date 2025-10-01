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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	// Test ARNs - specific to this test file
	testRoleARN = "arn:aws:iam::123456789012:role/test-role"

	// Test names - specific to this test file
	testNameValidConfigWithRegionInData = "valid configuration with region in data"
	testNameValidConfigWithAWSRegionEnv = "valid configuration with AWS_REGION env var"
	testNameMissingRoleARN              = "missing role ARN"
	testNameEmptyRoleARN                = "empty role ARN"
	testNameDefaultRegionWithValidRole  = "default region with valid role ARN"
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
			name: testNameValidConfigWithRegionInData,
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
				saslAWSRegion:  []byte(testRegionEuWest1),
			},
			wantRegion:  testRegionEuWest1,
			wantRoleARN: testRoleARN,
			setupEnv:    func() {},
			cleanupEnv:  func() {},
		},
		{
			name: testNameValidConfigWithAWSRegionEnv,
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
			},
			envVars: map[string]string{
				awsRegionEnvVar: testRegionUsWest2,
			},
			wantRegion:  testRegionUsWest2,
			wantRoleARN: testRoleARN,
			setupEnv: func() {
				os.Setenv(awsRegionEnvVar, testRegionUsWest2)
			},
			cleanupEnv: func() {
				os.Unsetenv(awsRegionEnvVar)
			},
		},
		{
			name: testNameMissingRoleARN,
			data: map[string][]byte{
				saslAWSRegion: []byte(testRegionEuWest1),
			},
			wantErr:    true,
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: testNameEmptyRoleARN,
			data: map[string][]byte{
				saslRoleARNKey: []byte(""),
				saslAWSRegion:  []byte(testRegionEuWest1),
			},
			wantErr:    true,
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: testNameDefaultRegionWithValidRole,
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
			},
			wantRegion:  testRegionUsEast1, // default region
			wantRoleARN: testRoleARN,
			setupEnv: func() {
				os.Unsetenv(awsRegionEnvVar)
				os.Unsetenv(awsDefaultRegionEnvVar)
			},
			cleanupEnv: func() {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup environment
			t.Setenv(awsRegionEnvVar, tt.envVars[awsRegionEnvVar])
			t.Setenv(awsDefaultRegionEnvVar, tt.envVars[awsDefaultRegionEnvVar])

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

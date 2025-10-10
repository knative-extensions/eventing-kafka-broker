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
	testRoleARN = "arn:aws:iam::123456789012:role/test-role"
)

func TestNewMSKRoleAccessTokenIssuer(t *testing.T) {
	tests := []struct {
		name        string
		data        map[string][]byte
		envVars     map[string]string
		wantRegion  string
		wantRoleARN string
		wantErr     bool
	}{
		{
			name: "valid configuration with region in data",
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
				saslAWSRegion:  []byte(testRegionEuWest1),
			},
			wantRegion:  testRegionEuWest1,
			wantRoleARN: testRoleARN,
		},
		{
			name: "valid configuration with AWS_REGION env var",
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
			},
			envVars: map[string]string{
				awsRegionEnvVar: testRegionUsWest2,
			},
			wantRegion:  testRegionUsWest2,
			wantRoleARN: testRoleARN,
		},
		{
			name: "missing role ARN",
			data: map[string][]byte{
				saslAWSRegion: []byte(testRegionEuWest1),
			},
			wantErr: true,
		},
		{
			name: "empty role ARN",
			data: map[string][]byte{
				saslRoleARNKey: []byte(""),
				saslAWSRegion:  []byte(testRegionEuWest1),
			},
			wantErr: true,
		},
		{
			name: "default region with valid role ARN",
			data: map[string][]byte{
				saslRoleARNKey: []byte(testRoleARN),
			},
			wantRegion:  testRegionUsEast1, // default region
			wantRoleARN: testRoleARN,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(awsRegionEnvVar, tt.envVars[awsRegionEnvVar])
			t.Setenv(awsDefaultRegionEnvVar, tt.envVars[awsDefaultRegionEnvVar])

			issuer, err := newMSKRoleAccessTokenIssuer(tt.data)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, issuer)
			assert.Equal(t, tt.wantRegion, issuer.region)
			assert.Equal(t, tt.wantRoleARN, issuer.roleARN)
		})
	}
}

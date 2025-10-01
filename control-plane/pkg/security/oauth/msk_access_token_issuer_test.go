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
	// Test regions
	testRegionEuWest1      = "eu-west-1"
	testRegionUsWest2      = "us-west-2"
	testRegionApSoutheast1 = "ap-southeast-1"
	testRegionUsEast1      = "us-east-1"

	// Test names
	testNameWithRegionInData        = "with region in data"
	testNameWithAWSRegionEnv        = "with AWS_REGION env var"
	testNameWithAWSDefaultRegionEnv = "with AWS_DEFAULT_REGION env var"
	testNameWithNoRegion            = "with no region specified"
	testNameEmptyRegionInData       = "empty region in data"
)

func TestNewMSKAccessTokenIssuer(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string][]byte
		envVars    map[string]string
		wantRegion string
		wantErr    bool
		setupEnv   func()
		cleanupEnv func()
	}{
		{
			name: testNameWithRegionInData,
			data: map[string][]byte{
				saslAWSRegion: []byte(testRegionEuWest1),
			},
			wantRegion: testRegionEuWest1,
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: testNameWithAWSRegionEnv,
			data: map[string][]byte{},
			envVars: map[string]string{
				awsRegionEnvVar: testRegionUsWest2,
			},
			wantRegion: testRegionUsWest2,
			setupEnv: func() {
				os.Setenv(awsRegionEnvVar, testRegionUsWest2)
			},
			cleanupEnv: func() {
				os.Unsetenv(awsRegionEnvVar)
			},
		},
		{
			name: testNameWithAWSDefaultRegionEnv,
			data: map[string][]byte{},
			envVars: map[string]string{
				awsDefaultRegionEnvVar: testRegionApSoutheast1,
			},
			wantRegion: testRegionApSoutheast1,
			setupEnv: func() {
				os.Setenv(awsDefaultRegionEnvVar, testRegionApSoutheast1)
			},
			cleanupEnv: func() {
				os.Unsetenv(awsDefaultRegionEnvVar)
			},
		},
		{
			name:       testNameWithNoRegion,
			data:       map[string][]byte{},
			wantRegion: testRegionUsEast1, // default region
			setupEnv: func() {
				os.Unsetenv(awsRegionEnvVar)
				os.Unsetenv(awsDefaultRegionEnvVar)
			},
			cleanupEnv: func() {},
		},
		{
			name: testNameEmptyRegionInData,
			data: map[string][]byte{
				saslAWSRegion: []byte(""),
			},
			wantRegion: testRegionUsEast1, // should fall back to default
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
			issuer, err := NewMSKAccessTokenIssuer(tt.data)

			// Check error
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, issuer)

			// Check region
			assert.Equal(t, tt.wantRegion, issuer.region)
		})
	}
}

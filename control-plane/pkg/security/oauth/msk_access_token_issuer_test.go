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
	testRegionEuWest1      = "eu-west-1"
	testRegionUsWest2      = "us-west-2"
	testRegionApSoutheast1 = "ap-southeast-1"
	testRegionUsEast1      = "us-east-1"
)

func TestNewMSKAccessTokenIssuer(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string][]byte
		envVars    map[string]string
		wantRegion string
	}{
		{
			name: "with region in data",
			data: map[string][]byte{
				saslAWSRegion: []byte(testRegionEuWest1),
			},
			wantRegion: testRegionEuWest1,
		},
		{
			name: "with AWS_REGION env var",
			data: map[string][]byte{},
			envVars: map[string]string{
				awsRegionEnvVar: testRegionUsWest2,
			},
			wantRegion: testRegionUsWest2,
		},
		{
			name: "with AWS_DEFAULT_REGION env var",
			data: map[string][]byte{},
			envVars: map[string]string{
				awsDefaultRegionEnvVar: testRegionApSoutheast1,
			},
			wantRegion: testRegionApSoutheast1,
		},
		{
			name:       "with no region specified",
			data:       map[string][]byte{},
			wantRegion: testRegionUsEast1, // default region
		},
		{
			name: "empty region in data",
			data: map[string][]byte{
				saslAWSRegion: []byte(""),
			},
			wantRegion: testRegionUsEast1, // should fall back to default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(awsRegionEnvVar, tt.envVars[awsRegionEnvVar])
			t.Setenv(awsDefaultRegionEnvVar, tt.envVars[awsDefaultRegionEnvVar])

			issuer, err := newMSKAccessTokenIssuer(tt.data)

			assert.NoError(t, err)
			assert.NotNil(t, issuer)
			assert.Equal(t, tt.wantRegion, issuer.region)
		})
	}
}

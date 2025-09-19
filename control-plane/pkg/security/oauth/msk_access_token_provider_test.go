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
			name: "with region in data",
			data: map[string][]byte{
				SaslAWSRegion: []byte("eu-west-1"),
			},
			wantRegion: "eu-west-1",
			setupEnv:   func() {},
			cleanupEnv: func() {},
		},
		{
			name: "with AWS_REGION env var",
			data: map[string][]byte{},
			envVars: map[string]string{
				"AWS_REGION": "us-west-2",
			},
			wantRegion: "us-west-2",
			setupEnv: func() {
				os.Setenv("AWS_REGION", "us-west-2")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_REGION")
			},
		},
		{
			name: "with AWS_DEFAULT_REGION env var",
			data: map[string][]byte{},
			envVars: map[string]string{
				"AWS_DEFAULT_REGION": "ap-southeast-1",
			},
			wantRegion: "ap-southeast-1",
			setupEnv: func() {
				os.Setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
			},
			cleanupEnv: func() {
				os.Unsetenv("AWS_DEFAULT_REGION")
			},
		},
		{
			name:       "with no region specified",
			data:       map[string][]byte{},
			wantRegion: "us-east-1", // default region
			setupEnv: func() {
				os.Unsetenv("AWS_REGION")
				os.Unsetenv("AWS_DEFAULT_REGION")
			},
			cleanupEnv: func() {},
		},
		{
			name: "empty region in data",
			data: map[string][]byte{
				SaslAWSRegion: []byte(""),
			},
			wantRegion: "us-east-1", // should fall back to default
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

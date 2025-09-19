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
)

const (
	SaslTokenProviderKey = "tokenProvider"
	SaslRoleARNKey       = "roleARN"
	SaslAWSRegion        = "awsRegion"
)

func getAWSRegion(data map[string][]byte) string {
	if region, ok := data[SaslAWSRegion]; ok && len(region) > 0 {
		return string(region)
	}
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		// Fallback to AWS_DEFAULT_REGION if AWS_REGION is not set
		awsRegion = os.Getenv("AWS_DEFAULT_REGION")
		if awsRegion == "" {
			awsRegion = "us-east-1"
		}
	}
	return awsRegion
}

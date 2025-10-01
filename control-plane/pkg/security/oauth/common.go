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
)

const (
	saslTokenProviderKey = "tokenProvider"
	saslRoleARNKey       = "roleARN"
	saslAWSRegion        = "awsRegion"

	// Environment variable names
	awsRegionEnvVar        = "AWS_REGION"
	awsDefaultRegionEnvVar = "AWS_DEFAULT_REGION"

	// Default AWS region
	defaultAWSRegion = "us-east-1"

	// Token provider types
	MSKAccessTokenProvider     = "MSKAccessTokenProvider"
	MSKRoleAccessTokenProvider = "MSKRoleAccessTokenProvider"

	// MSK IAM Auth constants
	knativeEventingUserAgent = "knative-eventing"
)

func getAWSRegion(data map[string][]byte) string {
	if region, ok := data[saslAWSRegion]; ok && len(region) > 0 {
		return string(region)
	}

	awsRegion := os.Getenv(awsRegionEnvVar)
	if awsRegion != "" {
		return awsRegion
	}

	// Fallback to AWS_DEFAULT_REGION if AWS_REGION is not set
	awsRegion = os.Getenv(awsDefaultRegionEnvVar)
	if awsRegion != "" {
		return awsRegion
	}

	return defaultAWSRegion
}

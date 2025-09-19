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
	"context"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

// MSKAccessTokenIssuer implements TokenIssuer for the MSK access token
type MSKAccessTokenIssuer struct {
	region string
}

func (m *MSKAccessTokenIssuer) IssueToken(ctx context.Context) (string, error) {
	token, _, err := signer.GenerateAuthToken(ctx, m.region)
	return token, err
}

func NewMSKAccessTokenIssuer(data map[string][]byte) (*MSKAccessTokenIssuer, error) {
	region := getAWSRegion(data)
	return &MSKAccessTokenIssuer{
		region: region,
	}, nil
}

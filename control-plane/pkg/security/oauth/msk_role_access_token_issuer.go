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
	"context"
	"fmt"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
)

// mskRoleAccessTokenIssuer implements TokenIssuer for the MSK role access token
type mskRoleAccessTokenIssuer struct {
	region  string
	roleARN string
}

func (m *mskRoleAccessTokenIssuer) issueToken(ctx context.Context) (string, error) {
	token, _, err := signer.GenerateAuthTokenFromRole(ctx, m.region, m.roleARN, knativeEventingUserAgent)
	return token, err
}

func newMSKRoleAccessTokenIssuer(data map[string][]byte) (*mskRoleAccessTokenIssuer, error) {
	roleARN, ok := data[saslRoleARNKey]
	if !ok || len(roleARN) == 0 {
		return nil, fmt.Errorf("TokenProvider MSKRoleAccessTokenProvider required (key: %s)", saslRoleARNKey)
	}
	region := getAWSRegion(data)
	return &mskRoleAccessTokenIssuer{
		region:  region,
		roleARN: string(roleARN),
	}, nil
}

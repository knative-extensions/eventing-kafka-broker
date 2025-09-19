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
	"fmt"

	"github.com/IBM/sarama"
)

// getAWSRegion retrieves the AWS region from environment variables
// with fallback logic for default region
// TokenIssuer defines the interface for generating access tokens
type TokenIssuer interface {
	IssueToken(ctx context.Context) (string, error)
}

// TokenProvider provides common functionality for OAuth token providers
type TokenProvider struct {
	tokenIssuer TokenIssuer
}

// Token implements the sarama.AccessTokenProvider interface
func (b *TokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := b.tokenIssuer.IssueToken(context.TODO())
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}

func NewTokenProvider(data map[string][]byte) (*TokenProvider, error) {
	tokenProvider, ok := data[SaslTokenProviderKey]
	if !ok || len(tokenProvider) == 0 {
		return nil, fmt.Errorf("OAUTHBEARER token provider required (key: %s)", SaslTokenProviderKey)
	}
	tokenProviderStr := string(tokenProvider)
	var tokenIssuer TokenIssuer
	var err error
	switch tokenProviderStr {
	case "MSKAccessTokenProvider":
		tokenIssuer, err = NewMSKAccessTokenIssuer(data)
	case "MSKRoleAccessTokenProvider":
		tokenIssuer, err = NewMSKRoleAccessTokenIssuer(data)
	default:
		return nil, fmt.Errorf("unsupported OAUTHBEARER token provider (key: %s), supported: MSKAccessTokenProvider, MSKRoleAccessTokenProvider", SaslTokenProviderKey)
	}
	if err != nil {
		return nil, err
	}
	return &TokenProvider{
		tokenIssuer: tokenIssuer,
	}, nil
}

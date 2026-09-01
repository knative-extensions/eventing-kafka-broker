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

	"github.com/IBM/sarama"
)

// tokenIssuer defines the interface for generating access tokens
type tokenIssuer interface {
	issueToken(ctx context.Context) (string, error)
}

// TokenProvider provides common functionality for OAuth token providers
type TokenProvider struct {
	tokenIssuer tokenIssuer
}

// Token implements the sarama.AccessTokenProvider interface
func (b *TokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := b.tokenIssuer.issueToken(context.TODO())
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}

func NewTokenProvider(data map[string][]byte) (*TokenProvider, error) {
	tokenProvider, ok := data[saslTokenProviderKey]
	if !ok || len(tokenProvider) == 0 {
		return nil, fmt.Errorf("OAUTHBEARER token provider required (key: %s)", saslTokenProviderKey)
	}
	tokenProviderStr := string(tokenProvider)
	var tokenIssuer tokenIssuer
	var err error
	switch tokenProviderStr {
	case mskAccessTokenProvider:
		tokenIssuer, err = newMSKAccessTokenIssuer(data)
	case mskRoleAccessTokenProvider:
		tokenIssuer, err = newMSKRoleAccessTokenIssuer(data)
	default:
		return nil, fmt.Errorf("unsupported OAUTHBEARER token provider (key: %s), supported: %s, %s", saslTokenProviderKey, mskAccessTokenProvider, mskRoleAccessTokenProvider)
	}
	if err != nil {
		return nil, err
	}
	return &TokenProvider{
		tokenIssuer: tokenIssuer,
	}, nil
}

// errorTokenProvider is a sarama.AccessTokenProvider that always fails with a
// descriptive error. It is used when an OAUTHBEARER secret has no tokenProvider
// (the sasl.jaas.config / sasl.login.callback.handler.class keys are consumed by
// the Java data plane only). Setting this instead of leaving TokenProvider nil
// prevents a nil-pointer panic in sarama's OAUTHBEARER auth path when the Go
// control plane attempts an admin connection, turning it into a clear reconciler
// error instead.
type errorTokenProvider struct{ msg string }

func (e *errorTokenProvider) Token() (*sarama.AccessToken, error) {
	return nil, fmt.Errorf("%s", e.msg)
}

// UnsupportedTokenProvider returns an AccessTokenProvider that always fails with
// the given message. See errorTokenProvider for rationale.
func UnsupportedTokenProvider(msg string) sarama.AccessTokenProvider {
	return &errorTokenProvider{msg: msg}
}

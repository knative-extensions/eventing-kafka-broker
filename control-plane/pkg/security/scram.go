/*
 * Copyright 2020 The Knative Authors
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
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

var (
	// sha256HashGenerator hash generator function for SCRAM conversation.
	sha256HashGenerator scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

	// sha512HashGenerator hash generator function for SCRAM conversation.
	sha512HashGenerator scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }
)

type xdgScramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgScramClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgScramClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *xdgScramClient) Done() bool {
	return x.ClientConversation.Done()
}

func sha256ScramClientGeneratorFunc() sarama.SCRAMClient {
	return &xdgScramClient{HashGeneratorFcn: sha256HashGenerator}
}

func sha512ScramClientGeneratorFunc() sarama.SCRAMClient {
	return &xdgScramClient{HashGeneratorFcn: sha512HashGenerator}
}

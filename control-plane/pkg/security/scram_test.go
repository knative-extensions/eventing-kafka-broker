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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSha256ScramClientGeneratorFunc(t *testing.T) {
	c := sha256ScramClientGeneratorFunc()
	assert.NotNil(t, c)
}

func TestSha512ScramClientGeneratorFunc(t *testing.T) {
	c := sha512ScramClientGeneratorFunc()
	assert.NotNil(t, c)
}

func TestNoChallenge(t *testing.T) {
	c := sha512ScramClientGeneratorFunc()
	assert.Nil(t, c.Begin("user", "pass", "authz"))
	assert.False(t, c.Done())
}

func TestWithChallenge(t *testing.T) {
	c := sha512ScramClientGeneratorFunc()
	assert.Nil(t, c.Begin("user", "pass", "authz"))
	_, err := c.Step("step")
	assert.Nil(t, err)
	assert.False(t, c.Done())
}

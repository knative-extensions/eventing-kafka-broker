/*
 * Copyright 2023 The Knative Authors
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

package counter_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/counter"
)

func TestCounter(t *testing.T) {
	ctx := context.Background()
	c := counter.NewExpiringCounter(ctx)

	key := "a"

	v1 := c.Inc(key)
	v2 := c.Inc(key)

	assert.Equal(t, 1, v1)
	assert.Equal(t, 2, v2)

	c.Del(key)
	v3 := c.Inc(key)
	assert.Equal(t, 1, v3)
}

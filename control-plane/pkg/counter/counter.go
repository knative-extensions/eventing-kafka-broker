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

package counter

import (
	"context"
	"time"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
)

type Counter struct {
	cache prober.Cache[string, int, struct{}]
}

func NewExpiringCounter(ctx context.Context) *Counter {
	cache := prober.NewLocalExpiringCache[string, int, struct{}](ctx, 10*time.Minute)
	return &Counter{
		cache: cache,
	}
}

func (c *Counter) Inc(uuid string) int {
	value, _ := c.cache.Get(uuid)
	c.cache.UpsertStatus(uuid, value+1, struct{}{}, func(key string, value int, arg struct{}) {})
	return value + 1
}

func (c *Counter) Del(uuid string) {
	c.cache.Expire(uuid)
}

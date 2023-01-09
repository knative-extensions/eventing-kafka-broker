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
	"sync"
	"time"

	"knative.dev/eventing-kafka-broker/control-plane/pkg/prober"
)

type Counter struct {
	counterLock sync.RWMutex
	counterMap  map[string]int

	cache prober.Cache
}

func NewExpiringCounter(ctx context.Context) *Counter {
	cache := prober.NewLocalExpiringCache(ctx, 10*time.Minute)
	return &Counter{
		cache: cache,
	}
}

func (c *Counter) Inc(uuid string) int {
	value := int(c.cache.GetStatus(uuid))
	c.cache.UpsertStatus(uuid, prober.Status(value+1), nil, func(key string, arg interface{}) {})
	return value
}

func (c *Counter) Del(uuid string) {
	c.cache.Expire(uuid)
}

/*
 * Copyright 2021 The Knative Authors
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

package prober

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const (
	// StatusReady signals that a given object is ready.
	StatusReady Status = iota
	// StatusUnknown signals that a given object is not ready and its state is unknown.
	StatusUnknown
	// StatusUnknownErr signals that a given object is not ready and its state is unknown due to
	// networking problems between control plane and data plane.
	StatusUnknownErr
	// StatusNotReady signals that a given object is not ready.
	StatusNotReady
)

// Status represents the resource status.
type Status int

func (s Status) String() string {
	switch s {
	case StatusReady:
		return "Ready"
	case StatusNotReady:
		return "NotReady"
	case StatusUnknownErr:
		return "UnknownError"
	default:
		return "Unknown"
	}
}

// Cache is a key-status store.
type Cache[K comparable, V, A interface{}] interface {
	// GetStatus retries the status associated with the given key.
	Get(key K) (V, bool)
	// UpsertStatus add or updates the status associated with the given key.
	// Once the given key expires the onExpired callback will be called passing the arg parameter.
	UpsertStatus(key K, value V, arg A, onExpired ExpirationFunc[K, V, A])
	// Expire will expire the given key.
	Expire(key K)
}

// ExpirationFunc is a callback called once an entry in the cache is expired.
type ExpirationFunc[K comparable, V, A interface{}] func(key K, value V, arg A)

type localExpiringCache[K comparable, V, A interface{}] struct {
	// mu protects targets and entries
	mu sync.RWMutex
	// targets is a map of key-pointer to an element in the entries list.
	targets map[K]*list.Element
	// entries is a doubly-linked list of all entries present in the cache.
	// This allows fast deletion of expired entries.
	entries *list.List

	expiration time.Duration

	// defaultValue is the default value returned by Get
	defaultValue V
}

type value[K comparable, V, A interface{}] struct {
	lastUpsert time.Time
	key        K
	value      V
	arg        A
	onExpired  ExpirationFunc[K, V, A]
}

func NewLocalExpiringCache[K comparable, V, A interface{}](ctx context.Context, expiration time.Duration) Cache[K, V, A] {
	var defaultValue V
	return NewLocalExpiringCacheWithDefault[K, V, A](ctx, expiration, defaultValue)
}

func NewLocalExpiringCacheWithDefault[K comparable, V, A interface{}](ctx context.Context, expiration time.Duration, defaultValue V) Cache[K, V, A] {
	c := &localExpiringCache[K, V, A]{
		mu:           sync.RWMutex{},
		targets:      make(map[K]*list.Element, 64),
		entries:      list.New().Init(),
		expiration:   expiration,
		defaultValue: defaultValue,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(expiration):
				c.removeExpiredEntries(time.Now())
			}
		}
	}()
	return c
}

func (c *localExpiringCache[K, V, A]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.targets[key]; ok {
		value := v.Value.(*value[K, V, A])
		if !c.isExpired(value, time.Now()) {
			return value.value, true
		}
	}
	return c.defaultValue, false

}

func (c *localExpiringCache[K, V, A]) UpsertStatus(key K, val V, arg A, onExpired ExpirationFunc[K, V, A]) {
	c.Expire(key)

	c.mu.Lock()
	defer c.mu.Unlock()

	value := &value[K, V, A]{value: val, lastUpsert: time.Now(), key: key, arg: arg, onExpired: onExpired}
	element := c.entries.PushBack(value)
	c.targets[key] = element
}

func (c *localExpiringCache[K, V, A]) Expire(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.targets[key]
	if !ok {
		return
	}
	c.entries.Remove(element)
	delete(c.targets, key)

	v := element.Value.(*value[K, V, A])

	v.onExpired(v.key, v.value, v.arg)
}

func (c *localExpiringCache[K, V, A]) removeExpiredEntries(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for curr := c.entries.Front(); curr != nil && c.isExpired(curr.Value.(*value[K, V, A]), now); {
		v := curr.Value.(*value[K, V, A])
		delete(c.targets, v.key)
		prev := curr
		curr = curr.Next()
		c.entries.Remove(prev)

		v.onExpired(v.key, v.value, v.arg)
	}
}

func (c *localExpiringCache[K, V, A]) isExpired(v *value[K, V, A], now time.Time) bool {
	return v.lastUpsert.Add(c.expiration).Before(now)
}

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
type Cache[K comparable, V interface{}] interface {
	// GetStatus retries the status associated with the given key.
	GetStatus(key K) Status
	// Get retrieves the status and value associated with the given key.
	Get(key K) (V, Status)
	// UpsertStatus add or updates the status associated with the given key.
	// Once the given key expires the onExpired callback will be called passing the arg parameter.
	UpsertStatus(key K, status Status, arg V, onExpired ExpirationFunc[K, V])
	// Expire will expire the given key.
	Expire(key K)
}

// ExpirationFunc is a callback called once an entry in the cache is expired.
type ExpirationFunc[K comparable, V interface{}] func(key K, arg V)

type localExpiringCache[K comparable, V interface{}] struct {
	// mu protects targets and entries
	mu sync.RWMutex
	// targets is a map of key-pointer to an element in the entries list.
	targets map[K]*list.Element
	// entries is a doubly-linked list of all entries present in the cache.
	// This allows fast deletion of expired entries.
	entries *list.List

	expiration time.Duration
}

type value[K comparable, V interface{}] struct {
	status     Status
	lastUpsert time.Time
	key        K
	arg        V
	onExpired  ExpirationFunc[K, V]
}

func NewLocalExpiringCache[K comparable, V interface{}](ctx context.Context, expiration time.Duration) Cache[K, V] {
	c := &localExpiringCache[K, V]{
		mu:         sync.RWMutex{},
		targets:    make(map[K]*list.Element, 64),
		entries:    list.New().Init(),
		expiration: expiration,
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

func (c *localExpiringCache[K, V]) Get(key K) (V, Status) {
	var defaultValue V

	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.targets[key]; ok {
		value := v.Value.(*value[K, V])
		if !c.isExpired(value, time.Now()) {
			return value.arg, value.status
		}
	}
	return defaultValue, StatusUnknown

}

func (c *localExpiringCache[K, V]) GetStatus(key K) Status {
	_, status := c.Get(key)
	return status
}

func (c *localExpiringCache[K, V]) UpsertStatus(key K, status Status, arg V, onExpired ExpirationFunc[K, V]) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.targets[key]; ok {
		delete(c.targets, key)
		c.entries.Remove(v)
	}

	value := &value[K, V]{status: status, lastUpsert: time.Now(), key: key, arg: arg, onExpired: onExpired}
	element := c.entries.PushBack(value)
	c.targets[key] = element
}

func (c *localExpiringCache[K, V]) Expire(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	element, ok := c.targets[key]
	if !ok {
		return
	}
	c.entries.Remove(element)
	delete(c.targets, key)
}

func (c *localExpiringCache[K, V]) removeExpiredEntries(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for curr := c.entries.Front(); curr != nil && c.isExpired(curr.Value.(*value[K, V]), now); {
		v := curr.Value.(*value[K, V])
		delete(c.targets, v.key)
		prev := curr
		curr = curr.Next()
		c.entries.Remove(prev)

		v.onExpired(v.key, v.arg)
	}
}

func (c *localExpiringCache[K, V]) isExpired(v *value[K, V], now time.Time) bool {
	return v.lastUpsert.Add(c.expiration).Before(now)
}

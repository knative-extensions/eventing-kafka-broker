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
	// StatusNotReady signals that a given object is not ready.
	StatusNotReady
)

// Status represents the resource status.
type Status int

// Cache is a key-status store.
type Cache interface {
	// GetStatus retries the status associated with the given key.
	GetStatus(key string) Status
	// UpsertStatus add or updates the status associated with the given key.
	// Once the given key expires the onExpired callback will be called passing the arg parameter.
	UpsertStatus(key string, status Status, arg interface{}, onExpired ExpirationFunc)
}

// ExpirationFunc is a callback called once an entry in the cache is expired.
type ExpirationFunc func(key string, arg interface{})

type localExpiringCache struct {
	// mu protects targets and entries
	mu sync.RWMutex
	// targets is a map of key-pointer to an element in the entries list.
	targets map[string]*list.Element
	// entries is a doubly-linked list of all entries present in the cache.
	// This allows fast deletion of expired entries.
	entries *list.List

	expiration time.Duration
}

type value struct {
	status     Status
	lastUpsert time.Time
	key        string
	arg        interface{}
	onExpired  ExpirationFunc
}

func NewLocalExpiringCache(ctx context.Context, expiration time.Duration) Cache {
	c := &localExpiringCache{
		mu:         sync.RWMutex{},
		targets:    make(map[string]*list.Element, 64),
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

func (c *localExpiringCache) GetStatus(key string) Status {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.targets[key]; ok {
		value := v.Value.(*value)
		if !c.isExpired(value, time.Now()) {
			return value.status
		}
	}
	return StatusUnknown
}

func (c *localExpiringCache) UpsertStatus(key string, status Status, arg interface{}, onExpired ExpirationFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.targets[key]; ok {
		delete(c.targets, key)
		c.entries.Remove(v)
	}

	value := &value{status: status, lastUpsert: time.Now(), key: key, arg: arg, onExpired: onExpired}
	element := c.entries.PushBack(value)
	c.targets[key] = element
}

func (c *localExpiringCache) removeExpiredEntries(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for curr := c.entries.Front(); curr != nil && c.isExpired(curr.Value.(*value), now); {
		v := curr.Value.(*value)
		delete(c.targets, v.key)
		prev := curr
		curr = curr.Next()
		c.entries.Remove(prev)

		v.onExpired(v.key, v.arg)
	}
}

func (c *localExpiringCache) isExpired(v *value, now time.Time) bool {
	return v.lastUpsert.Add(c.expiration).Before(now)
}

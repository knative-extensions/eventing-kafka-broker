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

package clientpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type cachePool[K comparable, V Closeable] struct {
	lock    sync.RWMutex // protects changes to the evictionList
	entries map[K]*cacheEntry[K, V]
}

type cacheEntry[K comparable, V Closeable] struct {
	lock      sync.RWMutex
	available chan *cacheValue[K, V]
	capacity  int // how many entries are currently in use. Used to make sure we close all connections when updating/removing a value for a key
}

type cacheValue[K comparable, V Closeable] struct {
	lock       sync.Mutex             // used to indicate whether a caller is using the value currently, or if it is safe to access the value
	returnChan chan *cacheValue[K, V] // this is the same channel as the parent "available" channel. Used to return the cacheValue to the available queue
	key        K
	value      V
}

type Closeable interface {
	Close() error
}

type CreateNewValue[T Closeable] func() (T, error)

func NilReturnCapacityToCache() {}

func NewLRUCache[K comparable, V Closeable](maxSize int) (*cachePool[K, V], error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be > 0")
	}

	return &cachePool[K, V]{
		entries: map[K]*cacheEntry[K, V]{},
	}, nil
}

func (c *cachePool[K, V]) Add(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) error {
	_, returnCapacity, err := c.AddAndAcquire(ctx, key, createValue, maxEntries)
	defer returnCapacity()

	if err != nil {
		return err
	}

	return nil
}

func (c *cachePool[K, V]) AddAndAcquire(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) (V, ReturnClientFunc, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var defaultValue V

	if entry, ok := c.entries[key]; ok {
		newAvailable := make(chan *cacheValue[K, V], maxEntries)
		entry.lock.Lock()
		oldAvailable := entry.available
		entry.available = newAvailable
		entry.lock.Unlock()
		// need to update all the existing entries
		var err error = nil
		j := 0
		for i := 0; i < entry.capacity; i++ {
			value := <-oldAvailable
			if j > maxEntries {
				value.lock.Lock()
				value.value.Close()
				value.lock.Unlock()
				continue
			}
			newValue, e := createValue()
			err = errors.Join(err, e)
			value.updateValue(newValue, newAvailable)
			if e == nil {
				j++
				newAvailable <- value
			}
		}
		// we failed to fully initialize all the clients, so lets fail atomically rather than be in an inbetween state
		if j != maxEntries {
			entry.lock.Lock()
			defer entry.lock.Unlock()
			for i := 0; i < entry.capacity; i++ {
				value := <-entry.available
				// technically we don't need to lock the value, as no one else can read from the channel now
				value.value.Close()

			}
			delete(c.entries, key)
			return defaultValue, NilReturnCapacityToCache, fmt.Errorf("failed to create new values for every value in the cache: %v", err)
		}

		select {
		case <-ctx.Done():
			return defaultValue, NilReturnCapacityToCache, fmt.Errorf("timed out waiting for value from the cache after updating them: %v", ctx.Err())
		case value := <-entry.available:
			value.lock.Lock()
			return value.value, value.returnToCache, nil

		}
	}

	available := make(chan *cacheValue[K, V], maxEntries)
	entry := &cacheEntry[K, V]{
		available: available,
		capacity:  maxEntries,
	}

	var err error = nil
	j := 0
	for i := 0; i < maxEntries; i++ {
		value, e := createValue()
		err = errors.Join(err, e)
		if e != nil {
			continue
		}

		cacheValue := &cacheValue[K, V]{
			returnChan: available,
			key:        key,
			value:      value,
		}
		available <- cacheValue
		j++
	}

	// we failed to fully initialize all the clients, so lets fail atomically rather than be in an inbetween state
	// if there was an error creating one of the clients, we need to close them all so that we don't leak memory
	if err != nil {
		for i := 0; i < j; i++ {
			value := <-available
			value.value.Close()
		}
		return defaultValue, NilReturnCapacityToCache, err
	}
	c.entries[key] = entry
	value := <-available
	value.lock.Lock()
	return value.value, value.returnToCache, nil
}

func (c *cachePool[K, V]) Get(ctx context.Context, key K) (V, ReturnClientFunc, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	var defaultValue V

	entry, ok := c.entries[key]
	if !ok {
		return defaultValue, NilReturnCapacityToCache, false
	}

	entry.lock.RLock()
	defer entry.lock.RUnlock()

	select {
	case <-ctx.Done():
		return defaultValue, NilReturnCapacityToCache, false
	case value := <-entry.available:
		value.lock.Lock()
		return value.value, value.returnToCache, true
	}
}

func (c *cachePool[K, V]) Keys() []K {
	c.lock.RLock()
	defer c.lock.RUnlock()

	keys := make([]K, 0, len(c.entries))

	for k := range c.entries {
		keys = append(keys, k)
	}

	return keys
}

func (cv *cacheValue[K, V]) updateValue(newValue V, newAvailable chan *cacheValue[K, V]) {
	cv.lock.Lock()
	defer cv.lock.Unlock()
	cv.value.Close()
	cv.value = newValue
	cv.returnChan = newAvailable
}

func (cv *cacheValue[K, V]) returnToCache() {
	defer cv.lock.Unlock()
	cv.returnChan <- cv
}

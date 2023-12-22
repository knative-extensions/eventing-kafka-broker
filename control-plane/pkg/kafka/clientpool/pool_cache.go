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
	"sync"
	"time"
)

type zeroSized struct{}
type CachePool[K comparable, V Closeable] struct {
	lock           sync.RWMutex // protects changes to the evictionList
	entries        map[K]*cacheEntry[K, V]
	lastChecked    time.Time
	recheckPeriod  time.Duration // how long before rechecking entries for cleanup
	expiryDuration time.Duration // how long before an entry expires
}

type cacheEntry[K comparable, V Closeable] struct {
	lock        sync.RWMutex
	available   chan *cacheValue[K, V]
	capacity    chan zeroSized
	maxCapacity int
	createValue CreateNewValue[V]
	key         K
}

type cacheValue[K comparable, V Closeable] struct {
	returnChan chan *cacheValue[K, V] // this is the same channel as the parent "available" channel. Used to return the cacheValue to the available queue
	value      V
	lastUsed   time.Time
}

type Closeable interface {
	Close() error
}

type CreateNewValue[T Closeable] func() (T, error)

func NilReturnCapacityToCache() {}

func NewLRUCache[K comparable, V Closeable](recheckPeriod, expiryDuration time.Duration) *CachePool[K, V] {
	return &CachePool[K, V]{
		entries:        map[K]*cacheEntry[K, V]{},
		lastChecked:    time.Now(),
		recheckPeriod:  recheckPeriod,
		expiryDuration: expiryDuration,
	}
}

// AddAndAcquire adds a new value if there is not already a key in the cache, otherwise it returns a boolean indicating that the value already existed.
// It also acquires one of the values for the pool. This MUST be returned by calling ReturnClientFunc when the caller is done with the returned value.
// If you want to update the value, please call the Update method instead of AddAndAcquire.
func (c *CachePool[K, V]) AddAndAcquire(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) (V, ReturnClientFunc, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var defaultValue V

	if time.Since(c.lastChecked) >= c.recheckPeriod {
		c.lastChecked = time.Now()
		go c.cleanupEntries() // do this in another goroutine so that we don't block here
	}

	if entry, ok := c.entries[key]; ok {
		value, returnValue, err := entry.getValue(ctx)
		if err != nil {
			returnValue()
			return defaultValue, NilReturnCapacityToCache, true, err
		}
		return value, returnValue, true, nil
	}

	available := make(chan *cacheValue[K, V], maxEntries)
	capacity := make(chan zeroSized, maxEntries)

	// need to fill up capacity chan initially so that we have starting capacity
	for i := 0; i < maxEntries; i++ {
		capacity <- zeroSized{}
	}

	entry := &cacheEntry[K, V]{
		available:   available,
		maxCapacity: maxEntries,
		createValue: createValue,
		capacity:    capacity,
		key:         key,
	}

	c.entries[key] = entry

	<-capacity

	value, err := entry.createCacheValue()

	if err != nil {
		capacity <- zeroSized{}
		return defaultValue, NilReturnCapacityToCache, false, err
	}

	value.acquireFromCache()
	return value.value, value.returnToCache, false, nil
}

func (c *CachePool[K, V]) Get(ctx context.Context, key K) (V, ReturnClientFunc, bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if time.Since(c.lastChecked) >= c.recheckPeriod {
		c.lastChecked = time.Now()
		go c.cleanupEntries() // do this in another goroutine so that we don't block here
	}

	var defaultValue V

	entry, ok := c.entries[key]
	if !ok {
		return defaultValue, NilReturnCapacityToCache, false, nil
	}

	value, returnValue, err := entry.getValue(ctx)
	if err != nil {
		returnValue()
		return defaultValue, NilReturnCapacityToCache, true, err
	}

	return value, returnValue, true, nil

}

func (c *CachePool[K, V]) Keys() []K {
	c.lock.RLock()
	defer c.lock.RUnlock()

	keys := make([]K, 0, len(c.entries))

	for k := range c.entries {
		keys = append(keys, k)
	}

	return keys
}

// UpdateIfExists updates the values in the entry corresponding to key if the entry exists
// This is mostly non-blocking, as new Get requests for key will return updated values even before
// all values have been updated
func (c *CachePool[K, V]) UpdateIfExists(key K, createValue CreateNewValue[V], maxEntries int) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	entry, ok := c.entries[key]
	if !ok {
		return false, nil
	}

	newAvailable := make(chan *cacheValue[K, V], maxEntries)
	newCapacity := make(chan zeroSized, maxEntries)

	entry.lock.Lock()
	availableCapacity := entry.getAvailableCapacity()
	oldAvailable := entry.available
	oldMaxCapacity := entry.maxCapacity
	entry.available = newAvailable
	entry.maxCapacity = maxEntries
	entry.capacity = newCapacity
	entry.createValue = createValue

	inUse := oldMaxCapacity - availableCapacity

	if maxEntries-inUse > 0 {
		for i := 0; i < maxEntries-inUse; i++ {
			newCapacity <- zeroSized{}
		}
	}

	entry.lock.Unlock()

	var err error
	j := 0
	for inUse > 0 {
		value := <-oldAvailable
		inUse -= 1
		if j >= maxEntries {
			// we have enough updated values now, any more would exceed the new max so just close these
			value.value.Close()
			continue
		}

		e := value.updateValue(createValue, newAvailable)
		if e != nil {
			entry.capacity <- zeroSized{} // this value is no longer in use
			err = errors.Join(err, e)
			continue
		}
		j += 1
		newAvailable <- value
	}

	if err != nil && j == 0 {
		return true, err
	}

	return true, nil
}

func (c *CachePool[K, V]) cleanupEntries() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, entry := range c.entries {
		if noneLeft := entry.cleanupValues(c.expiryDuration); noneLeft {
			delete(c.entries, entry.key)
		}
	}
}

func (cv *cacheValue[K, V]) updateValue(createNewValue CreateNewValue[V], newAvailable chan *cacheValue[K, V]) error {
	cv.value.Close()

	newValue, err := createNewValue()
	if err != nil {
		return err
	}

	cv.value = newValue
	cv.returnChan = newAvailable

	return nil
}

func (ce *cacheEntry[K, V]) getValue(ctx context.Context) (V, ReturnClientFunc, error) {
	ce.lock.RLock()
	defer ce.lock.RUnlock()

	var defaultValue V

	select {
	case <-ctx.Done():
		return defaultValue, NilReturnCapacityToCache, ctx.Err()
	case value := <-ce.available:
		value.acquireFromCache()
		return value.value, value.returnToCache, nil
	case <-ce.capacity:
		value, err := ce.createCacheValue()
		if err != nil {
			ce.capacity <- zeroSized{}
			return defaultValue, NilReturnCapacityToCache, err
		}

		value.acquireFromCache()
		return value.value, value.returnToCache, nil
	}
}

func (ce *cacheEntry[K, V]) createCacheValue() (*cacheValue[K, V], error) {
	val, err := ce.createValue()
	if err != nil {
		return nil, err
	}

	value := &cacheValue[K, V]{
		returnChan: ce.available,
		value:      val,
	}

	return value, nil

}

func (ce *cacheEntry[K, V]) cleanupValues(expiryDuration time.Duration) bool {
	ce.lock.Lock()
	defer ce.lock.Unlock()

	stillValid := ce.getValidValuesAndCleanupOthers(expiryDuration)
	for _, value := range stillValid {
		ce.available <- value
	}

	return ce.getAvailableCapacity() == ce.maxCapacity
}

func (ce *cacheEntry[K, V]) getValidValuesAndCleanupOthers(expiryDuration time.Duration) []*cacheValue[K, V] {
	stillValid := make([]*cacheValue[K, V], 0, 8)
L:
	for {
		select {
		case value := <-ce.available:
			if time.Since(value.lastUsed) >= expiryDuration {
				value.value.Close()
				ce.capacity <- zeroSized{}
			} else {
				stillValid = append(stillValid, value)
			}
		default:
			break L
		}
	}

	return stillValid
}

func (ce *cacheEntry[K, V]) getAvailableCapacity() int {
	availableCapacity := 0
L:
	for {
		select {
		case <-ce.capacity:
			availableCapacity += 1
		default:
			break L
		}
	}

	// we need to return the available capacity
	for i := 0; i < availableCapacity; i++ {
		ce.capacity <- zeroSized{}
	}

	return availableCapacity
}

func (cv *cacheValue[K, V]) returnToCache() {
	cv.returnChan <- cv
}

func (cv *cacheValue[K, V]) acquireFromCache() {
	cv.lastUsed = time.Now()
}

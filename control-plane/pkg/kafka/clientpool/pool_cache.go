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

type cachePool[K comparable, V Closeable] struct {
	lock        sync.RWMutex // protects changes to the evictionList
	entries     map[K]*cacheEntry[K, V]
	lastChecked time.Time
}

type cacheEntry[K comparable, V Closeable] struct {
	lock             sync.RWMutex
	available        chan *cacheValue[K, V]
	capacity         chan int
	maxCapacity      int
	createValue      CreateNewValue[V]
	willDeleteSoon   bool
	doNotDeleteEntry bool
	key              K
}

type cacheValue[K comparable, V Closeable] struct {
	lock       sync.Mutex             // used to indicate whether a caller is using the value currently, or if it is safe to access the value
	returnChan chan *cacheValue[K, V] // this is the same channel as the parent "available" channel. Used to return the cacheValue to the available queue
	value      V
	lastUsed   time.Time
}

type Closeable interface {
	Close() error
}

type CreateNewValue[T Closeable] func() (T, error)

func NilReturnCapacityToCache() {}

func NewLRUCache[K comparable, V Closeable]() (*cachePool[K, V], error) {
	return &cachePool[K, V]{
		entries:     map[K]*cacheEntry[K, V]{},
		lastChecked: time.Now(),
	}, nil
}

// AddAndAcquire adds a new value if there is not already a key in the cache, otherwise it returns a boolean indicating that the value already existed.
// It also acquires one of the values for the pool. This MUST be returned by calling ReturnClientFunc when the caller is done with the returned value.
// If you want to update the value, please call the Update method instead of AddAndAcquire.
func (c *cachePool[K, V]) AddAndAcquire(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) (V, ReturnClientFunc, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var defaultValue V

	if time.Since(c.lastChecked) >= 5*time.Minute {
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
	capacity := make(chan int, maxEntries)

	// need to fill up capacity chan initially so that we have starting capacity
	for i := 0; i < maxEntries; i++ {
		capacity <- 1
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
		capacity <- 1
		return defaultValue, NilReturnCapacityToCache, false, err
	}

	value.acquireFromCache()
	return value.value, value.returnToCache, false, nil
}

func (c *cachePool[K, V]) Get(ctx context.Context, key K) (V, ReturnClientFunc, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if time.Since(c.lastChecked) >= 5*time.Minute {
		c.lastChecked = time.Now()
		go c.cleanupEntries() // do this in another goroutine so that we don't block here
	}

	var defaultValue V

	entry, ok := c.entries[key]
	if !ok {
		return defaultValue, NilReturnCapacityToCache, false
	}

	value, returnValue, err := entry.getValue(ctx)
	if err != nil {
		returnValue()
		return defaultValue, NilReturnCapacityToCache, false
	}
	return value, returnValue, true

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

func (c *cachePool[K, V]) UpdateIfExists(key K, createValue CreateNewValue[V], maxEntries int) (bool, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	entry, ok := c.entries[key]
	if !ok {
		return false, nil
	}

	newAvailable := make(chan *cacheValue[K, V], maxEntries)
	newCapacity := make(chan int, maxEntries)

	entry.lock.Lock()
	availableCapacity := entry.getAvailableCapacity()
	oldAvailable := entry.available
	oldMaxCapacity := entry.maxCapacity
	entry.available = newAvailable
	entry.maxCapacity = maxEntries
	entry.capacity = newCapacity
	entry.lock.Unlock()

	inUse := oldMaxCapacity - availableCapacity

	if maxEntries > oldMaxCapacity {
		for i := 0; i < maxEntries-inUse; i++ {
			newCapacity <- 1
		}
	}

	var err error
	j := 0
	for inUse > 0 {
		value := <-oldAvailable
		inUse -= 1
		if j >= maxEntries {
			value.lock.Lock()
			value.value.Close()
			value.lock.Unlock()
			continue
		}

		e := value.updateValue(createValue, newAvailable)
		if e != nil {
			err = errors.Join(err, e)
			continue
		}

		newAvailable <- value
	}

	if err != nil {
		return true, err
	}

	return true, nil
}

func (c *cachePool[K, V]) cleanupEntries() {
	c.lock.RLock()

	toDelete := make([]*cacheEntry[K, V], 8)

	for _, entry := range c.entries {
		if noneLeft := entry.cleanupValues(); noneLeft {
			toDelete = append(toDelete, entry)
		}
	}

	c.lock.RUnlock()

	if len(toDelete) > 0 {
		c.lock.Lock()
		defer c.lock.Unlock()
		for _, entry := range toDelete {
			entry.lock.Lock()
			if !entry.doNotDeleteEntry {
				delete(c.entries, entry.key)
			}
			entry.lock.Unlock()
		}
	}
}

func (cv *cacheValue[K, V]) updateValue(createNewValue CreateNewValue[V], newAvailable chan *cacheValue[K, V]) error {
	cv.lock.Lock()
	defer cv.lock.Unlock()
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
	if ce.willDeleteSoon {
		ce.doNotDeleteEntry = true // we can write to this without acquiring the write lock as we only ever set it to true when read locked, and false when write locked
	}

	select {
	case <-ctx.Done():
		return defaultValue, NilReturnCapacityToCache, ctx.Err()
	case value := <-ce.available:
		value.acquireFromCache()
		return value.value, value.returnToCache, nil
	case <-ce.capacity:
		value, err := ce.createCacheValue()
		if err != nil {
			ce.capacity <- 1
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

func (ce *cacheEntry[K, V]) cleanupValues() bool {
	ce.lock.RLock()

	stillValid := ce.getValidValuesAndCleanupOthers()
	for _, value := range stillValid {
		ce.available <- value
	}

	if ce.getAvailableCapacity() == ce.maxCapacity {
		// all capacity is available, we should delete the entry
		ce.lock.RUnlock()
		ce.lock.Lock()
		defer ce.lock.Unlock()

		// we need to re-check this as the condition may no longer be true now that we have the write lock
		ce.willDeleteSoon = ce.getAvailableCapacity() == ce.maxCapacity
		return true
	}

	ce.lock.RUnlock()

	return false
}

func (ce *cacheEntry[K, V]) getValidValuesAndCleanupOthers() []*cacheValue[K, V] {
	stillValid := make([]*cacheValue[K, V], 8)
L:
	for {
		select {
		case value := <-ce.available:
			value.lock.Lock()
			if time.Since(value.lastUsed) >= time.Minute*30 {
				value.value.Close()
				ce.capacity <- 1
			} else {
				stillValid = append(stillValid, value)
			}
			value.lock.Unlock()
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
		ce.capacity <- 1
	}

	return availableCapacity
}

func (cv *cacheValue[K, V]) returnToCache() {
	defer cv.lock.Unlock()
	cv.returnChan <- cv
}

func (cv *cacheValue[K, V]) acquireFromCache() {
	cv.lock.Lock()
	cv.lastUsed = time.Now()
}

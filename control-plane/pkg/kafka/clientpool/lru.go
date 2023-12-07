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
	"container/list"
	"context"
	"fmt"
	"sync"
)

type LRUCache[K comparable, V Closeable] struct {
	lock         sync.Mutex // protects changes to the evictionList
	evictionList *list.List
	capacity     chan int
	maxCapacity  int
	entries      map[K]*cacheEntry[K, V]
}

type cacheEntry[K comparable, V Closeable] struct {
	lock          sync.Mutex // used to protect writes to inUse
	available     chan *cacheValue[K, V]
	entryCapacity chan int // how much capacity is remaining for the current entry
	cacheCapacity chan int // how much capacity is remaining for the whole cache
	inUse         int      // how many entries are currently in use. Used to make sure we close all connections when updating/removing a value for a key
	newValue      CreateNewValue[V]
}

type cacheValue[K comparable, V Closeable] struct {
	lock              sync.Mutex             // used to indicate whether a caller is using the value currently, or if it is safe to access the value
	returnChan        chan *cacheValue[K, V] // this is the same channel as the parent "available" channel. Used to return the cacheValue to the available queue
	evictionListEntry *list.Element
	done              bool // done indicates if this value has been scheduled for deletion/has been deleted, as the cacheValue may still be in the values channel
	key               K
	value             V
}

type Closeable interface {
	Close() error
}

type CreateNewValue[T Closeable] func() (T, error)

func NilReturnCapacityToCache() {}

func NewLRUCache[K comparable, V Closeable](maxSize int) (*LRUCache[K, V], error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be > 0")
	}

	capacity := make(chan int, maxSize)

	for i := 0; i < maxSize; i++ {
		capacity <- 1
	}

	return &LRUCache[K, V]{
		evictionList: list.New(),
		capacity:     capacity,
		maxCapacity:  maxSize,
		entries:      map[K]*cacheEntry[K, V]{},
	}, nil
}

func (c *LRUCache[K, V]) Add(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) error {
	_, returnCapacity, err := c.AddAndAcquire(ctx, key, createValue, maxEntries)
	defer returnCapacity()

	if err != nil {
		return err
	}

	return nil
}

func (c *LRUCache[K, V]) AddAndAcquire(ctx context.Context, key K, createValue CreateNewValue[V], maxEntries int) (V, ReturnClientFunc, error) {
	var defaultValue V
	if createValue == nil {
		return defaultValue, NilReturnCapacityToCache, fmt.Errorf("createValue must be provided")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// we will never be able to acquire more than maxCapacity, so to keep logic simpler this allows us to assume maxEntries <= maxCapacity always
	if maxEntries > c.maxCapacity {
		maxEntries = c.maxCapacity
	}

	// check if the element already exists
	if entry, ok := c.entries[key]; ok {
		// switch the channel being used so we can update the cacheValues as they are beind returned without worrying about concurrency
		entry.lock.Lock()
		oldAvailble := entry.available
		available := make(chan *cacheValue[K, V])
		entry.available = available
		entry.lock.Unlock()
		// we need to call the Close function on the old values before setting the new value, in case the caller wants to clean up resources
		// For example, closing a connection on the old value
		for i := 0; i < entry.inUse; i++ {
			cacheVal := <-oldAvailble
			newVal, err := createValue()
			if err != nil {
				return defaultValue, NilReturnCapacityToCache, fmt.Errorf("failed to create new value: %v", err)
			}
			cacheVal.updateValue(newVal, available)
			entry.available <- cacheVal
		}

		// get an available value
		cacheVal := <-entry.available
		cacheVal.lock.Lock()
		return cacheVal.value, cacheVal.returnToCache, nil
	}

	select {
	case <-ctx.Done():
		return defaultValue, NilReturnCapacityToCache, ctx.Err()
	case <-c.capacity: // we have capacity to make a new entry in the cache
	default:
		c.evict() // need to evict an entry before we can make a new entry in the cache
	}

	cacheVal, err := c.makeNewEntryWithValue(key, createValue, maxEntries, c.capacity)
	if err != nil {
		return defaultValue, NilReturnCapacityToCache, err
	}

	cacheVal.lock.Lock()

	return cacheVal.value, cacheVal.returnToCache, nil
}

func (c *LRUCache[K, V]) Get(ctx context.Context, key K) (V, ReturnClientFunc, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var defaultValue V

	entry, ok := c.entries[key]
	if !ok {
		return defaultValue, NilReturnCapacityToCache, false
	}

	entry.lock.Lock()
	defer entry.lock.Unlock()

	var value *cacheValue[K, V]
	found := false
	for !found {
		select {
		case <-ctx.Done():
			return defaultValue, NilReturnCapacityToCache, false
		case value = <-entry.available:
			// acquire a lock on the value so we can use it safely
			value.lock.Lock()
			if value.done {
				// don't want to use this value, it is already done, so just unlock. It will be cleaned up by GC after this.
				value.lock.Unlock()
			} else {
				// we found a valid entry, let's use it. We need to move it to the front of the list to maintain the LRU guarantee
				c.evictionList.MoveToFront(value.evictionListEntry)
				found = true
			}
		case <-entry.entryCapacity:
			// there is capacity in the entry, let's make sure we have capacity in the cache to make a new entry
			select {
			case <-c.capacity:
			default:
				c.evict()
			}

			// make the new value in the cache
			var err error
			value, err = entry.makeValue(key, entry.available)
			if err != nil {
				return defaultValue, NilReturnCapacityToCache, false
			}

			value.evictionListEntry = c.evictionList.PushFront(value)

			value.lock.Lock()

			found = true
		}

	}

	return value.value, value.returnToCache, true
}

func (c *LRUCache[K, V]) Keys() []K {
	c.lock.Lock()
	defer c.lock.Unlock()

	keys := make([]K, 0, len(c.entries))

	for k := range c.entries {
		keys = append(keys, k)
	}

	return keys
}

func (c *LRUCache[K, V]) evict() {
	evict := c.evictionList.Back()
	evictValue := evict.Value.(*cacheValue[K, V])
	evictyEntry := c.entries[evictValue.key]

	// it's okay if when removeValueKeepingCapacity is called this isn't the last value in the LRU cache anymore
	// what's important is just that it was oldest recently, and as such is probable not in use or won't be soon
	c.removeValueKeepingCapacity(evictValue, evictyEntry)
}

// removeValueKeepingCapacity removes a value from the cache, but does not return capacity to the cache. It is the caller's responsibility
// to return 1 capacity when appropriate. The locking seems slightly more complicated than it would if we deferred all unlocks,
// however we lock and unlock manually to ensure that only one of value.lock and entry.lock are held at the same time to ensure no deadlocks
func (c *LRUCache[K, V]) removeValueKeepingCapacity(value *cacheValue[K, V], entry *cacheEntry[K, V]) {

	// acquire the valueLock so that we know no one is using this value and can safely close it
	value.lock.Lock()
	value.value.Close()
	// this value may stay in a channel for a while before it is read, so switch the reference to the value to an emptry struct
	// so that the value can be garbage collected
	var emptyValue V
	value.value = emptyValue
	// set the done variable so that if this entry is currently in an available channel it will not be used
	value.done = true
	// we need to unlock here rather than defer the unlock to prevent deadlocks
	value.lock.Unlock()

	entry.lock.Lock()
	defer entry.lock.Unlock()
	entry.inUse -= 1

	// no more values for this entry, let's remove it from the map and close the associated channels
	if entry.inUse == 0 {
		close(entry.available)
		close(entry.entryCapacity)
		// let's unlock here so that we can guarantee no deadlocks
		delete(c.entries, value.key)
		return
	}

	// return capacity to the entry
	entry.entryCapacity <- 1
}

func (c *LRUCache[K, V]) makeNewEntryWithValue(key K, makeNewValue CreateNewValue[V], maxEntries int, cacheCapacity chan int) (*cacheValue[K, V], error) {
	available := make(chan *cacheValue[K, V], maxEntries)
	entryCapacity := make(chan int, maxEntries)

	for i := 0; i < maxEntries; i++ {
		entryCapacity <- 1
	}

	entry := &cacheEntry[K, V]{
		available:     available,
		entryCapacity: entryCapacity,
		cacheCapacity: cacheCapacity,
		inUse:         0,
		newValue:      makeNewValue,
	}

	// remove one from the capacity before creating a value to put in this entry
	<-entryCapacity

	cacheVal, err := entry.makeValue(key, available)
	if err != nil {
		close(available)
		close(entryCapacity)
		return nil, err
	}

	cacheVal.evictionListEntry = c.evictionList.PushFront(cacheVal)

	c.entries[key] = entry

	return cacheVal, nil
}

func (cv *cacheValue[K, V]) updateValue(newValue V, newChan chan *cacheValue[K, V]) {
	cv.lock.Lock()
	defer cv.lock.Unlock()
	cv.value.Close()
	cv.value = newValue
	cv.returnChan = newChan
}

func (cv *cacheValue[K, V]) returnToCache() {
	defer cv.lock.Unlock()
	cv.returnChan <- cv
}

func (ce *cacheEntry[K, V]) makeValue(key K, available chan *cacheValue[K, V]) (*cacheValue[K, V], error) {
	ce.inUse++
	value, err := ce.newValue()
	if err != nil {
		return nil, err
	}
	cacheVal := &cacheValue[K, V]{
		key:        key,
		returnChan: available,
		value:      value,
		done:       false,
	}

	return cacheVal, nil
}

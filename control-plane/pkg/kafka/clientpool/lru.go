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

type LRUCache struct {
	lock         sync.Mutex // protects changes to the evictionList
	evictionList *list.List
	capacity     chan int
	entries      map[interface{}]*cacheEntry
}

type cacheEntry struct {
	lock          sync.Mutex // used to protect writes to inUse
	available     chan *cacheValue
	entryCapacity chan int // how much capacity is remaining for the current entry
	cacheCapacity chan int // how much capacity is remaining for the whole cache
	inUse         int      // how many entries are currently in use. Used to make sure we close all connections when updating/removing a value for a key
	newValue      CreateNewValue
}

type cacheValue struct {
	lock              sync.Mutex       // used to indicate whether a caller is using the value currently, or if it is safe to access the value
	returnChan        chan *cacheValue // this is the same channel as the parent "available" channel. Used to return the cacheValue to the available queue
	evictionListEntry *list.Element
	done              bool // done indicates if this value has been scheduled for deletion/has been deleted, as the cacheValue may still be in the values channel
	key               interface{}
	value             Closeable
}

type Closeable interface {
	Close() error
}

type emptyCloseable struct{}

func (e emptyCloseable) Close() error { return nil }

type CreateNewValue func() (Closeable, error)

func NilReturnCapacityToCache() {}

func NewLRUCache(maxSize int) (*LRUCache, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be > 0")
	}

	capacity := make(chan int, maxSize)

	for i := 0; i < maxSize; i++ {
		capacity <- 1
	}

	return &LRUCache{
		evictionList: list.New(),
		capacity:     capacity,
		entries:      map[interface{}]*cacheEntry{},
	}, nil
}

func (c *LRUCache) Add(ctx context.Context, key interface{}, createValue CreateNewValue, maxEntries int) error {
	_, returnCapacity, err := c.AddAndAcquire(ctx, key, createValue, maxEntries)
	defer returnCapacity()

	if err != nil {
		return err
	}

	return nil
}

func (c *LRUCache) AddAndAcquire(ctx context.Context, key interface{}, createValue CreateNewValue, maxEntries int) (Closeable, ReturnClientFunc, error) {
	if createValue == nil {
		return nil, NilReturnCapacityToCache, fmt.Errorf("createValue must be provided")
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// check if the element already exists
	if entry, ok := c.entries[key]; ok {
		// switch the channel being used so we can update the cacheValues as they are beind returned without worrying about concurrency
		entry.lock.Lock()
		oldAvailble := entry.available
		available := make(chan *cacheValue)
		entry.available = available
		entry.lock.Unlock()
		// we need to call the Close function on the old values before setting the new value, in case the caller wants to clean up resources
		// For example, closing a connection on the old value
		for i := 0; i < entry.inUse; i++ {
			cacheVal := <-oldAvailble
			newVal, err := createValue()
			if err != nil {
				return nil, NilReturnCapacityToCache, fmt.Errorf("failed to create new value: %v", err)
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
		return nil, NilReturnCapacityToCache, ctx.Err()
	case <-c.capacity: // we have capacity to make a new entry in the cache
	default:
		c.evict() // need to evict an entry before we can make a new entry in the cache
	}

	cacheVal, err := c.makeNewEntryWithValue(key, createValue, maxEntries, c.capacity)
	if err != nil {
		return nil, NilReturnCapacityToCache, err
	}

	cacheVal.lock.Lock()

	return cacheVal.value, cacheVal.returnToCache, nil
}

func (c *LRUCache) Get(ctx context.Context, key interface{}) (Closeable, ReturnClientFunc, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		return nil, NilReturnCapacityToCache, false
	}

	entry.lock.Lock()
	defer entry.lock.Unlock()

	var value *cacheValue
	found := false
	for !found {
		select {
		case <-ctx.Done():
			return nil, NilReturnCapacityToCache, false
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
				return nil, NilReturnCapacityToCache, false
			}

			value.evictionListEntry = c.evictionList.PushFront(value)

			value.lock.Lock()

			found = true
		}

	}

	return value.value, value.returnToCache, true
}

func (c *LRUCache) Keys() []interface{} {
	c.lock.Lock()
	defer c.lock.Unlock()

	keys := make([]interface{}, 0, len(c.entries))

	for k := range c.entries {
		keys = append(keys, k)
	}

	return keys
}

func (c *LRUCache) evict() {
	evict := c.evictionList.Back()
	evictValue := evict.Value.(*cacheValue)
	evictyEntry := c.entries[evictValue.key]

	// it's okay if when removeValueKeepingCapacity is called this isn't the last value in the LRU cache anymore
	// what's important is just that it was oldest recently, and as such is probable not in use or won't be soon
	c.removeValueKeepingCapacity(evictValue, evictyEntry)
}

// removeValueKeepingCapacity removes a value from the cache, but does not return capacity to the cache. It is the caller's responsibility
// to return 1 capacity when appropriate. The locking seems slightly more complicated than it would if we deferred all unlocks,
// however we lock and unlock manually to ensure that only one of value.lock and entry.lock are held at the same time to ensure no deadlocks
func (c *LRUCache) removeValueKeepingCapacity(value *cacheValue, entry *cacheEntry) {

	// acquire the valueLock so that we know no one is using this value and can safely close it
	value.lock.Lock()
	value.value.Close()
	// this value may stay in a channel for a while before it is read, so switch the reference to the value to an emptry struct
	// so that the value can be garbage collected
	value.value = &emptyCloseable{}
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

func (c *LRUCache) makeNewEntryWithValue(key interface{}, makeNewValue CreateNewValue, maxEntries int, cacheCapacity chan int) (*cacheValue, error) {
	available := make(chan *cacheValue, maxEntries)
	entryCapacity := make(chan int, maxEntries)

	for i := 0; i < maxEntries; i++ {
		entryCapacity <- 1
	}

	entry := &cacheEntry{
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

func (cv *cacheValue) updateValue(newValue Closeable, newChan chan *cacheValue) {
	cv.lock.Lock()
	defer cv.lock.Unlock()
	cv.value.Close()
	cv.value = newValue
	cv.returnChan = newChan
}

func (cv *cacheValue) returnToCache() {
	defer cv.lock.Unlock()
	if !cv.done {
		cv.returnChan <- cv
	}
}

func (ce *cacheEntry) makeValue(key interface{}, available chan *cacheValue) (*cacheValue, error) {
	ce.inUse++
	value, err := ce.newValue()
	if err != nil {
		return nil, err
	}
	cacheVal := &cacheValue{
		key:        key,
		returnChan: available,
		value:      value,
		done:       false,
	}

	return cacheVal, nil
}

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

	"golang.org/x/sync/semaphore"
)

type cacheEntry struct {
	key               interface{}
	value             interface{}
	capacity          semaphore.Weighted
	evictionListEntry *list.Element
	replicaListEntry  *list.Element
	makeNewReplica    CreateNewReplica
}

// A ReplicatingLRUCache is an LRU cache which makes a new replica of an entry when a Get request is made for an entry and all
// of the replicas of the entry are used to their full entryCapacity.
type ReplicatingLRUCache struct {
	lock          sync.Mutex
	maxSize       int
	entryCapacity int64
	evictionList  *list.List
	entries       map[interface{}]*list.List
	cleanupFunc   func(key, val interface{})
}

type CreateNewReplica func() (interface{}, error)
type CleanupFunc func(key, val interface{})

func NilReturnCapacityToCache() {}

func NewReplicatingLRUCacheWithCleanupFunc(maxSize int, entryCapacity int64, cleanupFunc CleanupFunc) (*ReplicatingLRUCache, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be > 0")
	}
	if entryCapacity <= 0 {
		return nil, fmt.Errorf("entryCapacity must be > 0")
	}

	if cleanupFunc == nil {
		cleanupFunc = func(_, _ interface{}) {}
	}

	return &ReplicatingLRUCache{
		maxSize:       maxSize,
		entryCapacity: entryCapacity,
		evictionList:  list.New(),
		entries:       map[interface{}]*list.List{},
		cleanupFunc:   cleanupFunc,
	}, nil
}

func (c *ReplicatingLRUCache) Add(ctx context.Context, key interface{}, value interface{}, createNewReplica CreateNewReplica) error {
	returnFunc, err := c.AddAndAcquire(ctx, key, value, createNewReplica)
	returnFunc()
	if err != nil {
		return err
	}
	return nil
}

func (c *ReplicatingLRUCache) AddAndAcquire(ctx context.Context, key interface{}, value interface{}, createNewReplica CreateNewReplica) (ReturnClientFunc, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if createNewReplica == nil {
		createNewReplica = func() (interface{}, error) { return nil, nil }
	}

	// check if the element already exists
	if oldElements, ok := c.entries[key]; ok {
		// we need to call the cleanup function on the old values before setting the new value, in case the caller wants to clean up resources
		// For example, closing a connection on the old value
		for oldElement := oldElements.Front(); oldElement != nil; oldElement = oldElement.Next() {
			oldValue := oldElement.Value.(*cacheEntry)
			err := oldValue.capacity.Acquire(ctx, c.entryCapacity)
			if err != nil {
				return nil, err
			}
			c.cleanupFunc(key, oldValue.value)

			// we are updating the value, so mark it as "new" for the LRU cache
			c.evictionList.MoveToFront(oldElement)
			oldValue.value = value
			oldValue.makeNewReplica = createNewReplica

			oldValue.capacity.Release(c.entryCapacity)
		}

		// let's acquire one of the newly updated values for the current caller
		value := oldElements.Front().Value.(*cacheEntry)
		err := value.capacity.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		return makeReturnCapacityToCacheFunc(value), nil
	}

	c.evictIfAtMaxCapacity(ctx)

	entry := c.makeNewEntry(key, value, createNewReplica)
	err := entry.capacity.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}

	return makeReturnCapacityToCacheFunc(entry), nil
}

func (c *ReplicatingLRUCache) Get(ctx context.Context, key interface{}) (interface{}, ReturnClientFunc, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	replicas, ok := c.entries[key]
	if !ok {
		return nil, NilReturnCapacityToCache, false
	}

	count := 0
	for element := replicas.Front(); element != nil; element = element.Next() {
		count++
		entry := element.Value.(*cacheEntry)
		if entry.capacity.TryAcquire(1) {
			c.evictionList.MoveToFront(entry.evictionListEntry)
			return entry.value, makeReturnCapacityToCacheFunc(entry), true
		}
	}

	// check if the entire cache capacity is being used for this entry
	// if so, wait on the least recently used entry to become available
	if count == c.maxSize {
		entry := c.evictionList.Back().Value.(*cacheEntry)
		err := entry.capacity.Acquire(ctx, 1)
		if err != nil {
			return nil, NilReturnCapacityToCache, false
		}

		c.evictionList.MoveToFront(entry.evictionListEntry)
		return entry.value, makeReturnCapacityToCacheFunc(entry), true
	}

	// all replicas are fully used, make a new one
	c.evictIfAtMaxCapacity(ctx)

	makeNewReplica := replicas.Back().Value.(*cacheEntry).makeNewReplica
	newValue, err := makeNewReplica()
	// failed to make a new replica, use an existing replica once it becomes available
	if err != nil {
		entry := replicas.Back().Value.(*cacheEntry)
		err = entry.capacity.Acquire(ctx, 1)
		if err != nil {
			return nil, NilReturnCapacityToCache, false
		}
		return entry.value, makeReturnCapacityToCacheFunc(entry), true
	}

	entry := c.makeNewEntry(key, newValue, makeNewReplica)
	err = entry.capacity.Acquire(ctx, 1)
	if err != nil {
		return nil, NilReturnCapacityToCache, false
	}
	return entry.value, makeReturnCapacityToCacheFunc(entry), true
}

func (c *ReplicatingLRUCache) Remove(ctx context.Context, key interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	replicas, ok := c.entries[key]
	if !ok {
		return nil
	}

	for replica := replicas.Front(); replica != nil; replica = replica.Next() {
		entry := replica.Value.(*cacheEntry)

		err := entry.capacity.Acquire(ctx, c.entryCapacity)
		if err != nil {
			return fmt.Errorf("error aquiring all the capacity of replica back: %v", err)
		}

		c.cleanupFunc(entry.key, entry.value)
		c.evictionList.Remove(entry.evictionListEntry)
	}

	delete(c.entries, key)

	return nil
}

func (c *ReplicatingLRUCache) evictIfAtMaxCapacity(ctx context.Context) {
	if c.evictionList.Len() >= c.maxSize {
		toEvictValue := c.evictionList.Back().Value.(*cacheEntry)

		toEvictValue.capacity.Acquire(ctx, c.entryCapacity)

		c.cleanupFunc(toEvictValue.key, toEvictValue.value)

		c.evictionList.Remove(toEvictValue.evictionListEntry)

		replicaList := c.entries[toEvictValue.key]
		replicaList.Remove(toEvictValue.replicaListEntry)
		if replicaList.Len() == 0 {
			delete(c.entries, toEvictValue.key)
		}
	}
}

func (c *ReplicatingLRUCache) makeNewEntry(key, value interface{}, makeNewReplica CreateNewReplica) *cacheEntry {
	entry := &cacheEntry{
		key:            key,
		value:          value,
		capacity:       *semaphore.NewWeighted(c.entryCapacity),
		makeNewReplica: makeNewReplica,
	}

	element := c.evictionList.PushFront(entry)
	entry.evictionListEntry = element

	var entryReplicas *list.List
	var ok bool
	if entryReplicas, ok = c.entries[key]; !ok {
		entryReplicas = list.New()
	}
	replicaElement := entryReplicas.PushBack(entry)
	entry.replicaListEntry = replicaElement

	c.entries[key] = entryReplicas

	return entry
}

func makeReturnCapacityToCacheFunc(entry *cacheEntry) ReturnClientFunc {
	return func() {
		entry.capacity.Release(1)
	}
}

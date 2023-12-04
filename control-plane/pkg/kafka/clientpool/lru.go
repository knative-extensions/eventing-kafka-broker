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
	replicaNum        int
	evictionListEntry *list.Element
	replicaListEntry  *list.Element
	makeNewReplica    func() interface{}
}

// A ReplicatingLRUCache is an LRU cache which makes a new replica of an entry when a Get request is made for an entry and all
// of the replicas of the entry are used to their full entryCapacity.
type ReplicatingLRUCache struct {
	lock          sync.Mutex
	maxSize       int
	entryCapacity int64
	evictionList  *list.List
	entries       map[interface{}]*list.List
	cleanupFunc   func(key, val interface{}, replicaNum int)
}

type ReturnCapacityToCache func()
type CreateNewReplica func() interface{}

func NilReturnCapacityToCache() {}

func NewReplicatingLRUCacheWithCleanupFunc(maxSize int, entryCapacity int64, cleanupFunc func(interface{}, interface{}, int)) (*ReplicatingLRUCache, error) {
	if maxSize <= 0 {
		return nil, fmt.Errorf("maxSize must be > 0")
	}
	if entryCapacity <= 0 {
		return nil, fmt.Errorf("entryCapacity must be > 0")
	}

	if cleanupFunc == nil {
		cleanupFunc = func(_, _ interface{}, _ int) {}
	}

	return &ReplicatingLRUCache{
		maxSize:       maxSize,
		entryCapacity: entryCapacity,
		evictionList:  list.New(),
		entries:       map[interface{}]*list.List{},
		cleanupFunc:   cleanupFunc,
	}, nil
}

func (c *ReplicatingLRUCache) Add(ctx context.Context, key interface{}, value interface{}, createNewReplica CreateNewReplica) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if createNewReplica == nil {
		createNewReplica = func() interface{} { return nil }
	}

	// check if the element already exists
	if oldElements, ok := c.entries[key]; ok {
		// we need to call the cleanup function on the old values before setting the new value, in case the caller wants to clean up resources
		// For example, closing a connection on the old value
		for oldElement := oldElements.Front(); oldElement != nil; oldElement = oldElement.Next() {
			oldValue := oldElement.Value.(*cacheEntry)
			c.cleanupFunc(key, oldValue.value, oldValue.replicaNum)
			// we are updating the value, so mark it as "new" for the LRU cache
			c.evictionList.MoveToFront(oldElement)
			oldValue.value = value
			oldValue.makeNewReplica = createNewReplica
		}
		return
	}

	c.evictIfAtMaxCapacity(ctx)

	c.makeNewEntry(key, value, 0, createNewReplica)
}

func (c *ReplicatingLRUCache) Get(ctx context.Context, key interface{}) (interface{}, ReturnCapacityToCache, bool) {
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
	entry := c.makeNewEntry(key, makeNewReplica(), count, makeNewReplica)
	err := entry.capacity.Acquire(ctx, 1)
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
			return fmt.Errorf("error aquiring all the capacity of replica %d back: %v", entry.replicaNum, err)
		}

		c.cleanupFunc(entry.key, entry.value, entry.replicaNum)
		c.evictionList.Remove(entry.evictionListEntry)
	}

	delete(c.entries, key)

	return nil
}

func (c *ReplicatingLRUCache) evictIfAtMaxCapacity(ctx context.Context) {
	if c.evictionList.Len() >= c.maxSize {
		toEvictValue := c.evictionList.Back().Value.(*cacheEntry)

		toEvictValue.capacity.Acquire(ctx, c.entryCapacity)

		c.cleanupFunc(toEvictValue.key, toEvictValue.value, toEvictValue.replicaNum)

		c.evictionList.Remove(toEvictValue.evictionListEntry)

		replicaList := c.entries[toEvictValue.key]
		replicaList.Remove(toEvictValue.replicaListEntry)
		if replicaList.Len() == 0 {
			delete(c.entries, toEvictValue.key)
		}
	}
}

func (c *ReplicatingLRUCache) makeNewEntry(key, value interface{}, replicaNum int, makeNewReplica func() interface{}) *cacheEntry {
	entry := &cacheEntry{
		key:            key,
		value:          value,
		capacity:       *semaphore.NewWeighted(c.entryCapacity),
		replicaNum:     replicaNum,
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

func makeReturnCapacityToCacheFunc(entry *cacheEntry) ReturnCapacityToCache {
	return func() {
		entry.capacity.Release(1)
	}
}

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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddValuesToCache(t *testing.T) {
	evicted := []int{}
	cache, err := NewReplicatingLRUCacheWithCleanupFunc(3, 1, func(key, val interface{}) {
		keyInt := key.(int)
		valInt := val.(int)
		assert.Equal(t, keyInt, valInt, "should have correct value for the key")
		evicted = append(evicted, keyInt)
	})
	assert.NoError(t, err, "creating the cache should not return an error")

	cache.Add(context.TODO(), 1, 1, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	cache.Add(context.TODO(), 2, 2, func() (interface{}, error) { return 2, nil })
	assert.Contains(t, cache.entries, 2, "should contain the newly added key")

	cache.Add(context.TODO(), 3, 3, func() (interface{}, error) { return 3, nil })
	assert.Contains(t, cache.entries, 3, "should contain the newly added key")

	cache.Add(context.TODO(), 4, 4, func() (interface{}, error) { return 4, nil })
	assert.Contains(t, cache.entries, 4, "should contain the newly added key")
	assert.NotContains(t, cache.entries, 1, "should not contain the first key added")
	assert.Contains(t, evicted, 1, "should contain the evicted key")
}

func TestGrowsReplicas(t *testing.T) {
	cache, err := NewReplicatingLRUCacheWithCleanupFunc(3, 1, func(_, _ interface{}) {})
	assert.NoError(t, err, "creating the cache should not return an error")

	cache.Add(context.TODO(), 1, 1, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	entry, returnFirstEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	entry, returnSecondEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	returnFirstEntry()

	entry, returnThirdEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	count := 0
	for entry := cache.evictionList.Front(); entry != nil; entry = entry.Next() {
		val := entry.Value.(*cacheEntry)
		assert.Equal(t, 1, val.key.(int), "each replica should have the correct key")
		count++
	}

	assert.Equal(t, 2, count, "should have the same number of entries in the cache as expected")

	entry, returnFourthEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	count = 0
	for entry := cache.evictionList.Front(); entry != nil; entry = entry.Next() {
		val := entry.Value.(*cacheEntry)
		assert.Equal(t, 1, val.key.(int), "each replica should have the correct key")
		count++
	}
	assert.Equal(t, 3, count, "should have the same number of entries in the cache as expected")

	returnSecondEntry()
	returnThirdEntry()
	returnFourthEntry()

}

func TestReplicasWithCapacityOver1(t *testing.T) {
	cache, err := NewReplicatingLRUCacheWithCleanupFunc(3, 2, func(_, _ interface{}) {})
	assert.NoError(t, err, "creating the cache should not return an error")

	cache.Add(context.TODO(), 1, 1, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	entry, returnFirstEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	entry, returnSecondEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	count := 0
	for entry := cache.evictionList.Front(); entry != nil; entry = entry.Next() {
		val := entry.Value.(*cacheEntry)
		assert.Equal(t, 1, val.key.(int), "each replica should have the correct key")
		count++
	}

	assert.Equal(t, 1, count, "should only have one entry shared between to requests, because capacity is 2")

	entry, returnThirdEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	entry, returnFourthEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	returnFirstEntry()

	entry, returnFifthEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	count = 0

	for entry := cache.evictionList.Front(); entry != nil; entry = entry.Next() {
		val := entry.Value.(*cacheEntry)
		assert.Equal(t, 1, val.key.(int), "each replica should have the correct key")
		count++
	}
	assert.Equal(t, 2, count, "should have the same number of entries in the cache as expected")

	returnSecondEntry()
	returnThirdEntry()
	returnFourthEntry()
	returnFifthEntry()
}

func TestInvalidCacheSize(t *testing.T) {
	_, err := NewReplicatingLRUCacheWithCleanupFunc(0, 2, func(_, _ interface{}) {})
	assert.Error(t, err, "the created cache should error with a size of 0")

	_, err = NewReplicatingLRUCacheWithCleanupFunc(-1, 2, func(_, _ interface{}) {})
	assert.Error(t, err, "the created cache should error with a size less than 0")
}

func TestInvalidEntryCapacity(t *testing.T) {
	_, err := NewReplicatingLRUCacheWithCleanupFunc(1, 0, func(_, _ interface{}) {})
	assert.Error(t, err, "the created cache should error with an entry capacity of 0")

	_, err = NewReplicatingLRUCacheWithCleanupFunc(1, -1, func(_, _ interface{}) {})
	assert.Error(t, err, "the created cache should error with an entry capacity less than 0")
}

func TestAddExistingKeyCallsCleanupFunction(t *testing.T) {
	keys := []int{}
	oldValues := []int{}
	cache, err := NewReplicatingLRUCacheWithCleanupFunc(3, 1, func(key, val interface{}) {
		keyInt := key.(int)
		valInt := val.(int)
		keys = append(keys, keyInt)
		oldValues = append(oldValues, valInt)
	})
	assert.NoError(t, err, "creating the cache should not fail with valid parameters")

	cache.Add(context.TODO(), 1, 1, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	entry, returnFirstEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	entry, returnSecondEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	returnFirstEntry()
	returnSecondEntry()

	cache.Add(context.TODO(), 1, 2, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	assert.ElementsMatch(t, keys, []int{1, 1}, "cleanup function should be called with key=1 twice")
	assert.ElementsMatch(t, oldValues, []int{1, 1}, "cleanup function should be called with value=1 twice")

	newValue, returnNewValue, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "the value should be present after being updated")
	assert.Equal(t, 2, newValue.(int), "the value in the cache should be updated")

	returnNewValue()
}

func TestRemoveKey(t *testing.T) {
	keys := []int{}
	oldValues := []int{}
	cache, err := NewReplicatingLRUCacheWithCleanupFunc(3, 1, func(key, val interface{}) {
		keyInt := key.(int)
		valInt := val.(int)
		keys = append(keys, keyInt)
		oldValues = append(oldValues, valInt)
	})
	assert.NoError(t, err, "creating the cache should not fail with valid parameters")

	cache.Add(context.TODO(), 1, 1, func() (interface{}, error) { return 1, nil })
	assert.Contains(t, cache.entries, 1, "should contain the newly added key")

	entry, returnFirstEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	entry, returnSecondEntry, ok := cache.Get(context.TODO(), 1)
	assert.True(t, ok, "should have entry in cache")
	assert.Equal(t, 1, entry.(int), "entry should have the correct value")

	returnFirstEntry()
	returnSecondEntry()

	cache.Remove(context.TODO(), 1)
	assert.NotContains(t, cache.entries, 1, "should not contain the removed key")

	assert.ElementsMatch(t, keys, []int{1, 1}, "cleanup function should be called with key=1 twice")
	assert.ElementsMatch(t, oldValues, []int{1, 1}, "cleanup function should be called with value=1 twice")

	_, _, ok = cache.Get(context.TODO(), 1)
	assert.False(t, ok, "the value should not be present after being updated")

}

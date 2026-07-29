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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
)

func TestInMemoryLocalCacheDefaults(t *testing.T) {
	d := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), d*4)
	defer cancel()
	c := NewLocalExpiringCacheWithDefault[string, Status, int](ctx, d, StatusUnknown)

	v, ok := c.Get("unknown")
	require.False(t, ok)
	require.Equal(t, v, StatusUnknown)
}

func TestInMemoryLocalCache(t *testing.T) {
	d := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), d*4)
	defer cancel()
	c := NewLocalExpiringCache[string, Status, int](ctx, d)
	testCache(t, ctx, c, d)
}

func testCache(t *testing.T, ctx context.Context, c Cache[string, Status, int], d time.Duration) {
	var wg sync.WaitGroup
	errors := make(chan error, 1)

	wg.Add(4)

	c.UpsertStatus("key1", StatusUnknown, 4, verifyOnExpired("key1", 4, &wg, errors))
	status, ok := c.Get("key1")
	require.Equal(t, StatusUnknown, status)
	require.True(t, ok)

	c.UpsertStatus("key2", StatusNotReady, 42, verifyOnExpired("key2", 42, &wg, errors))
	status, ok = c.Get("key2")
	require.Equal(t, StatusNotReady, status)
	require.True(t, ok)

	c.UpsertStatus("key1", StatusReady, 41, verifyOnExpired("key1", 41, &wg, errors))
	status, ok = c.Get("key1")
	require.Equal(t, StatusReady, status)
	require.True(t, ok)

	c.UpsertStatus("key2", StatusReady, 43, verifyOnExpired("key2", 43, &wg, errors))
	status, ok = c.Get("key2")
	require.Equal(t, StatusReady, status)
	require.True(t, ok)

	ctx, cancel := context.WithTimeout(ctx, d*2)
	defer cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		t.Errorf("Timeout waiting for wait group to be done")
	case err := <-errors:
		t.Error(err.Error())
	case <-done:
		// Wait expiration
		require.Nil(t, wait.PollUntilContextTimeout(context.TODO(), d, d*2, true, func(ctx2 context.Context) (done bool, err error) { _, ok := c.Get("key1"); return !ok, nil }))
		require.Nil(t, wait.PollUntilContextTimeout(context.TODO(), d, d*2, true, func(ctx2 context.Context) (done bool, err error) { _, ok := c.Get("key2"); return !ok, nil }))
	}
}

func verifyOnExpired(expectedKey string, expectedArg int, wg *sync.WaitGroup, errors chan<- error) func(key string, val Status, arg int) {
	return func(key string, _ Status, arg int) {
		if expectedKey != key {
			errors <- fmt.Errorf("expected key to be %v got %v", expectedKey, key)
		}
		if expectedArg != arg {
			errors <- fmt.Errorf("expected arg for key %v to be %v got %v", key, expectedArg, arg)
		}
		wg.Done()
	}
}

func TestExpireCallbackDoesNotDeadlock(t *testing.T) {
	d := 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := NewLocalExpiringCache[string, int, int](ctx, d)

	done := make(chan struct{})
	go func() {
		c.UpsertStatus("k1", 1, 1, func(key string, val int, arg int) {
			// Callback that touches the cache — would deadlock if called under lock.
			c.Get("k1")
			c.UpsertStatus("k2", 2, 2, func(string, int, int) {})
		})
		c.Expire("k1")
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("deadlock: Expire callback that touches cache did not complete in time")
	}
}

func TestRemoveExpiredEntriesCallbackDoesNotDeadlock(t *testing.T) {
	d := 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := NewLocalExpiringCache[string, int, int](ctx, d)

	callbackCalled := make(chan struct{})
	c.UpsertStatus("k1", 1, 1, func(key string, val int, arg int) {
		// Callback that touches the cache — would deadlock if called under lock.
		c.Get("k1")
		close(callbackCalled)
	})

	select {
	case <-callbackCalled:
	case <-ctx.Done():
		t.Fatal("deadlock: removeExpiredEntries callback that touches cache did not complete in time")
	}
}

func TestPanicInCallbackDoesNotKillGoroutine(t *testing.T) {
	d := 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := NewLocalExpiringCache[string, int, int](ctx, d)

	secondExpired := make(chan struct{})

	c.UpsertStatus("k1", 1, 1, func(string, int, int) {
		panic("test panic")
	})

	// Wait for the panicking callback to fire, then insert a second entry
	time.Sleep(d + 100*time.Millisecond)

	c.UpsertStatus("k2", 2, 2, func(string, int, int) {
		close(secondExpired)
	})

	select {
	case <-secondExpired:
	case <-ctx.Done():
		t.Fatal("background goroutine died after panic in onExpired callback")
	}
}

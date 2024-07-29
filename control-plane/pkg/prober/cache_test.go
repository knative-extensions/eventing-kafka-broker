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
		t.Errorf(err.Error())
	case <-done:
		// Wait expiration
		require.Nil(t, wait.PollImmediate(d, d*2, func() (done bool, err error) { _, ok := c.Get("key1"); return !ok, nil }))
		require.Nil(t, wait.PollImmediate(d, d*2, func() (done bool, err error) { _, ok := c.Get("key2"); return !ok, nil }))
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

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

func TestInMemoryLocalCache(t *testing.T) {
	d := time.Second
	ctx, cancel := context.WithTimeout(context.Background(), d*2)
	defer cancel()
	c := NewLocalExpiringCache(ctx, d)
	testCache(t, ctx, c, d)
}

func testCache(t *testing.T, ctx context.Context, c Cache, d time.Duration) {
	var wg sync.WaitGroup
	errors := make(chan error, 1)

	wg.Add(2)

	c.UpsertStatus("key1", StatusUnknown, 4, verifyNoExpired(errors))
	require.Equal(t, StatusUnknown, c.GetStatus("key1"))

	c.UpsertStatus("key2", StatusNotReady, 42, verifyNoExpired(errors))
	require.Equal(t, StatusNotReady, c.GetStatus("key2"))

	c.UpsertStatus("key1", StatusReady, 41, verifyOnExpired("key1", 41, &wg, errors))
	require.Equal(t, StatusReady, c.GetStatus("key1"))

	c.UpsertStatus("key2", StatusReady, 43, verifyOnExpired("key2", 43, &wg, errors))
	require.Equal(t, StatusReady, c.GetStatus("key2"))

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
		require.Nil(t, wait.PollImmediate(d, d*2, func() (done bool, err error) { return StatusUnknown == c.GetStatus("key1"), nil }))
		require.Nil(t, wait.PollImmediate(d, d*2, func() (done bool, err error) { return StatusUnknown == c.GetStatus("key2"), nil }))
	}
}

func verifyNoExpired(errors chan<- error) func(key string, arg interface{}) {
	return func(key string, arg interface{}) {
		errors <- fmt.Errorf("unexpected call to onExpired callback")
	}
}

func verifyOnExpired(expectedKey string, expectedArg interface{}, wg *sync.WaitGroup, errors chan<- error) func(key string, arg interface{}) {
	return func(key string, arg interface{}) {
		if expectedKey != key {
			errors <- fmt.Errorf("expected key to be %v got %v", expectedKey, key)
		}
		if expectedArg != arg {
			errors <- fmt.Errorf("expected arg for key %v to be %v got %v", key, expectedArg, arg)
		}
		wg.Done()
	}
}

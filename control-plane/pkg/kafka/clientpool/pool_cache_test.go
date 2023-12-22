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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testCloseable struct {
	lock   sync.Mutex
	data   int
	closed bool
}

func (tc *testCloseable) Close() error {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	if tc.closed == true {
		return fmt.Errorf("can't close an already closed value")
	}
	tc.closed = true
	return nil
}

func TestAddThreeClientsAndAcquire(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.AddAndAcquire(ctx, "2", func() (*testCloseable, error) {
		return &testCloseable{data: 2}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 2, value2.data)
	assert.False(t, exists)

	value3, returnValue3, exists, err := pool.AddAndAcquire(ctx, "3", func() (*testCloseable, error) {
		return &testCloseable{data: 3}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 3, value3.data)
	assert.False(t, exists)

	returnValue1()
	returnValue2()
	returnValue3()

	cancel()
}

func TestAddSameKeyTwice(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value2.data)
	assert.True(t, exists)

	returnValue1()
	returnValue2()

	cancel()
}

func TestAddAndGetKeyWithoutReturningClient(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.Equal(t, 1, value2.data)
	assert.True(t, exists)

	returnValue1()
	returnValue2()

	cancel()
}

func TestNoMoreGetsUntilReturn(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 2)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.Equal(t, 1, value2.data)
	assert.True(t, exists)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond*250)

	_, returnValue3, exists, err := pool.Get(ctx2, "1")

	assert.Error(t, err)
	assert.True(t, exists)

	returnValue3()
	returnValue1()

	value3, returnValue3, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, 1, value3.data)

	assert.NoError(t, err)

	returnValue2()
	returnValue3()

	cancel2()
	cancel()
}

func TestGetBeforeAdd(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)

	_, _, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.False(t, exists)

	cancel()
}

func TestUpdateUpdatesValue(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 2)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.Equal(t, 1, value2.data)
	assert.True(t, exists)

	returnValue1()
	returnValue2()

	exists, err = pool.UpdateIfExists("1", func() (*testCloseable, error) {
		return &testCloseable{
			data: 2,
		}, nil
	}, 2)

	assert.NoError(t, err)
	assert.True(t, exists)

	value3, returnValue3, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.Equal(t, 2, value3.data)
	assert.True(t, exists)

	returnValue3()

	cancel()
}

func TestUpdateIncreaseCapacity(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value1, returnValue1, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 2)

	assert.NoError(t, err)
	assert.Equal(t, 1, value1.data)
	assert.False(t, exists)

	value2, returnValue2, exists, err := pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.Equal(t, 1, value2.data)
	assert.True(t, exists)

	returnValue1()
	returnValue2()

	exists, err = pool.UpdateIfExists("1", func() (*testCloseable, error) {
		return &testCloseable{
			data: 2,
		}, nil
	}, 4)

	assert.NoError(t, err)
	assert.True(t, exists)

	returnValues := make([]ReturnClientFunc, 0, 4)

	for i := 0; i < 4; i++ {
		value, returnValue, exists, err := pool.Get(ctx, "1")
		returnValues = append(returnValues, returnValue)
		assert.NoError(t, err)
		assert.Equal(t, 2, value.data)
		assert.True(t, exists)
	}

	for _, returnValue := range returnValues {
		returnValue()
	}

	cancel()
}

func TestUpdateDecreaseCapacity(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](time.Minute, time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)

	value, returnValue, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value.data)
	assert.False(t, exists)

	returnValue()

	// get all of the values up to the capacity
	returnValues := make([]ReturnClientFunc, 0, 8)
	for i := 0; i < 8; i++ {
		value, returnValue, exists, err := pool.Get(ctx, "1")
		returnValues = append(returnValues, returnValue)
		assert.NoError(t, err)
		assert.Equal(t, 1, value.data)
		assert.True(t, exists)
	}

	shortCtx, cancelShortCtx := context.WithTimeout(context.Background(), time.Millisecond*250)
	_, returnValue, exists, err = pool.Get(shortCtx, "1")
	assert.Error(t, err)
	assert.True(t, exists)
	returnValue()
	cancelShortCtx()

	for _, returnValue := range returnValues {
		returnValue()
	}

	exists, err = pool.UpdateIfExists("1", func() (*testCloseable, error) {
		return &testCloseable{
			data: 2,
		}, nil
	}, 4)

	assert.NoError(t, err)
	assert.True(t, exists)

	returnValues = make([]ReturnClientFunc, 0, 4)

	for i := 0; i < 4; i++ {
		value, returnValue, exists, err := pool.Get(ctx, "1")
		returnValues = append(returnValues, returnValue)
		assert.NoError(t, err)
		assert.Equal(t, 2, value.data)
		assert.True(t, exists)
	}

	shortCtx, cancelShortCtx = context.WithTimeout(context.Background(), time.Millisecond*250)
	_, returnValue, exists, err = pool.Get(shortCtx, "1")
	assert.Error(t, err)
	assert.True(t, exists)
	returnValue()
	cancelShortCtx()

	for _, returnValue := range returnValues {
		returnValue()
	}

	cancel()
}

func TestCleanupValues(t *testing.T) {
	t.Parallel()

	pool := NewLRUCache[string, *testCloseable](100*time.Millisecond, 100*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*30)

	value, returnValue, exists, err := pool.AddAndAcquire(ctx, "1", func() (*testCloseable, error) {
		return &testCloseable{data: 1}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 1, value.data)
	assert.False(t, exists)

	returnValue()

	// make sure we wait long enough that the value will be cleaned up
	time.Sleep(100 * time.Millisecond)

	value, returnValue, exists, err = pool.AddAndAcquire(ctx, "2", func() (*testCloseable, error) {
		return &testCloseable{data: 2}, nil
	}, 8)

	assert.NoError(t, err)
	assert.Equal(t, 2, value.data)
	assert.False(t, exists)

	returnValue()

	// give the goroutine an opportunity to clean up the value
	time.Sleep(time.Millisecond * 100)

	_, returnValue, exists, err = pool.Get(ctx, "1")

	assert.NoError(t, err)
	assert.False(t, exists)

	returnValue()

	cancel()
}

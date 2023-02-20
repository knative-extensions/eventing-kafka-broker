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

package util

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestExpiringLockMap_GetLock(t *testing.T) {
	chanCount := 1000 // controls concurrency
	size := 50        // controls how many keys are used and thus the collision rate

	r := rand.New(rand.NewSource(42))
	randomKey := func() string {
		return fmt.Sprintf("key-%d", r.Intn(size))
	}

	wg := sync.WaitGroup{}
	chans := make([]chan string, chanCount)

	// fill channels with data
	for i := 0; i < chanCount; i++ {
		chans[i] = make(chan string, size)
		for j := 0; j < size; j++ {
			chans[i] <- randomKey()
		}
		close(chans[i])
	}
	wg.Add(chanCount)

	// consume data from channels concurrently. Lock and unlock the lock for each key.
	// TTL is 10 hours, so the locks should never expire.
	lm := NewExpiringLockMap[string](context.Background(), 10*time.Hour)

	for i := 0; i < chanCount; i++ {
		go func(i int) {
			defer wg.Done()
			for key := range chans[i] {
				func() {
					lock := lm.GetLock(key)
					lock.Lock()
					defer lock.Unlock()
				}()
			}
		}(i)
	}
	wg.Wait()
}

func TestExpiringLockMapExpiration(t *testing.T) {
	lm := NewExpiringLockMap[string](context.Background(), 100*time.Millisecond)

	l1_a := lm.GetLock("l1")

	time.Sleep(50 * time.Millisecond)
	l1_b := lm.GetLock("l1") // should be the same lock as l1_a, as TTL is 100ms and 50 < 100

	time.Sleep(300 * time.Millisecond)
	l1_c := lm.GetLock("l1") // should be a different lock as TTL is 100ms and 300 > 100

	if l1_a != l1_b {
		t.Errorf("l1_a != l1_b")
	}
	if l1_a == l1_c {
		t.Errorf("l1_a == l1_c")
	}
}

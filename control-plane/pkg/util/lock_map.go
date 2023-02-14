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
	"sync"
	"time"
)

// LockMap provides a mechanism to get a lock for a given key.
type LockMap interface {
	GetLock(interface{}) *sync.Mutex
}

type expiringLockMap struct {
	mu     sync.Mutex
	locks  map[interface{}]*sync.Mutex
	access map[interface{}]time.Time

	ttl time.Duration
}

// NewExpiringLockMap returns a new LockMap that removes entries after the given TTL.
func NewExpiringLockMap(ctx context.Context, ttl time.Duration) LockMap {
	lm := &expiringLockMap{
		locks:  make(map[interface{}]*sync.Mutex),
		access: make(map[interface{}]time.Time),
		ttl:    ttl,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(ttl):
				lm.removeExpiredEntries(time.Now())
			}
		}
	}()

	return lm
}

func (lm *expiringLockMap) GetLock(key interface{}) *sync.Mutex {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var lock *sync.Mutex
	var ok bool
	if lock, ok = lm.locks[key]; !ok {
		lock = &sync.Mutex{}
		lm.locks[key] = lock
	}

	lm.access[key] = time.Now()

	return lock
}

func (lm *expiringLockMap) removeExpiredEntries(now time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for key, lastAccess := range lm.access {
		if now.Sub(lastAccess) > lm.ttl {
			delete(lm.locks, key)
			delete(lm.access, key)
		}
	}
}

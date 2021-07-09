package prober

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const (
	// StatusReady signals that a given object is ready.
	StatusReady Status = iota
	// StatusUnknown signals that a given object is not ready and its state is unknown.
	StatusUnknown
	// StatusNotReady signals that a given object is not ready.
	StatusNotReady
)

// Status represents the resource status.
type Status int

// Cache is a key-status store.
type Cache interface {
	// GetStatus retries the status associated with the given key.
	GetStatus(key string) Status
	// UpsertStatus add or updates the status associated with the given key.
	// Once the given key expires the onExpired callback will be called passing the arg parameter.
	UpsertStatus(key string, status Status, arg interface{}, onExpired ExpiredFunc)
}

// ExpiredFunc is a callback called once an entry in the cache is expired.
type ExpiredFunc func(key string, arg interface{})

type inMemoryLocalCache struct {
	// mu protects targets and entries
	mu sync.RWMutex
	// targets is a map of key-pointer to an element in the entries list.
	targets map[string]*list.Element
	// entries is a doubly-linked list of all entries present in the cache.
	// This allow fast deletion of expired entries.
	entries *list.List

	expireDuration time.Duration
}

type value struct {
	status     Status
	lastUpsert time.Time
	key        string
	arg        interface{}
	onExpired  ExpiredFunc
}

func NewInMemoryLocalCache(ctx context.Context, expireDuration time.Duration) Cache {
	c := &inMemoryLocalCache{
		mu:             sync.RWMutex{},
		targets:        make(map[string]*list.Element, 64),
		entries:        list.New().Init(),
		expireDuration: expireDuration,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(expireDuration):
				c.removeExpiredEntries(time.Now())
			}
		}
	}()
	return c
}

func (c *inMemoryLocalCache) GetStatus(key string) Status {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.targets[key]; ok {
		value := v.Value.(*value)
		if !c.isExpired(value, time.Now()) {
			return value.status
		}
	}
	return StatusUnknown
}

func (c *inMemoryLocalCache) UpsertStatus(key string, status Status, arg interface{}, onExpired ExpiredFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.targets[key]; ok {
		delete(c.targets, key)
		c.entries.Remove(v)
	}

	value := &value{status: status, lastUpsert: time.Now(), key: key, arg: arg, onExpired: onExpired}
	element := c.entries.PushBack(value)
	c.targets[key] = element
}

func (c *inMemoryLocalCache) removeExpiredEntries(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for curr := c.entries.Front(); curr != nil && c.isExpired(curr.Value.(*value), now); {
		v := curr.Value.(*value)
		delete(c.targets, v.key)
		prev := curr
		curr = curr.Next()
		c.entries.Remove(prev)

		v.onExpired(v.key, v.arg)
	}
}

func (c *inMemoryLocalCache) isExpired(v *value, now time.Time) bool {
	return v.lastUpsert.Add(c.expireDuration).Before(now)
}

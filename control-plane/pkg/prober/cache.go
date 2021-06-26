package prober

import (
	"container/list"
	"context"
	"sync"
	"time"
)

type Status int

const (
	// StatusReady signals that a given object is ready.
	StatusReady Status = iota
	// StatusUnknown signals that a given object is not ready and its state is unknown.
	StatusUnknown
	// StatusNotReady signals that a given object is not ready.
	StatusNotReady
)

type Cache struct {
	mu      sync.RWMutex
	targets map[string]*list.Element
	entries list.List

	expireDurationMs int64
}

type ExpiredFunc func(key string, arg interface{})

type value struct {
	status     Status
	lastUpdate int64
	key        string
	arg        interface{}
	onExpired  ExpiredFunc
}

func NewCache(ctx context.Context, expireDuration time.Duration) *Cache {
	c := &Cache{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(expireDuration):
				c.removeExpiredEntries(time.Now().UnixNano())
			}
		}
	}()
	return c
}

func (c *Cache) GetStatus(key string) Status {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if v, ok := c.targets[key]; ok {
		return v.Value.(*value).status
	}
	return StatusUnknown
}

func (c *Cache) UpdateStatus(key string, status Status, arg interface{}, onExpired ExpiredFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v, ok := c.targets[key]; ok {
		c.entries.Remove(v)
		delete(c.targets, key)
	}

	value := &value{status: status, lastUpdate: time.Now().UnixNano(), key: key, arg: arg, onExpired: onExpired}
	element := c.entries.PushBack(value)
	c.targets[key] = element
}

func (c *Cache) removeExpiredEntries(now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for e := c.entries.Front(); e.Value.(*value).lastUpdate+c.expireDurationMs < now; e = e.Next() {
		v := e.Value.(*value)
		delete(c.targets, v.key)
		c.entries.Remove(e)
		v.onExpired(v.key, v.arg)
	}
}

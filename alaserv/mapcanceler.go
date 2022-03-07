package alaserv

import (
	"context"

	"github.com/tidwall/spinlock"
)

// newCanceler creates a new instance of canceler.
func newCanceler() *canceler {
	return &canceler{
		vals: map[string]context.CancelFunc{},
	}
}

// canceler contains context cancel function to control go routines.
type canceler struct {
	vals map[string]context.CancelFunc
	l    spinlock.Locker
}

// Cancel execute cancel function by key.
func (c *canceler) Cancel(key string) {
	cancel, ok := c.vals[key]
	if ok {
		cancel()
	}
}

func (c *canceler) CancelAll() {
	c.l.Lock()
	defer c.l.Unlock()
	for _, cancel := range c.vals {
		cancel()
	}
}

// Set a canceler atomically.
func (c *canceler) Set(key string, cancel context.CancelFunc) {
	c.l.Lock()
	defer c.l.Unlock()
	c.vals[key] = cancel
}

// Delete a key atomically.
func (c *canceler) Delete(key string) {
	c.l.Lock()
	defer c.l.Unlock()
	delete(c.vals, key)
}

package correlator

import (
	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/tidwall/spinlock"
)

// NewCorrelator creates a new instance of Correlator.
func NewCorrelator() *Correlator {
	return &Correlator{
		chs: map[string]chan amqp.Delivery{},
	}
}

// Correlator correlates correlationId with reply channel and usually used for RPC AMQP Message Broker.
//
// Warning Note: This doesn't have automatic cyclic garbage collector, use it cleanly to avoid memory leak by retrieving .
type Correlator struct {
	chs map[string]chan amqp.Delivery
	l   spinlock.Locker
}

// NewCorrelationID creates a new channel and random correlation id string and
// returns string for retrieving reply channel.
func (c *Correlator) NewCorrelationID() (cid string) {
	cid = uuid.NewV4().String()
	c.l.Lock()
	defer c.l.Unlock()
	c.chs[cid] = make(chan amqp.Delivery, 1)
	return cid
}

// isChanOpen checks if channel is open or not.
func (c *Correlator) isChanOpen(ch chan amqp.Delivery) bool {
	select {
	case _, ok := <-ch:
		if !ok {
			return false
		}
	default:
	}

	return true
}

// Reply publish a message to a channel using correlationID.
func (c *Correlator) Reply(correlationID string, data amqp.Delivery) error {
	c.l.Lock()
	ch, ok := c.chs[correlationID]
	if !ok {
		c.l.Unlock()
		return ErrCorrReplyChanNotExist
	}
	c.l.Unlock()

	if !c.isChanOpen(ch) {
		return ErrCorrReplyChanNotExist
	}

	ch <- data

	return nil
}

// Close cleans all memory held.
func (c *Correlator) Close() {
	c.l.Lock()
	defer c.l.Unlock()

	for _, ch := range c.chs {
		if c.isChanOpen(ch) {
			close(ch)
		}
	}
}

// ReplyChan returns reader of reply channel by its correlationID.
func (c *Correlator) ReplyChan(correlationID string) <-chan amqp.Delivery {
	c.l.Lock()
	defer c.l.Unlock()
	return c.chs[correlationID]
}

// Delete deletes and close reply channel by its correlationID.
func (c *Correlator) Delete(correlationID string) bool {
	c.l.Lock()
	defer c.l.Unlock()

	ch, ok := c.chs[correlationID]
	if !ok {
		return true
	}

	if !c.isChanOpen(ch) {
		return true
	}

	close(ch)

	return true
}

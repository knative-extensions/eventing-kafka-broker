package network

import (
	"context"
	"sync"

	ctrl "knative.dev/control-protocol/pkg"
)

// unboundedMessageQueue is a pull based message queue.
// If you're wondering why we need this,
// we're waiting for https://github.com/golang/go/issues/27935 to happen.
type unboundedMessageQueue struct {
	cond  *sync.Cond
	queue []*ctrl.Message
}

func newUnboundedMessageQueue() unboundedMessageQueue {
	return unboundedMessageQueue{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (q *unboundedMessageQueue) append(msg *ctrl.Message) {
	q.cond.L.Lock()
	q.queue = append(q.queue, msg)
	q.cond.L.Unlock()
	q.cond.Signal()
}

func (q *unboundedMessageQueue) prepend(msg *ctrl.Message) {
	q.cond.L.Lock()
	q.queue = append([]*ctrl.Message{msg}, q.queue...)
	q.cond.L.Unlock()
	q.cond.Signal()
}

// blockingPoll will block until there's something to grab from the queue.
// If the provided context get closed, this method unblocks and return nil.
func (q *unboundedMessageQueue) blockingPoll(ctx context.Context) *ctrl.Message {
	if ctx.Err() != nil { // Context closed, quit
		return nil
	}
	q.cond.L.Lock()
	for len(q.queue) == 0 {
		q.cond.Wait()
		if ctx.Err() != nil { // Context closed, quit
			q.cond.L.Unlock()
			return nil
		}
	}
	msg := q.queue[0]
	q.queue = q.queue[1:]
	q.cond.L.Unlock()
	return msg
}

func (q *unboundedMessageQueue) unblockPoll() {
	q.cond.Broadcast()
}

/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package network

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"

	ctrl "knative.dev/control-protocol/pkg"
)

type baseTcpConnection struct {
	ctx    context.Context
	logger *zap.SugaredLogger

	writeQueue unboundedMessageQueue
	readQueue  unboundedMessageQueue

	unrecoverableErrors chan error
}

var _ ctrl.Connection = (*baseTcpConnection)(nil)

func (t *baseTcpConnection) WriteMessage(msg *ctrl.Message) {
	t.writeQueue.append(msg)
}

func (t *baseTcpConnection) ReadMessage() *ctrl.Message {
	return t.readQueue.blockingPoll(t.ctx)
}

func (t *baseTcpConnection) Errors() <-chan error {
	return t.unrecoverableErrors
}

// consumeConnection associates a net.Conn to this baseTcpConnection struct, reading its internal write queue and writing its internal read queue.
// This method is blocking and returns when either the connection broke or the t.ctx get closed.
func (t *baseTcpConnection) consumeConnection(conn net.Conn) {
	t.logger.Infof("Started consuming new conn: %s", conn.RemoteAddr())

	// This channel get closed also when t.ctx is closed by the last goroutine in this method
	closedConnCtx, closedConnCancel := context.WithCancel(context.TODO())

	var wg sync.WaitGroup
	wg.Add(3)

	// We have 3 polling loops:
	// * One polls the 2 contexts and, if one of them is done, it closes the connection, in order to cleanup the other goroutines
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		defer wg.Done()

		// Wait for one of the two to complete
		select {
		case <-closedConnCtx.Done():
			break
		case <-t.ctx.Done():
			// Let's make sure this channel is closed
			closedConnCancel()
			break
		}

		// Either the connection is closed or the context is closed,
		// we unblock the polling to stop the below goroutine.
		// When unblocking, closedConnCtx is closed.
		t.writeQueue.unblockPoll()

		err := conn.Close()
		if err != nil && !isEOF(err) {
			t.logger.Warnf("Error while closing the connection with local %s and remote %s: %s", conn.LocalAddr().String(), conn.RemoteAddr().String(), err)
		}
		t.logger.Debugf("Connection closed with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
	}()
	go func() {
		defer wg.Done()
		for {
			// Blocking queue polling
			msg := t.writeQueue.blockingPoll(closedConnCtx)
			if msg == nil {
				return // Polling was
			}

			err := connWrite(conn, msg)
			if err != nil {
				// Let's re-enqueue the message
				t.writeQueue.prepend(msg)

				if isEOF(err) {
					return // Closed conn
				}

				// Signal the error
				t.unrecoverableErrors <- err
				if !isTransientError(err) {
					return // Broken conn
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		// If this goroutine returns, either the connection closed correctly from t.ctx, or the
		// connection broke
		defer closedConnCancel()
		for {
			// Blocking connection read
			msg, err := connRead(conn)

			if err != nil {
				// Check closed conn
				if isEOF(err) {
					return
				}

				// Signal the error
				t.unrecoverableErrors <- err

				// Broken connection
				if !isTransientError(err) {
					return
				}
			} else {
				t.readQueue.append(msg)
			}

			// t.ctx is closed
			if t.ctx.Err() != nil {
				return
			}
		}
	}()

	wg.Wait()

	t.logger.Debugf("Stopped consuming connection with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
}

// cleanup is safe to be invoked only if no connection is being consumed and t.ctx is closed
func (t *baseTcpConnection) cleanup() {
	// Let's make sure we unblock some dangling polling
	t.writeQueue.unblockPoll()
	t.readQueue.unblockPoll()

	// the unrecoverableErrors channel is closed
	close(t.unrecoverableErrors)
}

func connRead(conn net.Conn) (*ctrl.Message, error) {
	msg := &ctrl.Message{}
	n, err := msg.ReadFrom(conn)
	if err != nil {
		return nil, err
	}
	if n != int64(msg.Length())+24 {
		return nil, fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}

	return msg, nil
}

func connWrite(conn net.Conn, msg *ctrl.Message) error {
	n, err := msg.WriteTo(conn)
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}
	return nil
}

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
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"

	"go.uber.org/zap"

	ctrl "knative.dev/control-protocol/pkg"
)

type baseTcpConnection struct {
	ctx    context.Context
	logger *zap.SugaredLogger

	outboundMessageChannel chan *ctrl.Message
	inboundMessageChannel  chan *ctrl.Message
	errors                 chan error
}

func (t *baseTcpConnection) OutboundMessages() chan<- *ctrl.Message {
	return t.outboundMessageChannel
}

func (t *baseTcpConnection) InboundMessages() <-chan *ctrl.Message {
	return t.inboundMessageChannel
}

func (t *baseTcpConnection) Errors() <-chan error {
	return t.errors
}

func (t *baseTcpConnection) read(conn net.Conn) error {
	msg := &ctrl.Message{}
	n, err := msg.ReadFrom(conn)
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}

	t.inboundMessageChannel <- msg
	return nil
}

func (t *baseTcpConnection) write(conn net.Conn, msg *ctrl.Message) error {
	n, err := msg.WriteTo(conn)
	if err != nil {
		return err
	}
	if n != int64(msg.Length())+24 {
		return fmt.Errorf("the number of read bytes doesn't match the expected length: %d != %d", n, int64(msg.Length())+24)
	}
	return nil
}

func (t *baseTcpConnection) consumeConnection(conn net.Conn) {
	t.logger.Infof("Started consuming new conn: %s", conn.RemoteAddr())

	closedConnCtx, closedConnCancel := context.WithCancel(context.TODO())

	var wg sync.WaitGroup
	wg.Add(3)

	// We have 3 polling loops:
	// * One polls the 2 contexts and, if one of them is done, it closes the connection, in order to close the other goroutines
	// * One polls outbound messages and writes to the conn
	// * One reads from the conn and push to inbound messages
	go func() {
		defer wg.Done()
		// Wait for one of the two to complete
		select {
		case <-closedConnCtx.Done():
			break
		case <-t.ctx.Done():
			break
		}
		err := conn.Close()
		if err != nil && !isEOF(err) && !isUseOfClosedConnection(err) {
			t.logger.Warnf("Error while closing the connection with local %s and remote %s: %s", conn.LocalAddr().String(), conn.RemoteAddr().String(), err)
		}
		t.logger.Debugf("Connection closed with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closedConnCtx.Done():
				return
			case <-t.ctx.Done():
				return
			case msg, ok := <-t.outboundMessageChannel:
				if !ok {
					t.logger.Debugf("Outbound channel closed, closing the polling")
					return
				}
				err := t.write(conn, msg)
				if err != nil {
					t.outboundMessageChannel <- msg

					if isEOF(err) {
						return // Closed conn
					}
					t.errors <- err
					if !isTransientError(err) {
						return // Broken conn
					}
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
			// Blocking read
			err := t.read(conn)
			select {
			case <-closedConnCtx.Done():
				return
			case <-t.ctx.Done():
				return
			default:
				if err != nil {
					if isEOF(err) {
						return // Closed conn
					}
					t.errors <- err
					if !isTransientError(err) {
						return // Broken conn
					}
				}
			}
		}
	}()

	wg.Wait()

	t.logger.Debugf("Stopped consuming connection with local %s and remote %s", conn.LocalAddr().String(), conn.RemoteAddr().String())
}

func (t *baseTcpConnection) close() {
	close(t.inboundMessageChannel)
	close(t.outboundMessageChannel)
	close(t.errors)
}

func isTransientError(err error) bool {
	// Transient errors are fine
	if neterr, ok := err.(net.Error); ok {
		if neterr.Temporary() || neterr.Timeout() {
			return true
		}
	}
	return false
}

func isEOF(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

var isUseOfClosedConnectionRegex = regexp.MustCompile("use of closed.* connection")

func isUseOfClosedConnection(err error) bool {
	// Don't rely on this check, it's just used to reduce logging noise, it shouldn't be used as assertion
	return isUseOfClosedConnectionRegex.MatchString(err.Error())
}

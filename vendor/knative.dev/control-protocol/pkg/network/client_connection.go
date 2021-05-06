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
	"strings"
	"time"

	"knative.dev/pkg/logging"

	ctrl "knative.dev/control-protocol/pkg"
	ctrlservice "knative.dev/control-protocol/pkg/service"
)

type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

func StartControlClient(ctx context.Context, dialer Dialer, target string) (ctrl.Service, error) {
	if !strings.Contains(target, ":") {
		target = target + ":9000"
	}
	logging.FromContext(ctx).Infof("Starting control client to %s", target)

	// Let's try the dial
	conn, err := tryDial(ctx, dialer, target, clientInitialDialRetry, clientDialRetryInterval)
	if err != nil {
		return nil, fmt.Errorf("cannot perform the initial dial to target %s: %w", target, err)
	}

	tcpConn := newClientTcpConnection(ctx, dialer)
	svc := ctrlservice.NewService(ctx, tcpConn)

	tcpConn.startPolling(conn)

	return svc, nil
}

func tryDial(ctx context.Context, dialer Dialer, target string, retries int, interval time.Duration) (net.Conn, error) {
	var conn net.Conn
	var err error
	for i := 0; i < retries; i++ {
		conn, err = dialer.DialContext(ctx, "tcp", target)
		if err == nil {
			return conn, nil
		}
		logging.FromContext(ctx).Warnf("Error while trying to connect: %v", err)
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(interval):
		}
	}
	return nil, err
}

type clientTcpConnection struct {
	baseTcpConnection

	dialer Dialer
}

func newClientTcpConnection(ctx context.Context, dialer Dialer) *clientTcpConnection {
	c := &clientTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                 ctx,
			logger:              logging.FromContext(ctx),
			writeQueue:          newUnboundedMessageQueue(),
			readQueue:           newUnboundedMessageQueue(),
			unrecoverableErrors: make(chan error, 10),
		},
		dialer: dialer,
	}
	return c
}

func (t *clientTcpConnection) startPolling(initialConn net.Conn) {
	// We have 1 goroutine that consumes the connections and it eventually reconnects.
	// When done, it closed the internal channels
	go func(initialConn net.Conn) {
		// Consume the initial connection
		t.consumeConnection(initialConn)

		// This returns when either the context is closed
		// or when redialing is not possible anymore
		t.reDialLoop(initialConn.RemoteAddr())

		t.logger.Infof("Closing control client")
		t.cleanup()
		t.logger.Infof("Connection closed")
	}(initialConn)
}

func (t *clientTcpConnection) reDialLoop(remoteAddr net.Addr) {
	// Retry until connection closed
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			t.logger.Warnf("Connection lost, retrying to reconnect %s", remoteAddr.String())

			// Let's try the dial
			conn, err := tryDial(t.ctx, t.dialer, remoteAddr.String(), clientReconnectionRetry, clientDialRetryInterval)
			if err != nil {
				t.logger.Warnf("Cannot re-dial to target %s: %v", remoteAddr.String(), err)
				return
			}

			t.consumeConnection(conn)
		}
	}
}

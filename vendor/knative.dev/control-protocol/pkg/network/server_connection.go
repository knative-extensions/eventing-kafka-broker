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
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"knative.dev/pkg/logging"

	ctrl "knative.dev/control-protocol/pkg"
	"knative.dev/control-protocol/pkg/service"
)

var listenConfig = net.ListenConfig{
	KeepAlive: 30 * time.Second,
}

type ControlServerOptions struct {
	port         int
	listenConfig *net.ListenConfig
}

type ControlServerOption func(*ControlServerOptions)

func WithPort(port int) ControlServerOption {
	return func(options *ControlServerOptions) {
		options.port = port
	}
}

type ControlServer struct {
	ctrl.Service
	closedCh <-chan struct{}
	port     int
}

// ClosedCh returns a channel which is closed after the server stopped listening
func (cs *ControlServer) ClosedCh() <-chan struct{} {
	return cs.closedCh
}

// ListeningPort is the port where the server is actually listening
func (cs *ControlServer) ListeningPort() int {
	return cs.port
}

func StartInsecureControlServer(ctx context.Context, options ...ControlServerOption) (*ControlServer, error) {
	return StartControlServer(ctx, nil, options...)
}

func StartControlServer(ctx context.Context, tlsConfigLoader func() (*tls.Config, error), options ...ControlServerOption) (*ControlServer, error) {
	opts := ControlServerOptions{
		port:         9000,
		listenConfig: &listenConfig,
	}

	for _, fn := range options {
		fn(&opts)
	}

	ln, err := opts.listenConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", opts.port))
	if err != nil {
		return nil, err
	}
	listeningAddress := ln.Addr().String()
	logging.FromContext(ctx).Infof("Started listener: %s", listeningAddress)

	// Parse the port
	port, err := strconv.Atoi(listeningAddress[strings.LastIndex(listeningAddress, ":")+1:])
	if err != nil {
		return nil, fmt.Errorf("cannot parse the listening port: %w", err)
	}

	tcpConn := newServerTcpConnection(ctx, ln, tlsConfigLoader)
	ctrlService := service.NewService(ctx, tcpConn)

	closedServerCh := make(chan struct{})

	tcpConn.startAcceptPolling(closedServerCh)

	ctrlServer := &ControlServer{
		Service:  ctrlService,
		closedCh: closedServerCh,
		port:     port,
	}

	return ctrlServer, nil
}

type serverTcpConnection struct {
	baseTcpConnection

	listener        net.Listener
	tlsConfigLoader func() (*tls.Config, error)
}

func newServerTcpConnection(ctx context.Context, listener net.Listener, tlsConfigLoader func() (*tls.Config, error)) *serverTcpConnection {
	c := &serverTcpConnection{
		baseTcpConnection: baseTcpConnection{
			ctx:                    ctx,
			logger:                 logging.FromContext(ctx),
			outboundMessageChannel: make(chan *ctrl.Message, 10),
			inboundMessageChannel:  make(chan *ctrl.Message, 10),
			errors:                 make(chan error, 10),
		},
		listener:        listener,
		tlsConfigLoader: tlsConfigLoader,
	}
	return c
}

func (t *serverTcpConnection) startAcceptPolling(closedServerChannel chan struct{}) {
	// We have 2 goroutines:
	// * One pools the t.ctx and closes the listener
	// * One polls the listener to accept new conns. When done, it closes the connection channels
	go func() {
		<-t.ctx.Done()
		t.logger.Infof("Closing control server")
		err := t.listener.Close()
		t.logger.Infof("Listener closed")
		if err != nil {
			t.logger.Warnf("Error while closing the listener: %s", err)
		}
	}()
	go func() {
		// This returns when either the listener is closed by external signal
		// or catastrophic failure happened. In both cases, we want to close
		t.listenLoop()

		t.close()
		close(closedServerChannel)
	}()
}

func (t *serverTcpConnection) listenLoop() {
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			conn, err := t.listener.Accept()
			if err != nil {
				t.logger.Warnf("Error while accepting the connection, closing the accept loop: %s", err)
				return
			}
			if t.tlsConfigLoader != nil {
				t.logger.Debugf("Upgrading conn %v to tls", conn.RemoteAddr())
				tlsConf, err := t.tlsConfigLoader()
				if err != nil {
					t.logger.Warnf("Cannot load tls configuration: %v", err)
					t.errors <- err
					_ = conn.Close()
					continue
				}
				conn = tls.Server(conn, tlsConf)
			}
			t.logger.Debugf("Accepting new control connection from %s", conn.RemoteAddr())
			t.consumeConnection(conn)
		}
	}
}

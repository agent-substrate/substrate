// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dns

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
)

type serverConfig struct {
	upstreams []string
}

type netConn struct {
	dialer      net.Dialer
	udp         net.PacketConn
	egressUDP   net.PacketConn
	tcpListener net.Listener
}

// Server is one sandbox's running DNS relay, returned by [Relay.Serve].
type Server struct {
	n *netConn

	limiter         *limiter
	pendingRequests *pendingRequests
	dns             *dnsHandler
	udp             *udpHandler
	tcp             *tcpHandler

	stopServing context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
	serving     sync.WaitGroup
}

// newServer binds the sockets and starts serving goroutines.
func newServer(
	ctx context.Context,
	config *serverConfig,
	netC *netConn,
) (*Server, error) {
	if netC.egressUDP == nil {
		egressUDP, err := net.ListenPacket("udp", ":0")
		if err != nil {
			slog.WarnContext(ctx, "Failed to open worker DNS egress socket", slog.Any("err", err))
			return nil, fmt.Errorf("DNS egress socket ListenPacket: %w", err)
		}
		netC.egressUDP = egressUDP
	}

	// Detached from the activation RPC's context but cancelable so teardown
	// drops queries still in flight and releases the actor's sockets.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))

	udpAddrs := make([]*net.UDPAddr, 0, len(config.upstreams))
	for _, u := range config.upstreams {
		if addr, err := net.ResolveUDPAddr("udp", u); err == nil {
			udpAddrs = append(udpAddrs, addr)
		}
	}

	s := &Server{
		stopServing: stopServing,
		n:           netC,
		limiter:     newLimiter(),
	}

	s.pendingRequests = newPendingRequests(s.limiter)
	s.dns = newDNSHandler(s.pendingRequests, s.limiter, nil)
	s.udp = newUDPHandler(netC.udp, netC.egressUDP, udpAddrs, s.dns, s.pendingRequests)
	s.tcp = newTCPHandler(config.upstreams, netC.tcpListener, netC.dialer, s.dns, s.limiter)

	s.serving.Add(2)
	go func() {
		defer s.serving.Done()
		if err := s.udp.serve(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
		}
	}()
	go func() {
		defer s.serving.Done()
		if err := s.tcp.serve(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
		}
	}()

	return s, nil
}

// Stop cancels in-flight queries and connections, closes the sockets, and
// waits for the serving goroutines to exit or ctx to expire.
func (s *Server) Stop(ctx context.Context) error {
	s.closeOnce.Do(func() {
		// Cancel first: closing the sockets alone leaves the queries already
		// being resolved holding the relay.
		s.stopServing()
		for _, c := range []io.Closer{s.n.udp, s.n.egressUDP, s.n.tcpListener} {
			if c == nil {
				continue
			}
			if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				s.closeErr = errors.Join(s.closeErr, err)
			}
		}
	})
	stopped := make(chan struct{})
	go func() {
		s.serving.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
		return s.closeErr
	case <-ctx.Done():
		return errors.Join(s.closeErr, fmt.Errorf("dns: waiting for relay to stop: %w", ctx.Err()))
	}
}

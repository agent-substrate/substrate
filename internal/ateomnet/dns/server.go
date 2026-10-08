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
	"time"
)

type serverConfig struct {
	upstreams []string
}

type netConn struct {
	dialer      net.Dialer
	udp         net.PacketConn
	tcpListener net.Listener
}

// Server is one sandbox's running DNS relay, returned by [Relay.Serve].
type Server struct {
	config *serverConfig

	n       *netConn
	limiter *limiter

	stopServing context.CancelFunc
	closeOnce   sync.Once
	closeErr    error
	serving     sync.WaitGroup
}

func newServer(
	ctx context.Context,
	config *serverConfig,
	netC *netConn,
	limiter *limiter,
) *Server {
	// Detached from the activation RPC's context but cancelable: the relay's
	// capacity is the worker's, so teardown must drop queries still in flight.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))
	s := &Server{
		config:      config,
		stopServing: stopServing,
		n:           netC,
		limiter:     limiter,
	}
	s.serving.Add(2)
	go func() {
		defer s.serving.Done()
		if err := s.servePacket(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
		}
	}()
	go func() {
		defer s.serving.Done()
		if err := s.serveTCP(serveCtx); err != nil {
			slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
		}
	}()

	return s
}

// Stop cancels in-flight queries and connections, closes the sockets, and
// waits for the serving goroutines to exit or ctx to expire.
func (s *Server) Stop(ctx context.Context) error {
	s.closeOnce.Do(func() {
		// Cancel first: closing the sockets alone leaves the queries already
		// being resolved holding the relay.
		s.stopServing()
		for _, c := range []io.Closer{s.n.udp, s.n.tcpListener} {
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

// servePacket answers UDP queries until ctx is canceled or the socket fails.
func (s *Server) servePacket(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	buf := make([]byte, maxDNSDatagram)
	for {
		n, from, err := s.n.udp.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: reading actor DNS query: %w", err)
		}
		// Copied: the buffer is reused by the next read.
		query := make([]byte, n)
		copy(query, buf[:n])

		select {
		case s.limiter.inFlight <- struct{}{}:
		default:
			slog.DebugContext(ctx, "dns relay dropped a DNS query; too many in flight")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-s.limiter.inFlight }()
			answer, err := s.exchangeUDP(ctx, query)
			if err != nil {
				slog.WarnContext(ctx, "dns relay could not resolve an actor DNS query", slog.Any("err", err))
				return
			}
			if _, err := s.n.udp.WriteTo(answer, from); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "dns relay could not return a DNS answer", slog.Any("err", err))
			}
		}()
	}
}

// serveTCP relays TCP DNS connections until ctx is canceled or the listener closes.
func (s *Server) serveTCP(ctx context.Context) error {
	// Wait for the relays to drain before returning, so a closed listener
	// leaves no goroutine still holding a connection slot.
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := s.n.tcpListener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: accepting actor DNS connection: %w", err)
		}
		select {
		case s.limiter.connections <- struct{}{}:
		default:
			slog.DebugContext(ctx, "dns relay refused a DNS connection; too many open")
			_ = conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-s.limiter.connections }()
			s.relayTCP(ctx, conn)
		}()
	}
}

func (s *Server) exchangeUDP(ctx context.Context, query []byte) ([]byte, error) {
	var errs error
	// deferred holds a server-failure answer to fall back on, see below.
	var deferred []byte
	for _, upstream := range s.config.upstreams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := s.n.dialer.DialContext(ctx, "udp", upstream)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		answer, err := func() ([]byte, error) {
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stop()
			if err := conn.SetDeadline(time.Now().Add(dnsExchangeTimeout)); err != nil {
				return nil, err
			}
			if _, err := conn.Write(query); err != nil {
				return nil, err
			}
			buf := make([]byte, maxDNSDatagram)
			n, err := conn.Read(buf)
			if err != nil {
				return nil, err
			}
			return buf[:n], nil
		}()
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("upstream %s: %w", upstream, err))
			continue
		}
		// SERVFAIL is not an answer, and the sandbox sees only the gateway, so
		// it cannot try the pod's other resolvers itself. Keep the last one to
		// return if none does better: a real response beats a timeout.
		if rcode, ok := failoverRcode(answer); ok {
			errs = errors.Join(errs, fmt.Errorf("upstream %s: %w", upstream, rcodeError(rcode)))
			deferred = answer
			continue
		}
		return answer, nil
	}
	if deferred != nil {
		return deferred, nil
	}
	return nil, fmt.Errorf("dns: no upstream resolver answered: %w", errs)
}

// relayTCP copies a DNS stream without parsing its length-prefixed messages.
func (s *Server) relayTCP(ctx context.Context, downstream net.Conn) {
	defer downstream.Close()

	var upstream net.Conn
	var errs error
	for _, address := range s.config.upstreams {
		conn, err := s.n.dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		upstream = conn
		break
	}
	if upstream == nil {
		slog.WarnContext(ctx, "dns relay could not reach any resolver for an actor DNS connection", slog.Any("err", errs))
		return
	}
	defer upstream.Close()

	// Cancel active copies on teardown to release the worker's connection slots.
	relayDone := make(chan struct{})
	defer close(relayDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = downstream.Close()
			_ = upstream.Close()
		case <-relayDone:
		}
	}()

	deadline := time.Now().Add(dnsTCPTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = downstream.SetDeadline(deadline)
	_ = upstream.SetDeadline(deadline)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, downstream)
		if c, ok := upstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(downstream, upstream)
		if c, ok := downstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
	}()
	wg.Wait()
}

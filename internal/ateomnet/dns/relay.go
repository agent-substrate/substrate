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
	"strconv"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
)

const (
	// dnsPort is the relay port on the sandbox's gateway.
	dnsPort = 53

	// Read complete UDP datagrams without truncating EDNS responses.
	maxDNSDatagram = 65535

	// Drop excess UDP queries to bound goroutines and upstream sockets.
	maxInFlightDNS = 64

	// maxDNSConnections bounds open TCP connections.
	maxDNSConnections = 16

	// dnsTCPTimeout limits connection lifetime, including idle clients.
	dnsTCPTimeout = 30 * time.Second

	// dnsExchangeTimeout bounds each upstream attempt.
	dnsExchangeTimeout = 5 * time.Second
)

// Relay forwards UDP and TCP DNS unchanged to the worker pod's resolvers.
// It listens in the sandbox's gateway namespace and dials from the worker's.
// DNS bypasses the egress tunnel and is not checked against egress policy.
type Relay struct {
	upstreams []string
	// dialer reaches upstream resolvers from the worker namespace.
	dialer *net.Dialer

	// Limits are shared across all sandboxes using this relay.
	inFlight    chan struct{}
	connections chan struct{}
}

// NewRelay reads nameservers from resolvConfPath and forwards to each on port 53.
func NewRelay(resolvConfPath string) (*Relay, error) {
	upstreams, err := resolvConfNameservers(resolvConfPath)
	if err != nil {
		return nil, err
	}
	return NewRelayForUpstreams(upstreams)
}

// NewRelayForUpstreams forwards to upstreams, each "host:port", tried in order.
func NewRelayForUpstreams(upstreams []string) (*Relay, error) {
	if len(upstreams) == 0 {
		return nil, fmt.Errorf("dns: at least one upstream resolver is required")
	}
	for _, u := range upstreams {
		if _, _, err := net.SplitHostPort(u); err != nil {
			return nil, fmt.Errorf("dns: invalid upstream resolver %q: %w", u, err)
		}
	}

	slog.Info("DNS relay configured", slog.Any("upstreams", upstreams))

	return &Relay{
		upstreams:   upstreams,
		dialer:      &net.Dialer{Timeout: dnsExchangeTimeout},
		inFlight:    make(chan struct{}, maxInFlightDNS),
		connections: make(chan struct{}, maxDNSConnections),
	}, nil
}

// Serve serves UDP and TCP DNS in the sandbox's local gateway namespace.
func (r *Relay) Serve(ctx context.Context, ns netns.Handle) ([]io.Closer, []func(), error) {
	// Bind the wildcard because the microVM tap's gateway address is added later.
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(dnsPort))

	var packet net.PacketConn
	var stream net.Listener
	if err := netns.Do(ctx, ns, func(context.Context) error {
		pc, err := net.ListenPacket("udp", address)
		if err != nil {
			return fmt.Errorf("while opening the actor DNS socket: %w", err)
		}
		packet = pc
		l, err := net.Listen("tcp", address)
		if err != nil {
			_ = pc.Close()
			return fmt.Errorf("while opening the actor DNS listener: %w", err)
		}
		stream = l
		return nil
	}); err != nil {
		return nil, nil, err
	}

	closers, serve := r.ServeOn(ctx, packet, stream)
	return closers, serve, nil
}

// ServeOn serves UDP DNS on packet and TCP DNS on stream, which the caller has
// already bound. It returns the functions that serve, each to be run on its own
// goroutine, and the closers that stop them and release the sockets.
func (r *Relay) ServeOn(ctx context.Context, packet net.PacketConn, stream net.Listener) ([]io.Closer, []func()) {
	// Detached from the activation RPC's context but cancelable: the relay's
	// capacity is the worker's, so teardown must drop queries still in flight.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))
	serve := []func(){
		func() {
			if err := r.servePacket(serveCtx, packet); err != nil {
				slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
			}
		},
		func() {
			if err := r.serveTCP(serveCtx, stream); err != nil {
				slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
			}
		},
	}
	// Cancel first: closing the sockets alone leaves the queries already being
	// resolved holding the relay.
	closers := []io.Closer{closerFunc(func() error { stopServing(); return nil }), packet, stream}
	return closers, serve
}

// servePacket answers UDP queries until ctx is canceled or the socket fails.
func (r *Relay) servePacket(ctx context.Context, pc net.PacketConn) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = pc.Close()
		case <-done:
		}
	}()
	defer close(done)

	var wg sync.WaitGroup
	defer wg.Wait()

	buf := make([]byte, maxDNSDatagram)
	for {
		n, from, err := pc.ReadFrom(buf)
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
		case r.inFlight <- struct{}{}:
		default:
			slog.DebugContext(ctx, "dns relay dropped a DNS query; too many in flight")
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-r.inFlight }()
			answer, err := r.exchangeUDP(ctx, query)
			if err != nil {
				slog.WarnContext(ctx, "dns relay could not resolve an actor DNS query", slog.Any("err", err))
				return
			}
			if _, err := pc.WriteTo(answer, from); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "dns relay could not return a DNS answer", slog.Any("err", err))
			}
		}()
	}
}

// serveTCP relays TCP DNS connections until ctx is canceled or the listener closes.
func (r *Relay) serveTCP(ctx context.Context, listener net.Listener) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	// Wait for the relays to drain before returning, so a closed listener
	// leaves no goroutine still holding a connection slot.
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: accepting actor DNS connection: %w", err)
		}
		select {
		case r.connections <- struct{}{}:
		default:
			slog.DebugContext(ctx, "dns relay refused a DNS connection; too many open")
			_ = conn.Close()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-r.connections }()
			r.relayTCP(ctx, conn)
		}()
	}
}

func (r *Relay) exchangeUDP(ctx context.Context, query []byte) ([]byte, error) {
	var errs error
	// deferred holds a server-failure answer to fall back on, see below.
	var deferred []byte
	for _, upstream := range r.upstreams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := r.dialer.DialContext(ctx, "udp", upstream)
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
func (r *Relay) relayTCP(ctx context.Context, downstream net.Conn) {
	defer downstream.Close()

	var upstream net.Conn
	var errs error
	for _, address := range r.upstreams {
		conn, err := r.dialer.DialContext(ctx, "tcp", address)
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

// Response codes that say the resolver failed rather than answered. NXDOMAIN
// and NOERROR are answers and are passed back as they are.
const (
	rcodeServFail = 2
	rcodeNotImp   = 4
	rcodeRefused  = 5
)

// failoverRcode reports the response code when the relay should try the next
// upstream. Reads the 12-byte header only; anything shorter is passed through.
func failoverRcode(msg []byte) (byte, bool) {
	if len(msg) < 12 {
		return 0, false
	}
	rcode := msg[3] & 0x0f
	switch rcode {
	case rcodeServFail, rcodeNotImp, rcodeRefused:
		return rcode, true
	}
	return 0, false
}

func rcodeError(rcode byte) error {
	switch rcode {
	case rcodeServFail:
		return errors.New("answered SERVFAIL")
	case rcodeNotImp:
		return errors.New("answered NOTIMP")
	case rcodeRefused:
		return errors.New("answered REFUSED")
	}
	return fmt.Errorf("answered rcode %d", rcode)
}

// closerFunc adapts a cancel function to io.Closer, so a caller takes a
// sandbox's sockets and the work behind them down as one list.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

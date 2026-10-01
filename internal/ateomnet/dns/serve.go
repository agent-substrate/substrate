//go:build linux

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
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
)

// Server answers an actor's DNS. Satisfied by atunnel.DNSRelay; an interface
// so this package does not depend on it.
type Server interface {
	ServePacket(ctx context.Context, pc net.PacketConn) error
	Serve(ctx context.Context, listener net.Listener) error
}

// Serve serves UDP and TCP DNS in the sandbox's local gateway namespace.
func Serve(ctx context.Context, relay Server, ns netns.Handle, port uint16) ([]io.Closer, []func(), error) {
	// Bind the wildcard because the microVM tap's gateway address is added later.
	address := net.JoinHostPort("0.0.0.0", strconv.Itoa(int(port)))

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

	// Detached from the activation RPC's context but cancelable: the relay's
	// capacity is the worker's, so teardown must drop queries still in flight.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))
	serve := []func(){
		func() {
			if err := relay.ServePacket(serveCtx, packet); err != nil {
				slog.WarnContext(ctx, "Actor DNS socket stopped", slog.Any("err", err))
			}
		},
		func() {
			if err := relay.Serve(serveCtx, stream); err != nil {
				slog.WarnContext(ctx, "Actor DNS listener stopped", slog.Any("err", err))
			}
		},
	}
	// Cancel first: closing the sockets alone leaves the queries already being
	// resolved holding the relay.
	closers := []io.Closer{closerFunc(func() error { stopServing(); return nil }), packet, stream}
	return closers, serve, nil
}

// closerFunc adapts a cancel function to io.Closer, so a caller takes a
// sandbox's sockets and the work behind them down as one list.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

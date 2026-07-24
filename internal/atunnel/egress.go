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

package atunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/agent-substrate/substrate/internal/resources"
)

// EgressDialer opens an authenticated tunnel to an original destination.
type EgressDialer interface {
	DialContext(context.Context, string, EgressMetadata) (net.Conn, error)
}

// OriginalDestination returns the address that a transparently intercepted
// connection originally targeted.
type OriginalDestination func(net.Conn) (string, error)

// Egress proxies actor TCP connections through an egress CONNECT dialer. It is
// long-lived across actor activations, but only carries traffic while an actor
// is assigned to its worker.
type Egress struct {
	originalDestination OriginalDestination

	mu     sync.Mutex
	active *egressActivation
}

type egressActivation struct {
	metadata EgressMetadata
	dialer   EgressDialer
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewEgress creates an activation-aware egress proxy.
func NewEgress(originalDestination OriginalDestination) (*Egress, error) {
	if originalDestination == nil {
		return nil, fmt.Errorf("atunnel: original destination resolver is required")
	}
	return &Egress{
		originalDestination: originalDestination,
	}, nil
}

// Serve accepts intercepted actor connections until ctx is canceled or the
// listener fails.
func (e *Egress) Serve(ctx context.Context, listener net.Listener) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("atunnel: accepting actor egress connection: %w", err)
		}
		e.handle(conn)
	}
}

// Activate allows egress for one actor. There can be only one active actor per
// worker. bearerToken may be empty until actor JWT issuance is available.
func (e *Egress) Activate(dialer EgressDialer, atespace, actorName string, actorVersion int64, bearerToken string) error {
	if dialer == nil {
		return fmt.Errorf("atunnel: egress dialer is required")
	}
	if !resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(actorName) {
		return fmt.Errorf("atunnel: invalid actor identity %q/%q", atespace, actorName)
	}
	if actorVersion < 1 {
		return fmt.Errorf("atunnel: actor version must be positive")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		return fmt.Errorf("atunnel: actor %s/%s already has active egress", e.active.metadata.Atespace, e.active.metadata.ActorName)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.active = &egressActivation{
		metadata: EgressMetadata{
			Atespace:     atespace,
			ActorName:    actorName,
			ActorVersion: actorVersion,
			BearerToken:  bearerToken,
		},
		dialer: dialer,
		ctx:    ctx,
		cancel: cancel,
	}
	return nil
}

// Deactivate rejects new egress, closes active streams, and waits for their
// forwarding goroutines to exit.
func (e *Egress) Deactivate(ctx context.Context) error {
	e.mu.Lock()
	active := e.active
	e.active = nil
	if active != nil {
		active.cancel()
	}
	e.mu.Unlock()
	if active == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		active.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("atunnel: waiting for active egress streams to stop: %w", ctx.Err())
	}
}

func (e *Egress) handle(downstream net.Conn) {
	e.mu.Lock()
	active := e.active
	if active == nil {
		e.mu.Unlock()
		_ = downstream.Close()
		return
	}
	active.wg.Add(1)
	e.mu.Unlock()

	go func() {
		defer active.wg.Done()
		defer downstream.Close()

		destination, err := e.originalDestination(downstream)
		if err != nil {
			slog.WarnContext(active.ctx, "atunnel failed to resolve original egress destination", slog.Any("err", err))
			return
		}
		upstream, err := active.dialer.DialContext(active.ctx, destination, active.metadata)
		if err != nil {
			slog.WarnContext(active.ctx, "atunnel failed to open egress tunnel", slog.String("destination", destination), slog.Any("err", err))
			return
		}
		defer upstream.Close()

		stop := context.AfterFunc(active.ctx, func() {
			_ = downstream.Close()
			_ = upstream.Close()
		})
		defer stop()

		copyBothWays(downstream, upstream)
	}()
}

func copyBothWays(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(a, b)
		closeWrite(a)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(b, a)
		closeWrite(b)
		done <- struct{}{}
	}()
	<-done
	<-done
}

func closeWrite(conn net.Conn) {
	if conn, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = conn.CloseWrite()
	}
}

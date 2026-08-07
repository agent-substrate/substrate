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
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// egressDialer opens an authenticated tunnel to an original destination.
type egressDialer interface {
	DialContext(context.Context, string) (net.Conn, error)
}

type actorCertificateSource interface {
	Mint(context.Context) (time.Time, error)
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
	dialer            egressDialer
	certificateSource actorCertificateSource
	expiresAt         time.Time

	// ctx scopes certificate renewal and every tunnel opened by this activation. wg
	// lets Deactivate wait until both renewal and tunnel forwarding have exited.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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

// Activate allows egress with a previously obtained actor certificate and
// renews it until deactivation.
func (e *Egress) Activate(dialer egressDialer, certificateSource actorCertificateSource, expiresAt time.Time) error {
	if dialer == nil {
		return fmt.Errorf("atunnel: egress dialer is required")
	}
	if certificateSource == nil {
		return fmt.Errorf("atunnel: actor certificate source is required")
	}
	if !expiresAt.After(time.Now()) {
		return fmt.Errorf("atunnel: valid actor certificate is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		return fmt.Errorf("atunnel: actor already has active egress")
	}
	activationCtx, cancel := context.WithCancel(context.Background())
	active := &egressActivation{
		dialer:            dialer,
		certificateSource: certificateSource,
		expiresAt:         expiresAt,
		ctx:               activationCtx,
		cancel:            cancel,
	}
	e.active = active
	active.wg.Add(1)
	go e.renew(active, expiresAt)
	return nil
}

func (e *Egress) renew(active *egressActivation, expiresAt time.Time) {
	defer active.wg.Done()
	// Schedule from the credential's remaining lifetime: renew at 90%, then
	// keep retrying after expiry so egress can recover without reactivation.
	delay := renewAfter(expiresAt)
	expired := false
	for waitForRenewal(active.ctx, delay) {
		if !expiresAt.After(time.Now()) && !expired {
			slog.WarnContext(active.ctx, "Atunnel actor certificate expired; blocking new egress connections",
				slog.Time("expiredAt", expiresAt))
			expired = true
		}
		nextExpiry, err := active.certificateSource.Mint(active.ctx)
		if err != nil {
			code := status.Code(err)
			if code == codes.FailedPrecondition || code == codes.PermissionDenied {
				e.mu.Lock()
				active.expiresAt = time.Time{}
				e.mu.Unlock()
				slog.WarnContext(active.ctx, "Atunnel actor certificate renewal was denied; blocking new egress connections",
					slog.Any("err", err))
				return
			}
			delay = retryAfter(expiresAt)
			continue
		}
		if !nextExpiry.After(time.Now()) {
			delay = retryAfter(expiresAt)
			continue
		}
		e.mu.Lock()
		// Check cancellation under the same lock as Deactivate. Whichever wins
		// the lock last either installs a live expiry or leaves the activation empty;
		// renewal can never restore a credential after deactivation cleared it.
		if active.ctx.Err() != nil {
			e.mu.Unlock()
			return
		}
		active.expiresAt = nextExpiry
		e.mu.Unlock()
		if expired {
			slog.InfoContext(active.ctx, "Atunnel actor certificate renewed; allowing new egress connections",
				slog.Time("expiresAt", nextExpiry))
			expired = false
		}
		expiresAt = nextExpiry
		delay = renewAfter(expiresAt)
	}
}

func renewAfter(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	return remaining - remaining/10
}

func retryAfter(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return 25*time.Second + rand.N(10*time.Second)
	}
	return min(30*time.Second, max(time.Second, remaining/10), remaining)
}

func waitForRenewal(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Deactivate rejects new egress, closes active streams, and waits for their
// forwarding goroutines to exit.
func (e *Egress) Deactivate(ctx context.Context) error {
	e.mu.Lock()
	active := e.active
	e.active = nil
	if active != nil {
		active.expiresAt = time.Time{}
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
	if time.Now().Compare(active.expiresAt) >= 0 {
		// Expiry blocks only new tunnels. Connections admitted with a valid
		// certificate have completed mTLS and are allowed to drain normally.
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
		upstream, err := active.dialer.DialContext(active.ctx, destination)
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

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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEgressActivationFailsClosed(t *testing.T) {
	egress, err := NewEgress(func(net.Conn) (string, error) { return "", nil })
	if err != nil {
		t.Fatal(err)
	}
	dialer := egressDialerFunc(func(context.Context, string) (net.Conn, error) {
		t.Fatal("dialed after failed activation")
		return nil, nil
	})
	if err := egress.Activate(dialer, fakeActorCertificateSource{err: errors.New("renewal failed")}, time.Time{}); err == nil {
		t.Fatal("Activate() succeeded")
	}
	actor, proxy := net.Pipe()
	defer actor.Close()
	if err := actor.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	egress.handle(proxy)
	if _, err := actor.Read(make([]byte, 1)); err == nil {
		t.Fatal("failed activation admitted egress")
	}
}

func TestEgressExpiryRejectsNewButPreservesEstablished(t *testing.T) {
	upstreamProxy, upstreamGateway := net.Pipe()
	defer upstreamGateway.Close()
	var dials atomic.Int32
	var mints atomic.Int32
	dialer := egressDialerFunc(func(context.Context, string) (net.Conn, error) {
		dials.Add(1)
		return upstreamProxy, nil
	})
	egress, err := NewEgress(func(net.Conn) (string, error) { return "192.0.2.10:443", nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := egress.Activate(dialer, fakeActorCertificateSource{err: errors.New("renewal failed"), calls: &mints}, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	actor, proxy := net.Pipe()
	defer actor.Close()
	egress.handle(proxy)
	deadline := time.Now().Add(time.Second)
	for dials.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	time.Sleep(100 * time.Millisecond)
	go func() { _, _ = actor.Write([]byte("still-open")) }()
	buf := make([]byte, len("still-open"))
	if _, err := io.ReadFull(upstreamGateway, buf); err != nil {
		t.Fatalf("established tunnel closed after certificate expiry: %v", err)
	}

	newActor, newProxy := net.Pipe()
	defer newActor.Close()
	if err := newActor.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	egress.handle(newProxy)
	if _, err := newActor.Read(make([]byte, 1)); err == nil {
		t.Fatal("new tunnel admitted after certificate expiry")
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d after expiry, want 1", dials.Load())
	}
	if got := mints.Load(); got > 3 {
		t.Fatalf("mint attempts = %d, retry loop spun near expiry", got)
	}
	_ = egress.Deactivate(context.Background())
}

func TestEgressRenewsBeforeExpiry(t *testing.T) {
	var mints atomic.Int32
	renewed := make(chan struct{}, 1)
	renewedExpiry := time.Now().Add(time.Hour)
	source := fakeActorCertificateSource{
		expiresAt: renewedExpiry,
		calls:     &mints,
		called:    renewed,
	}
	upstream, gateway := net.Pipe()
	defer gateway.Close()
	dialer := egressDialerFunc(func(context.Context, string) (net.Conn, error) {
		return upstream, nil
	})
	egress, err := NewEgress(func(net.Conn) (string, error) { return "192.0.2.10:443", nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := egress.Activate(dialer, source, time.Now().Add(80*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-renewed:
	case <-time.After(time.Second):
		t.Fatal("certificate was not renewed")
	}
	deadline := time.Now().Add(time.Second)
	for {
		egress.mu.Lock()
		expiresAt := egress.active.expiresAt
		egress.mu.Unlock()
		if expiresAt.Equal(renewedExpiry) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renewed certificate expiry was not installed")
		}
		time.Sleep(time.Millisecond)
	}
	actor, proxy := net.Pipe()
	defer actor.Close()
	egress.handle(proxy)
	_ = egress.Deactivate(context.Background())
}

func TestEgressRetriesRenewalAfterExpiry(t *testing.T) {
	for range 100 {
		if got := retryAfter(time.Now().Add(-time.Second)); got < 25*time.Second || got >= 35*time.Second {
			t.Fatalf("retryAfter(expired) = %v, want [25s, 35s)", got)
		}
	}
}

func TestEgressStopsAfterTerminalRenewalFailure(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			called := make(chan struct{}, 1)
			egress, err := NewEgress(func(net.Conn) (string, error) { return "", nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := egress.Activate(egressDialerFunc(func(context.Context, string) (net.Conn, error) {
				t.Fatal("dialed after renewal was denied")
				return nil, nil
			}), fakeActorCertificateSource{
				err:    fmt.Errorf("mint: %w", status.Error(code, "stale activation")),
				called: called,
			}, time.Now().Add(50*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-called:
			case <-time.After(time.Second):
				t.Fatal("certificate renewal did not start")
			}

			deadline := time.Now().Add(time.Second)
			for {
				egress.mu.Lock()
				expiresAt := egress.active.expiresAt
				egress.mu.Unlock()
				if expiresAt.IsZero() {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("terminal renewal failure did not block new egress")
				}
				time.Sleep(time.Millisecond)
			}
			_ = egress.Deactivate(context.Background())
		})
	}
}

func TestEgressDeactivationDropsConcurrentRenewal(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	egress, err := NewEgress(func(net.Conn) (string, error) { return "", nil })
	if err != nil {
		t.Fatal(err)
	}
	dialer := egressDialerFunc(func(context.Context, string) (net.Conn, error) { return nil, nil })
	if err := egress.Activate(dialer, fakeActorCertificateSource{
		expiresAt: time.Now().Add(time.Hour),
		called:    started,
		release:   release,
	}, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	egress.mu.Lock()
	active := egress.active
	egress.mu.Unlock()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("certificate renewal did not start")
	}
	done := make(chan error, 1)
	go func() { done <- egress.Deactivate(context.Background()) }()
	<-active.ctx.Done()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !active.expiresAt.IsZero() {
		t.Fatalf("deactivated certificate expiry = %v, want zero", active.expiresAt)
	}
}

func TestEgressEndToEnd(t *testing.T) {
	ca := newTestCA(t)
	requests := make(chan *http.Request, 1)
	gatewayDone := make(chan struct{})
	gatewayAddress := serveTestConnectGateway(t, ca, func(conn net.Conn, req *http.Request) {
		defer close(gatewayDone)
		requests <- req

		tlsConn, ok := conn.(*tls.Conn)
		if !ok {
			t.Errorf("gateway connection has type %T, want *tls.Conn", conn)
		} else {
			peer := tlsConn.ConnectionState().PeerCertificates[0]
			if len(peer.URIs) != 1 || peer.URIs[0].String() != "spiffe://substrate-actor.local/atespace/team/actor/actor" {
				t.Errorf("client identity = %v, want actor SPIFFE ID", peer.URIs)
			}
		}

		if _, err := io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Errorf("writing CONNECT response: %v", err)
			return
		}
		payload := make([]byte, len("from actor"))
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Errorf("reading actor payload: %v", err)
			return
		}
		if string(payload) != "from actor" {
			t.Errorf("gateway payload = %q, want %q", payload, "from actor")
		}
		if _, err := io.WriteString(conn, "from gateway"); err != nil {
			t.Errorf("writing gateway payload: %v", err)
		}
	})
	client := newTestClient(t, ca, WithDialer(dialFixedAddress(gatewayAddress)))

	egress, err := NewEgress(func(net.Conn) (string, error) {
		return "192.0.2.10:443", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := egress.Activate(client, fakeActorCertificateSource{expiresAt: time.Now().Add(time.Hour)}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	downstreamActor, downstreamProxy := net.Pipe()
	t.Cleanup(func() {
		_ = downstreamActor.Close()
		_ = downstreamProxy.Close()
	})
	egress.handle(downstreamProxy)

	req := <-requests
	if req.Method != http.MethodConnect || req.Host != "192.0.2.10:443" {
		t.Errorf("request = %s %s, want CONNECT 192.0.2.10:443", req.Method, req.Host)
	}
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("Authorization = %q, want empty", got)
	}
	for name := range req.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-ate-") {
			t.Errorf("legacy identity header %q was sent", name)
		}
	}

	if err := downstreamActor.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(downstreamActor, "from actor"); err != nil {
		t.Fatal(err)
	}
	gotAtActor := make([]byte, len("from gateway"))
	if _, err := io.ReadFull(downstreamActor, gotAtActor); err != nil {
		t.Fatal(err)
	}
	if string(gotAtActor) != "from gateway" {
		t.Errorf("actor payload = %q, want %q", gotAtActor, "from gateway")
	}
	<-gatewayDone

	if err := egress.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEgressRejectsInactiveConnection(t *testing.T) {
	egress, err := NewEgress(func(net.Conn) (string, error) {
		t.Fatal("inactive egress resolved destination")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	actor, proxy := net.Pipe()
	defer actor.Close()
	if err := actor.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	egress.handle(proxy)
	if _, err := actor.Read(make([]byte, 1)); err == nil {
		t.Fatal("inactive connection remained open")
	}
}

type egressDialerFunc func(context.Context, string) (net.Conn, error)

func (f egressDialerFunc) DialContext(ctx context.Context, destination string) (net.Conn, error) {
	return f(ctx, destination)
}

type fakeActorCertificateSource struct {
	expiresAt time.Time
	err       error
	calls     *atomic.Int32
	called    chan<- struct{}
	release   <-chan struct{}
}

func (s fakeActorCertificateSource) Mint(context.Context) (time.Time, error) {
	if s.calls != nil {
		s.calls.Add(1)
	}
	if s.called != nil {
		select {
		case s.called <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		<-s.release
	}
	return s.expiresAt, s.err
}

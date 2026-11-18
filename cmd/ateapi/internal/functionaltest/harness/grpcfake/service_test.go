// Copyright 2026 The Agent Substrate Authors
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

package grpcfake_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness/grpcfake"
)

type method string

const (
	healthCheck = method(healthpb.Health_Check_FullMethodName)
	healthList  = method(healthpb.Health_List_FullMethodName)
)

// healthServer counts the calls that reach it. Check overwrites its request,
// so a recording that kept the request it was given would see the change.
type healthServer struct {
	healthpb.UnimplementedHealthServer
	mu    sync.Mutex
	calls int
}

func (s *healthServer) Check(_ context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	req.Service = "changed by the handler"
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (s *healthServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return &healthpb.HealthListResponse{}, nil
}

// made returns how many calls reached s.
func (s *healthServer) made() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// fixture is a health server behind a Service that instruments Check, and a
// client connected to it in memory.
type fixture struct {
	svc    *grpcfake.Service[method]
	srv    *grpc.Server
	health *healthServer
	client healthpb.HealthClient
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{svc: grpcfake.NewService(healthCheck), health: &healthServer{}}
	f.srv = f.svc.NewServer()
	healthpb.RegisterHealthServer(f.srv, f.health)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = f.srv.Serve(lis) }()
	t.Cleanup(f.srv.Stop)
	conn, err := grpc.NewClient("passthrough:///in-memory",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatalf("connecting in memory: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	f.client = healthpb.NewHealthClient(conn)
	return f
}

// check calls Check for service.
func (f *fixture) check(ctx context.Context, service string) error {
	_, err := f.client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	return err
}

// checkAsync calls Check in the background and returns its error on the
// channel.
func (f *fixture) checkAsync(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- f.check(ctx, "") }()
	return done
}

func TestService(t *testing.T) {
	t.Run("a call with no fault reaches the service", func(t *testing.T) {
		f := newFixture(t)
		if err := f.check(t.Context(), ""); err != nil || f.health.made() != 1 {
			t.Errorf("Check = %v after %d calls; want OK after one call", err, f.health.made())
		}
	})

	t.Run("FailNext fails exactly one call without reaching the service", func(t *testing.T) {
		f := newFixture(t)
		f.svc.On(healthCheck).FailNext(status.New(codes.Unavailable, "injected"))
		if err := f.check(t.Context(), ""); status.Code(err) != codes.Unavailable || f.health.made() != 0 {
			t.Errorf("first Check = %v after %d calls; want Unavailable and no call", err, f.health.made())
		}
		if err := f.check(t.Context(), ""); err != nil || f.health.made() != 1 {
			t.Errorf("second Check = %v after %d calls; want OK after one call", err, f.health.made())
		}
	})

	t.Run("faults apply to the next calls in the order they were added", func(t *testing.T) {
		f := newFixture(t)
		f.svc.On(healthCheck).FailNext(status.New(codes.Unavailable, "injected"))
		f.svc.On(healthCheck).FailNext(status.New(codes.Internal, "injected"))
		var got []codes.Code
		for range 3 {
			got = append(got, status.Code(f.check(t.Context(), "")))
		}
		if diff := cmp.Diff([]codes.Code{codes.Unavailable, codes.Internal, codes.OK}, got); diff != "" {
			t.Errorf("codes (-want +got):\n%s", diff)
		}
	})

	t.Run("FailNext panics on a status that is nil or OK", func(t *testing.T) {
		for _, st := range []*status.Status{nil, status.New(codes.OK, "")} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("FailNext(%v) did not panic", st)
					}
				}()
				newFixture(t).svc.On(healthCheck).FailNext(st)
			}()
		}
	})

	t.Run("Block holds the call until Release", func(t *testing.T) {
		f := newFixture(t)
		gate := f.svc.On(healthCheck).Block()
		done := f.checkAsync(t.Context())
		gate.Wait(t)
		select {
		case err := <-done:
			t.Fatalf("Check returned %v before Release", err)
		default:
		}
		if f.health.made() != 0 {
			t.Errorf("a blocked call reached the service")
		}
		gate.Release()
		if err := <-done; err != nil || f.health.made() != 1 {
			t.Errorf("released Check = %v after %d calls; want OK after one call", err, f.health.made())
		}
	})

	t.Run("a blocked call ends with its context without reaching the service", func(t *testing.T) {
		f := newFixture(t)
		gate := f.svc.On(healthCheck).Block()
		ctx, cancel := context.WithCancel(t.Context())
		done := f.checkAsync(ctx)
		gate.Wait(t)
		cancel()
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Errorf("canceled Check = %v, want Canceled", err)
		}
		// GracefulStop waits for the server to finish the canceled call.
		f.srv.GracefulStop()
		if f.health.made() != 0 {
			t.Errorf("a canceled blocked call reached the service")
		}
	})

	t.Run("BlockReply lets the call reach the service and holds its reply until Release", func(t *testing.T) {
		f := newFixture(t)
		gate := f.svc.On(healthCheck).BlockReply()
		done := f.checkAsync(t.Context())
		gate.Wait(t)
		if f.health.made() != 1 {
			t.Errorf("a call whose reply is held made %d calls, want 1", f.health.made())
		}
		select {
		case err := <-done:
			t.Fatalf("Check returned %v before Release", err)
		default:
		}
		gate.Release()
		if err := <-done; err != nil {
			t.Errorf("released Check = %v, want OK", err)
		}
	})

	t.Run("a held reply ends with its context after the call reached the service", func(t *testing.T) {
		f := newFixture(t)
		gate := f.svc.On(healthCheck).BlockReply()
		ctx, cancel := context.WithCancel(t.Context())
		done := f.checkAsync(ctx)
		gate.Wait(t)
		cancel()
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Errorf("canceled Check = %v, want Canceled", err)
		}
		// GracefulStop waits for the server to finish the canceled call.
		f.srv.GracefulStop()
		if f.health.made() != 1 {
			t.Errorf("a canceled call whose reply was held made %d calls, want 1", f.health.made())
		}
	})

	t.Run("Record holds a copy of every call from then on, faulted or not", func(t *testing.T) {
		f := newFixture(t)
		if err := f.check(t.Context(), "before"); err != nil {
			t.Fatalf("Check: %v", err)
		}
		rec := f.svc.Record()
		f.svc.On(healthCheck).FailNext(status.New(codes.Unavailable, "injected"))
		if err := f.check(t.Context(), "faulted"); err == nil {
			t.Fatal("faulted Check succeeded")
		}
		if err := f.check(t.Context(), "made"); err != nil {
			t.Fatalf("Check: %v", err)
		}
		want := []grpcfake.Call[method]{
			{Method: healthCheck, Request: &healthpb.HealthCheckRequest{Service: "faulted"}},
			{Method: healthCheck, Request: &healthpb.HealthCheckRequest{Service: "made"}},
		}
		if diff := cmp.Diff(want, rec.Calls(), protocmp.Transform()); diff != "" {
			t.Errorf("recorded calls (-want +got):\n%s", diff)
		}
	})

	t.Run("a method the Service does not instrument passes through unrecorded", func(t *testing.T) {
		f := newFixture(t)
		rec := f.svc.Record()
		if _, err := f.client.List(t.Context(), &healthpb.HealthListRequest{}); err != nil || f.health.made() != 1 {
			t.Errorf("List = %v after %d calls; want OK after one call", err, f.health.made())
		}
		if calls := rec.Calls(); len(calls) != 0 {
			t.Errorf("recorded %v, want no calls", calls)
		}
	})
}

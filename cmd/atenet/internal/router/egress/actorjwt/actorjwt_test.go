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

package actorjwt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var testActor = resources.ActorRef{Atespace: "default", Name: "my-actor"}

// fakeControl's nth MintActorJWT call returns "jwt-<n>", expiring
// expiration_seconds from now. err, when set, is returned instead, and gate,
// when non-nil, blocks each call until it is closed.
type fakeControl struct {
	ateapipb.UnimplementedControlServer
	calls atomic.Int32
	last  atomic.Pointer[ateapipb.MintActorJWTRequest]
	err   error
	gate  chan struct{}
}

func (f *fakeControl) MintActorJWT(ctx context.Context, req *ateapipb.MintActorJWTRequest) (*ateapipb.MintActorJWTResponse, error) {
	n := f.calls.Add(1)
	f.last.Store(req)
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.MintActorJWTResponse{
		ActorJwt:   fmt.Sprintf("jwt-%d", n),
		ExpireTime: timestamppb.New(time.Now().Add(time.Duration(req.GetExpirationSeconds()) * time.Second)),
	}, nil
}

// newMinter returns a Minter whose client reaches ctl over an in-memory gRPC
// connection.
func newMinter(t *testing.T, ctl *fakeControl) *Minter {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	ateapipb.RegisterControlServer(srv, ctl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return New(ateapipb.NewControlClient(conn))
}

func jwtSource(lifetime int64, audiences ...string) *ateapipb.ActorJWTSource {
	return &ateapipb.ActorJWTSource{Audiences: audiences, ExpirationSeconds: lifetime}
}

// wantToken calls m.Token and fails the test unless it returns want.
func wantToken(t *testing.T, m *Minter, ref resources.ActorRef, src *ateapipb.ActorJWTSource, want string) {
	t.Helper()
	got, err := m.Token(context.Background(), ref, src)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if got != want {
		t.Fatalf("Token = %q, want %q", got, want)
	}
}

func TestReuseTokenUntilAThirdOfItsLifetimeRemains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctl := &fakeControl{}
		m := newMinter(t, ctl)
		src := jwtSource(900, "https://b.example", "https://a.example")

		wantToken(t, m, testActor, src, "jwt-1")
		want := &ateapipb.MintActorJWTRequest{
			Actor:             &ateapipb.ObjectRef{Atespace: "default", Name: "my-actor"},
			Audiences:         []string{"https://a.example", "https://b.example"},
			ExpirationSeconds: 900,
		}
		if got := ctl.last.Load(); !proto.Equal(got, want) {
			t.Errorf("MintActorJWT request = %v, want %v", got, want)
		}

		time.Sleep(600*time.Second - time.Nanosecond)
		wantToken(t, m, testActor, src, "jwt-1")
		time.Sleep(time.Nanosecond)
		wantToken(t, m, testActor, src, "jwt-2")
	})
}

func TestKeyOnActorAudiencesAndLifetime(t *testing.T) {
	m := newMinter(t, &fakeControl{})
	other := resources.ActorRef{Atespace: "default", Name: "other-actor"}

	wantToken(t, m, testActor, jwtSource(900, "a", "b"), "jwt-1")
	wantToken(t, m, testActor, jwtSource(900, "b", "a"), "jwt-1")
	wantToken(t, m, testActor, jwtSource(900, "a"), "jwt-2")
	wantToken(t, m, testActor, jwtSource(600, "a", "b"), "jwt-3")
	wantToken(t, m, other, jwtSource(900, "a", "b"), "jwt-4")
	wantToken(t, m, testActor, jwtSource(900, "a b"), "jwt-5")
}

func TestDoNotCacheErrors(t *testing.T) {
	ctl := &fakeControl{err: status.Error(codes.Unavailable, "ateapi is down")}
	m := newMinter(t, ctl)
	src := jwtSource(900, "a")

	if _, err := m.Token(context.Background(), testActor, src); status.Code(err) != codes.Unavailable {
		t.Fatalf("Token = %v, want the Unavailable MintActorJWT error", err)
	}
	ctl.err = nil
	wantToken(t, m, testActor, src, "jwt-2")
}

func TestCollapseConcurrentMints(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctl := &fakeControl{gate: make(chan struct{})}
		m := newMinter(t, ctl)

		const callers = 8
		var wg sync.WaitGroup
		for range callers {
			wg.Go(func() {
				if _, err := m.Token(context.Background(), testActor, jwtSource(900, "a")); err != nil {
					t.Errorf("Token: %v", err)
				}
			})
		}
		synctest.Wait()
		close(ctl.gate)
		wg.Wait()
		if calls := ctl.calls.Load(); calls != 1 {
			t.Errorf("MintActorJWT calls = %d, want 1 for %d concurrent callers", calls, callers)
		}
	})
}

// The leader's cancellation must not fail the mint it started, and the token
// still lands in the cache.
func TestMintOutlivesCanceledCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctl := &fakeControl{gate: make(chan struct{})}
		m := newMinter(t, ctl)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := m.Token(ctx, testActor, jwtSource(900, "a"))
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller got %v, want context.Canceled", err)
		}

		close(ctl.gate)
		synctest.Wait()
		wantToken(t, m, testActor, jwtSource(900, "a"), "jwt-1")
		if calls := ctl.calls.Load(); calls != 1 {
			t.Errorf("MintActorJWT calls = %d, want 1", calls)
		}
	})
}

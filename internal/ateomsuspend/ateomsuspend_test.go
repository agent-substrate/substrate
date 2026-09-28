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

package ateomsuspend

import (
	"context"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/testing/protocmp"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// capturingAtelet stands in for the node-local atelet. It records every
// request it is sent, and answers the nth with errs[n]; once errs runs out it
// accepts. That is how a control plane that is briefly out of reach and then
// comes back is spelled here.
type capturingAtelet struct {
	ateletpb.UnimplementedAteomSupportServer

	errs []error

	mu  sync.Mutex
	got []*ateletpb.RequestActorSuspendRequest
}

func (a *capturingAtelet) RequestActorSuspend(_ context.Context, in *ateletpb.RequestActorSuspendRequest) (*ateletpb.RequestActorSuspendResponse, error) {
	a.mu.Lock()
	attempt := len(a.got)
	a.got = append(a.got, in)
	a.mu.Unlock()

	if attempt < len(a.errs) {
		return nil, a.errs[attempt]
	}
	return &ateletpb.RequestActorSuspendResponse{}, nil
}

// requests returns what the atelet was sent, including attempts it refused.
func (a *capturingAtelet) requests() []*ateletpb.RequestActorSuspendRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.got)
}

// testBackoff retries as many times as production does, without the waiting.
func testBackoff() wait.Backoff {
	return wait.Backoff{Steps: retrySteps, Duration: time.Millisecond}
}

// dialCapturingAtelet serves an in-process AteomSupport over bufconn, which
// exercises the request without a socket or certificates.
func dialCapturingAtelet(t *testing.T, atelet *capturingAtelet) *grpc.ClientConn {
	t.Helper()
	srv := grpc.NewServer()
	ateletpb.RegisterAteomSupportServer(srv, atelet)
	lis := bufconn.Listen(1 << 20)
	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("fake atelet server exited: %v", err)
		}
	}()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("connecting to the fake atelet: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestRequestSuspendNamesTheActor(t *testing.T) {
	atelet := &capturingAtelet{}
	conn := dialCapturingAtelet(t, atelet)

	actor := Actor{Atespace: "team-a", Name: "actor-1", UID: "actor-uid-1"}
	if err := requestWithRetry(context.Background(), conn, actor, testBackoff()); err != nil {
		t.Fatalf("requestWithRetry() failed: %v", err)
	}

	// The UID pins the request to the incarnation this ateom hosts, so it
	// cannot outlive the actor and suspend whatever took its name.
	want := []*ateletpb.RequestActorSuspendRequest{{
		ActorAtespace: "team-a",
		ActorName:     "actor-1",
		ActorUid:      "actor-uid-1",
	}}
	if diff := cmp.Diff(want, atelet.requests(), protocmp.Transform()); diff != "" {
		t.Errorf("request mismatch (-want +got):\n%s", diff)
	}
}

// The reason the retry exists: a caller that pushed an idleness signal may
// never push another, so a control plane that was briefly out of reach must
// not be what costs the platform a worker slot.
func TestRequestSuspendRidesOutATransientFailure(t *testing.T) {
	atelet := &capturingAtelet{errs: []error{
		status.Error(codes.Unavailable, "atelet restarting"),
		status.Error(codes.Aborted, "concurrent update conflict, please retry"),
	}}
	conn := dialCapturingAtelet(t, atelet)

	if err := requestWithRetry(context.Background(), conn, Actor{Atespace: "team-a", Name: "actor-1", UID: "actor-uid-1"}, testBackoff()); err != nil {
		t.Fatalf("requestWithRetry() failed: %v", err)
	}
	if got := len(atelet.requests()); got != 3 {
		t.Errorf("asked %d times, want 3 (two refusals then the accepted one)", got)
	}
}

// A refusal is an answer. Asking again cannot change it, and the caller needs
// to tell a decision from a failure to ask.
func TestRequestSuspendDoesNotRetryADecision(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not hosted here", status.Error(codes.NotFound, "Actor team-a/actor-1 not found")},
		{"lost a race", status.Error(codes.FailedPrecondition, "Actor is RESUMING")},
		{"malformed", status.Error(codes.InvalidArgument, "actor_uid is required")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// One more refusal than there are attempts, so a retry would be
			// answered rather than accepted and would show up in the count.
			atelet := &capturingAtelet{errs: slices.Repeat([]error{tc.err}, retrySteps+1)}
			conn := dialCapturingAtelet(t, atelet)

			err := requestWithRetry(context.Background(), conn, Actor{Atespace: "team-a", Name: "actor-1", UID: "actor-uid-1"}, testBackoff())
			if got, want := status.Code(err), status.Code(tc.err); got != want {
				t.Errorf("code = %v (err %v), want %v", got, err, want)
			}
			if got := len(atelet.requests()); got != 1 {
				t.Errorf("asked %d times, want 1: a refusal is not retried", got)
			}
		})
	}
}

// The budget is bounded. An idleness signal decays, so a request that cannot
// be delivered is eventually dropped rather than landing on an actor that has
// since picked up work.
func TestRequestSuspendGivesUpAndSaysWhy(t *testing.T) {
	unavailable := status.Error(codes.Unavailable, "atelet restarting")
	atelet := &capturingAtelet{errs: slices.Repeat([]error{unavailable}, retrySteps+1)}
	conn := dialCapturingAtelet(t, atelet)

	err := requestWithRetry(context.Background(), conn, Actor{Atespace: "team-a", Name: "actor-1", UID: "actor-uid-1"}, testBackoff())
	if err == nil {
		t.Fatal("requestWithRetry() succeeded against an atelet that refused every attempt")
	}
	// What kept failing, not just that time passed.
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("code = %v (err %v), want Unavailable", got, err)
	}
	if got := len(atelet.requests()); got != retrySteps {
		t.Errorf("asked %d times, want %d", got, retrySteps)
	}
}

// The caller's context bounds the whole thing, retries included.
func TestRequestSuspendStopsWhenTheContextEnds(t *testing.T) {
	atelet := &capturingAtelet{errs: slices.Repeat([]error{status.Error(codes.Unavailable, "atelet restarting")}, retrySteps+1)}
	conn := dialCapturingAtelet(t, atelet)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := requestWithRetry(ctx, conn, Actor{Atespace: "team-a", Name: "actor-1", UID: "actor-uid-1"}, testBackoff()); err == nil {
		t.Fatal("requestWithRetry() with a dead context succeeded")
	}
}

// The line between "could not ask" and "was answered", code by code.
func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want bool
	}{
		{codes.Unavailable, true},
		{codes.Aborted, true},
		{codes.ResourceExhausted, true},
		{codes.NotFound, false},
		{codes.FailedPrecondition, false},
		{codes.InvalidArgument, false},
		{codes.PermissionDenied, false},
		{codes.Unauthenticated, false},
		{codes.Internal, false},
		// An attempt reaches this only by outliving requestTimeout, which means
		// the suspend itself is slow rather than undelivered.
		{codes.DeadlineExceeded, false},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			if got := retryable(status.Error(tc.code, "")); got != tc.want {
				t.Errorf("retryable(%v) = %t, want %t", tc.code, got, tc.want)
			}
		})
	}
}

// A misconfiguration must surface at startup rather than at the first request:
// an ateom that cannot reach its atelet can never give a worker slot back, and
// finding that out only when an actor goes idle hides it for as long as the
// actor is busy.
func TestNewRequesterFailsOnBadCredentials(t *testing.T) {
	_, err := NewRequester(Config{
		SocketPath:           filepath.Join(t.TempDir(), "atelet.sock"),
		CredentialBundlePath: filepath.Join(t.TempDir(), "does-not-exist.pem"),
		TrustBundlePath:      filepath.Join(t.TempDir(), "also-missing.pem"),
	})
	if err == nil {
		t.Fatal("NewRequester() with unreadable credentials succeeded, want an error the caller can exit on")
	}
}

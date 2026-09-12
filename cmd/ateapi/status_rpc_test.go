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

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/statusz"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCFailureRecorderRecordsAuthenticatedControlFailure(t *testing.T) {
	start := time.Date(2026, 9, 12, 4, 5, 6, 0, time.UTC)
	now := clockSequence(start, start.Add(25*time.Millisecond))
	recorder := newRPCFailureRecorder(now)
	ctx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		Kind:   principal.KindJWT,
		ID:     "operator@example.com",
		Issuer: "must-not-be-retained",
	})
	wantErr := status.Error(codes.FailedPrecondition, "raw error must not be retained")
	_, gotErr := recorder.UnaryServerInterceptor()(ctx, "request payload must not be retained", &grpc.UnaryServerInfo{
		FullMethod: "/ateapi.Control/SuspendActor",
	}, func(context.Context, any) (any, error) {
		return "response payload must not be retained", wantErr
	})
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("interceptor error = %v, want original %v", gotErr, wantErr)
	}

	got := recorder.Failures()
	if len(got) != 1 {
		t.Fatalf("recorded failures = %d, want 1", len(got))
	}
	if got[0].CompletedAt != "2026-09-12T04:05:06.025Z" || got[0].Method != "/ateapi.Control/SuspendActor" || got[0].Code != "FailedPrecondition" || got[0].Elapsed != "25ms" {
		t.Fatalf("recorded failure = %#v", got[0])
	}
	blob := fmt.Sprintf("%#v", got[0])
	for _, forbidden := range []string{"operator@example.com", "jwt", "must-not-be-retained", "request payload", "response payload", "raw error"} {
		if strings.Contains(blob, forbidden) {
			t.Errorf("recorded failure contains %q", forbidden)
		}
	}
}

func TestRPCFailureRecorderFiltersCoverage(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		principal bool
		err       error
		want      int
	}{
		{name: "matching failure", method: "/ateapi.Control/GetActor", principal: true, err: status.Error(codes.NotFound, "missing"), want: 1},
		{name: "successful control call", method: "/ateapi.Control/GetActor", principal: true},
		{name: "missing principal", method: "/ateapi.Control/GetActor", err: status.Error(codes.NotFound, "missing")},
		{name: "other service", method: "/ateapi.ActorIdentity/GetActorIdentity", principal: true, err: status.Error(codes.Internal, "failed")},
		{name: "similarly prefixed service", method: "/ateapi.ControlPlane/GetActor", principal: true, err: status.Error(codes.Internal, "failed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := newRPCFailureRecorder(time.Now)
			ctx := context.Background()
			if tt.principal {
				ctx = principal.InjectContext(ctx, principal.PrincipalInfo{Kind: principal.KindMTLS, ID: "spiffe://operator"})
			}
			_, _ = recorder.UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: tt.method}, func(context.Context, any) (any, error) {
				return nil, tt.err
			})
			if got := len(recorder.Failures()); got != tt.want {
				t.Fatalf("recorded failures = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRPCFailureRecorderAuthOrdering(t *testing.T) {
	recorder := newRPCFailureRecorder(time.Now)
	var handlerCalls atomic.Int32
	rejectAuth := func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		return nil, status.Error(codes.Unauthenticated, "bad credential must not be retained")
	}
	interceptors := unaryServerInterceptors(rejectAuth, recorder)
	_, _ = invokeUnaryInterceptors(interceptors, context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/GetActor"}, func(context.Context, any) (any, error) {
		handlerCalls.Add(1)
		return nil, status.Error(codes.Internal, "must not run")
	})
	if handlerCalls.Load() != 0 || len(recorder.Failures()) != 0 {
		t.Fatalf("auth rejection called handler %d times and recorded %d failures; want 0, 0", handlerCalls.Load(), len(recorder.Failures()))
	}

	authenticate := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(principal.InjectContext(ctx, principal.PrincipalInfo{Kind: principal.KindJWT, ID: "authorized"}), req)
	}
	interceptors = unaryServerInterceptors(authenticate, recorder)
	_, _ = invokeUnaryInterceptors(interceptors, context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/GetActor"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	})
	if got := recorder.Failures(); len(got) != 1 {
		t.Fatalf("post-auth failures = %#v, want one authenticated failure", got)
	}
}

func TestRPCFailureRecorderSeesCanonicalizedInnerError(t *testing.T) {
	recorder := newRPCFailureRecorder(time.Now)
	authenticate := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(principal.InjectContext(ctx, principal.PrincipalInfo{Kind: principal.KindJWT, ID: "authorized"}), req)
	}
	interceptors := unaryServerInterceptors(authenticate, recorder)
	_, gotErr := invokeUnaryInterceptors(interceptors, context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/ateapi.Control/GetActor"}, func(context.Context, any) (any, error) {
		return nil, errors.New("database detail must not be retained")
	})
	if got := status.Code(gotErr); got != codes.Internal {
		t.Fatalf("returned status = %s, want Internal", got)
	}
	failures := recorder.Failures()
	if len(failures) != 1 || failures[0].Code != "Internal" {
		t.Fatalf("recorded failures = %#v, want canonical Internal", failures)
	}
	if strings.Contains(fmt.Sprintf("%#v", failures), "database detail") {
		t.Fatal("recorded failure retained raw error detail")
	}
}

func TestRPCFailureRecorderEvictsOldestAndBoundsText(t *testing.T) {
	recorder := newRPCFailureRecorder(time.Now)
	ctx := principal.InjectContext(context.Background(), principal.PrincipalInfo{
		Kind: strings.Repeat("kind", 100),
		ID:   strings.Repeat("界", 100),
	})
	for i := range 101 {
		method := fmt.Sprintf("/ateapi.Control/Call%03d%s", i, strings.Repeat("x", 300))
		_, _ = recorder.UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			return nil, status.Error(codes.Internal, "failed")
		})
	}
	got := recorder.Failures()
	if len(got) != 100 {
		t.Fatalf("recorded failures = %d, want 100", len(got))
	}
	if !strings.Contains(got[0].Method, "Call100") || !strings.Contains(got[99].Method, "Call001") {
		t.Fatalf("retained order newest=%q oldest=%q, want Call100 through Call001", got[0].Method, got[99].Method)
	}
	for _, field := range []string{got[0].Method} {
		if len(field) > maxRetainedTextBytes || !utf8.ValidString(field) {
			t.Errorf("bounded field has %d bytes, valid UTF-8=%t", len(field), utf8.ValidString(field))
		}
	}
}

func TestRPCFailureRecorderOrdersAndRetainsByCompletionTime(t *testing.T) {
	recorder := newRPCFailureRecorder(time.Now)
	base := time.Date(2026, 9, 12, 4, 5, 6, 0, time.UTC)
	for i := 1; i < maxRecordedRPCFailures; i++ {
		completedAt := base.Add(time.Duration(i) * time.Second)
		recorder.add(completedAt, rpcFailureAt(completedAt, fmt.Sprintf("base-%03d", i)))
	}

	type pendingFailure struct {
		completedAt time.Time
		failure     statusz.RPCFailure
		ready       chan<- struct{}
		store       <-chan struct{}
		stored      chan<- struct{}
	}
	store := func(pending pendingFailure) {
		pending.ready <- struct{}{}
		<-pending.store
		recorder.add(pending.completedAt, pending.failure)
		pending.stored <- struct{}{}
	}

	olderReady := make(chan struct{})
	newerReady := make(chan struct{})
	storeOlder := make(chan struct{})
	storeNewer := make(chan struct{})
	olderStored := make(chan struct{})
	newerStored := make(chan struct{})
	go store(pendingFailure{
		completedAt: base,
		failure:     rpcFailureAt(base, "delayed-oldest"),
		ready:       olderReady,
		store:       storeOlder,
		stored:      olderStored,
	})
	newestAt := base.Add(100 * time.Second)
	go store(pendingFailure{
		completedAt: newestAt,
		failure:     rpcFailureAt(newestAt, "newest"),
		ready:       newerReady,
		store:       storeNewer,
		stored:      newerStored,
	})
	<-olderReady
	<-newerReady
	close(storeNewer)
	<-newerStored
	close(storeOlder)
	<-olderStored

	got := recorder.Failures()
	if len(got) != maxRecordedRPCFailures {
		t.Fatalf("recorded failures = %d, want %d", len(got), maxRecordedRPCFailures)
	}
	if got[0].Method != "newest" {
		t.Fatalf("newest failure = %q, want newest", got[0].Method)
	}
	if got[len(got)-1].Method != "base-001" {
		t.Fatalf("oldest retained failure = %q, want base-001", got[len(got)-1].Method)
	}
	for _, failure := range got {
		if failure.Method == "delayed-oldest" {
			t.Fatal("delayed oldest failure displaced a newer completion")
		}
	}
}

func TestBoundedUTF8DetachesRetainedText(t *testing.T) {
	large := strings.Repeat("x", 4*maxRetainedTextBytes)
	tests := []struct {
		name  string
		input string
	}{
		{name: "short substring", input: large[17 : 17+32]},
		{name: "truncated substring", input: large[17:]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boundedUTF8(tt.input)
			if unsafe.StringData(got) == unsafe.StringData(tt.input) {
				t.Fatal("bounded text still aliases its input backing storage")
			}
			if len(got) > maxRetainedTextBytes || !utf8.ValidString(got) {
				t.Fatalf("bounded text has %d bytes, valid UTF-8=%t", len(got), utf8.ValidString(got))
			}
		})
	}
}

func TestBoundedUTF8BoundsInputInspection(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "long valid input",
			input: strings.Repeat("x", maxRetainedTextBytes) + strings.Repeat("tail", 1<<18),
			want:  strings.Repeat("x", maxRetainedTextBytes),
		},
		{
			name:  "long invalid run hides distant suffix",
			input: strings.Repeat("\xff", 1<<20) + "must-not-be-inspected",
			want:  "\uFFFD",
		},
		{
			name:  "complete rune at boundary",
			input: strings.Repeat("x", maxRetainedTextBytes-3) + "界" + "tail",
			want:  strings.Repeat("x", maxRetainedTextBytes-3) + "界",
		},
		{
			name:  "split rune at boundary",
			input: strings.Repeat("x", maxRetainedTextBytes-1) + "界" + "tail",
			want:  strings.Repeat("x", maxRetainedTextBytes-1),
		},
		{
			name:  "invalid byte near boundary",
			input: strings.Repeat("x", maxRetainedTextBytes-3) + "\xff" + "tail",
			want:  strings.Repeat("x", maxRetainedTextBytes-3) + "\uFFFD",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boundedUTF8(tt.input)
			if got != tt.want {
				t.Fatalf("bounded text = %q (%d bytes), want %q (%d bytes)", got, len(got), tt.want, len(tt.want))
			}
			if len(got) > maxRetainedTextBytes || !utf8.ValidString(got) {
				t.Fatalf("bounded text has %d bytes, valid UTF-8=%t", len(got), utf8.ValidString(got))
			}
		})
	}
}

func TestRPCFailureRecorderConcurrentRecordAndSnapshot(t *testing.T) {
	recorder := newRPCFailureRecorder(time.Now)
	ctx := principal.InjectContext(context.Background(), principal.PrincipalInfo{Kind: principal.KindJWT, ID: "concurrent"})
	const writers = 16
	const callsPerWriter = 50
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for call := range callsPerWriter {
				method := fmt.Sprintf("/ateapi.Control/Writer%dCall%d", writer, call)
				_, _ = recorder.UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
					return nil, status.Error(codes.Aborted, "retry")
				})
				_ = recorder.Failures()
			}
		}()
	}
	wg.Wait()
	if got := len(recorder.Failures()); got != 100 {
		t.Fatalf("recorded failures = %d, want bounded 100", got)
	}
}

func rpcFailureAt(completedAt time.Time, method string) statusz.RPCFailure {
	return statusz.RPCFailure{
		CompletedAt: completedAt.Format(time.RFC3339Nano),
		Method:      method,
	}
}

func clockSequence(times ...time.Time) func() time.Time {
	var mu sync.Mutex
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		if len(times) == 0 {
			panic("clock sequence exhausted")
		}
		now := times[0]
		times = times[1:]
		return now
	}
}

func invokeUnaryInterceptors(interceptors []grpc.UnaryServerInterceptor, ctx context.Context, req any, info *grpc.UnaryServerInfo, final grpc.UnaryHandler) (any, error) {
	handler := final
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor := interceptors[i]
		next := handler
		handler = func(ctx context.Context, req any) (any, error) {
			return interceptor(ctx, req, info, next)
		}
	}
	return handler(ctx, req)
}

func TestControlServiceNameMatchesRecorderNamespace(t *testing.T) {
	if got := ateapipb.Control_ServiceDesc.ServiceName; got != "ateapi.Control" {
		t.Fatalf("Control service name = %q, want ateapi.Control", got)
	}
}

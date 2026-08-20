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

package ingress

import (
	"context"
	"io"
	"log/slog"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// runningActorResponse is the control-plane reply for an actor that needs no
// resume — the fast path every benchmark here exercises.
func runningActorResponse() *ateapipb.ResumeActorResponse {
	return &ateapipb.ResumeActorResponse{
		Actor: &ateapipb.Actor{
			Status: &ateapipb.ActorStatus{
				State:            ateapipb.ActorState_ACTOR_STATE_RUNNING,
				WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: "10.0.0.52"},
			},
		},
		Resumed: false,
	}
}

// BenchmarkResumeActorAlreadyRunning measures the resumer's fast path — a
// request to an actor the control plane reports RUNNING on the first attempt.
// This is on every request's critical path, so the flight bookkeeping around
// the single RPC must stay cheap.
func BenchmarkResumeActorAlreadyRunning(b *testing.B) {
	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return runningActorResponse(), nil
		},
	}
	resumer := NewActorResumer(mock, withParking(DefaultParkedRequestConfig()))
	ref := resources.ActorRef{Atespace: "team-a", Name: "bench-actor"}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := resumer.ResumeActor(ctx, ref); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHandleRequestHeadersRunningActor measures the whole ingress
// handler's fast path, parking admission included — the production cost of one
// routed request to a running actor.
func BenchmarkHandleRequestHeadersRunningActor(b *testing.B) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(prev) })

	mock := &mockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			return runningActorResponse(), nil
		},
	}
	h := New(mock, DefaultParkedRequestConfig(), nil)

	const authority = "123e4567-e89b-12d3-a456-426614174000.team-a.actors.resources.substrate.ate.dev"
	md := benchRequestMetadata(b, authority)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := h.HandleRequestHeaders(ctx, md); err != nil {
			b.Fatal(err)
		}
	}
}

// benchRequestMetadata mirrors the requestMetadata test helper for benchmarks
// (that helper takes *testing.T).
func benchRequestMetadata(b *testing.B, authority string) *extproc.RequestMetadata {
	b.Helper()
	s, err := structpb.NewStruct(map[string]any{extproc.AuthorityFilterStateAttribute: authority})
	if err != nil {
		b.Fatalf("build authority attributes: %v", err)
	}
	return extproc.NewRequestMetadata(
		[]*corev3.HeaderValue{
			{Key: ":authority", Value: authority},
			{Key: ":method", Value: "POST"},
		},
		map[string]*structpb.Struct{"envoy.filters.http.ext_proc": s},
	)
}

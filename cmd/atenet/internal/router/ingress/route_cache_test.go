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

package ingress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestSafeRouteCache_EvictionOn421StaleAssignmentAndLocal503(t *testing.T) {
	var currentIP atomic.Value
	currentIP.Store("10.0.0.11")
	var calls atomic.Int32
	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			calls.Add(1)
			return &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Metadata: &ateapi.ResourceMetadata{
						Uid: "uid-actor-42",
					},
					ActorTemplate: &ateapi.ObjectRef{Atespace: "ate-env", Name: "tmpl"},
					Status: &ateapi.ActorStatus{
						State: ateapi.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapi.WorkerAssignment{
							WorkerPodIps:   []string{currentIP.Load().(string)},
							WorkerSpiffeId: "spiffe://cluster.local/ns/ate-env/sa/worker",
						},
					},
				},
				Resumed: false,
			}, nil
		},
	}

	h := New(mock, DefaultParkedRequestConfig(), nil, WithUpstreamConnect(false), WithWarmRouteCacheTTL(30*time.Second))
	mkMD := func() *extproc.RequestMetadata {
		return extproc.NewRequestMetadata([]*corev3.HeaderValue{
			{Key: atenet.TargetActorHeader, RawValue: []byte("ate-env/env-42")},
		}, nil)
	}

	// 1. Cold miss populates cache -> 10.0.0.11:443, checks ActorUIDHeader mutation.
	res1, err := h.HandleRequestHeaders(context.Background(), mkMD())
	if err != nil || res1.Target != "10.0.0.11:443" {
		t.Fatalf("req1: res=%+v err=%v", res1, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 ResumeActor call, got %d", calls.Load())
	}
	var foundUIDHeader bool
	for _, opt := range res1.Response.GetResponse().GetHeaderMutation().GetSetHeaders() {
		if opt.GetHeader().GetKey() == atenet.ActorUIDHeader && string(opt.GetHeader().GetRawValue()) == "uid-actor-42" {
			foundUIDHeader = true
			break
		}
	}
	if !foundUIDHeader {
		t.Fatalf("expected ActorUIDHeader in SetHeaders, got: %+v", res1.Response.GetResponse().GetHeaderMutation().GetSetHeaders())
	}

	// 2. Warm hit skips ResumeActor.
	currentIP.Store("10.0.0.22")
	res2, err := h.HandleRequestHeaders(context.Background(), mkMD())
	if err != nil || res2.Target != "10.0.0.11:443" {
		t.Fatalf("req2 expected cached 10.0.0.11:443, got res=%+v err=%v", res2, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected cached hit (still 1 call), got %d", calls.Load())
	}

	// 3. Application 404/500 from inside the actor (no X-Ate-Assignment-Stale)
	// must NOT evict the cache and returns nil.
	resp404 := h.OnUpstreamHeaders("ate-env/env-42", true, []*corev3.HeaderValue{
		{Key: ":status", RawValue: []byte("404")},
	}, nil)
	if resp404 != nil {
		t.Fatalf("expected nil from OnUpstreamHeaders on app 404, got %+v", resp404)
	}
	resp500 := h.OnUpstreamHeaders("ate-env/env-42", true, []*corev3.HeaderValue{
		{Key: ":status", RawValue: []byte("500")},
	}, nil)
	if resp500 != nil {
		t.Fatalf("expected nil from OnUpstreamHeaders on app 500, got %+v", resp500)
	}
	resStillWarm, err := h.HandleRequestHeaders(context.Background(), mkMD())
	if err != nil || resStillWarm.Target != "10.0.0.11:443" {
		t.Fatalf("expected route cache to remain intact after app 404/500, got res=%+v", resStillWarm)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 0 additional calls after app 404/500, got %d", calls.Load())
	}

	// 4. atunnel HTTP 421 with X-Ate-Assignment-Stale: true evicts the cache and
	// rewrites to ImmediateResponse 503 / GrpcStatus 14 (Unavailable).
	resp421 := h.OnUpstreamHeaders("ate-env/env-42", true, []*corev3.HeaderValue{
		{Key: ":status", RawValue: []byte("421")},
		{Key: atenet.StaleAssignmentHeader, RawValue: []byte("true")},
	}, nil)
	if resp421 == nil || resp421.GetImmediateResponse() == nil {
		t.Fatalf("expected ImmediateResponse for 421 stale assignment, got %+v", resp421)
	}
	imm := resp421.GetImmediateResponse()
	if imm.GetStatus().GetCode() != envoy_type.StatusCode_ServiceUnavailable {
		t.Fatalf("expected HttpStatus ServiceUnavailable, got %v", imm.GetStatus().GetCode())
	}
	if imm.GetGrpcStatus().GetStatus() != 14 {
		t.Fatalf("expected GrpcStatus 14, got %v", imm.GetGrpcStatus().GetStatus())
	}

	// 5. Subsequent request misses cache and picks up updated 10.0.0.22.
	res3, err := h.HandleRequestHeaders(context.Background(), mkMD())
	if err != nil || res3.Target != "10.0.0.22:443" {
		t.Fatalf("req3 expected refreshed 10.0.0.22:443 after 421 eviction, got res=%+v err=%v", res3, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2nd ResumeActor call after eviction, got %d", calls.Load())
	}

	// 6. Envoy local reply (e.g. upstream_reset_before_response_started) evicts
	// the route cache while returning nil so Envoy's own 503 / gRPC UNAVAILABLE passes through.
	currentIP.Store("10.0.0.33")
	resp503 := h.OnUpstreamHeaders("ate-env/env-42", true, []*corev3.HeaderValue{
		{Key: ":status", RawValue: []byte("503")},
	}, map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				"response.code_details": structpb.NewStringValue("upstream_reset_before_response_started{connection_failure}"),
			},
		},
	})
	if resp503 != nil {
		t.Fatalf("expected nil pass-through after local-reply cache eviction, got %+v", resp503)
	}

	// 7. Subsequent request picks up 10.0.0.33.
	res4, err := h.HandleRequestHeaders(context.Background(), mkMD())
	if err != nil || res4.Target != "10.0.0.33:443" {
		t.Fatalf("req4 expected refreshed 10.0.0.33:443 after local-reply eviction, got res=%+v err=%v", res4, err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3rd ResumeActor call after local-reply eviction, got %d", calls.Load())
	}
}

func TestSafeRouteCache_FlightInvalidationPreventsStalePublish(t *testing.T) {
	actorRef := resources.ActorRef{Atespace: "ate-env", Name: "race-actor"}
	key := actorRef.String()

	flightStarted := make(chan struct{})
	allowFlightFinish := make(chan struct{})

	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			close(flightStarted)
			<-allowFlightFinish
			return &ateapi.ResumeActorResponse{
				Actor: &ateapi.Actor{
					Metadata: &ateapi.ResourceMetadata{Uid: "uid-old"},
					Status: &ateapi.ActorStatus{
						State: ateapi.ActorState_ACTOR_STATE_RUNNING,
						WorkerAssignment: &ateapi.WorkerAssignment{
							WorkerPodIps:   []string{"10.0.0.99"},
							WorkerSpiffeId: "spiffe://cluster.local/ns/ate-env/sa/worker",
						},
					},
				},
				Resumed: false,
			}, nil
		},
	}

	resumer := NewActorResumer(mock, withWarmRouteCacheTTL(30*time.Second))

	resCh := make(chan *ateapi.Actor)
	go func() {
		actor, _, err := resumer.ResumeActor(context.Background(), actorRef)
		if err != nil {
			t.Errorf("ResumeActor failed: %v", err)
		}
		resCh <- actor
	}()

	<-flightStarted

	// Concurrent invalidation marks the in-flight resume as invalidated before publish.
	resumer.InvalidateRouteKey(actorRef.String())

	close(allowFlightFinish)
	<-resCh

	// Verify that routeCache did NOT store the invalidated in-flight placement.
	if _, ok := resumer.routeCache.Get(key); ok {
		t.Fatalf("expected routeCache to stay empty when flight was invalidated mid-run, got cached entry")
	}
}

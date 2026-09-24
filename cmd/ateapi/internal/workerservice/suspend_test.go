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

package workerservice

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	suspendAtespace = "team-a"
	suspendActor    = "actor-1"
)

// seedHostedActor stores an actor assigned to workerName, as the placement
// path leaves one that is RUNNING. An empty workerName seeds an actor with no
// assignment at all, which is how a SUSPENDED, PAUSED, or CRASHED one looks.
func seedHostedActor(t *testing.T, st store.Interface, workerName string) *ateapipb.Actor {
	t.Helper()
	actorStatus := &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}
	if workerName != "" {
		actorStatus = &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: workerName},
				WorkerNamespace: "ate-system",
				WorkerPool:      "pool-1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    workerName,
			},
		}
	}
	return storetest.MustCreateActor(t, context.Background(), st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: suspendAtespace, Name: suspendActor},
		Status:   actorStatus,
	})
}

func suspendRequest(actorUID string) *ateapipb.RequestActorSuspendRequest {
	return &ateapipb.RequestActorSuspendRequest{
		Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
		Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
		ActorUid: actorUID,
	}
}

// The point of the whole path: a worker asks, and the control plane runs the
// same suspend a client would have asked for.
func TestRequestActorSuspend(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	suspended := &ateapipb.Actor{
		Metadata: actor.GetMetadata(),
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
	suspender := &fakeSuspender{resp: &ateapipb.SuspendActorResponse{Actor: suspended}}
	s := New(st, suspender, testAteletSPIFFEID, nil)

	got, err := s.RequestActorSuspend(
		ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)),
		suspendRequest(actor.GetMetadata().GetUid()))
	if err != nil {
		t.Fatalf("RequestActorSuspend() failed: %v", err)
	}
	want := []*ateapipb.SuspendActorRequest{{
		Actor: &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
	}}
	if diff := cmp.Diff(want, suspender.calls, protocmp.Transform()); diff != "" {
		t.Errorf("suspend calls mismatch (-want +got):\n%s", diff)
	}
	// The caller gets the actor as the workflow left it, so an ateom can see
	// whether the suspend it asked for actually happened.
	if diff := cmp.Diff(suspended, got.GetActor(), protocmp.Transform()); diff != "" {
		t.Errorf("returned actor mismatch (-want +got):\n%s", diff)
	}
}

// A worker may speak only for what it hosts, and the store is what says so.
// Each of these is reported as absent rather than forbidden, so a caller learns
// nothing about actors that are not its own.
func TestRequestActorSuspend_OnlyForHostedActors(t *testing.T) {
	const otherWorker = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"

	tests := []struct {
		name string
		// node the caller's certificate names, and the worker the actor is
		// assigned to in the store.
		callerNode     string
		assignedWorker string
		// uid the request claims; empty means the actor's real one.
		requestedUID string
	}{
		{name: "worker on another node", callerNode: "some-other-node", assignedWorker: testWorkerName},
		{name: "actor hosted by another worker", callerNode: testNode, assignedWorker: otherWorker},
		{name: "actor has no worker", callerNode: testNode, assignedWorker: ""},
		{name: "stale actor incarnation", callerNode: testNode, assignedWorker: testWorkerName, requestedUID: "not-the-uid"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()
			seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
			actor := seedHostedActor(t, st, tc.assignedWorker)
			suspender := &fakeSuspender{}
			s := New(st, suspender, testAteletSPIFFEID, nil)

			uid := tc.requestedUID
			if uid == "" {
				uid = actor.GetMetadata().GetUid()
			}
			_, err := s.RequestActorSuspend(
				ateletauthtest.ContextWith(ateletauthtest.CertOn(t, tc.callerNode)),
				suspendRequest(uid))
			if got := status.Code(err); got != codes.NotFound {
				t.Fatalf("code = %v (err %v), want NotFound", got, err)
			}
			if len(suspender.calls) != 0 {
				t.Errorf("refused request still reached the suspend: %v", suspender.calls)
			}
		})
	}
}

func TestRequestActorSuspend_Errors(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	uid := actor.GetMetadata().GetUid()
	suspender := &fakeSuspender{}
	s := New(st, suspender, testAteletSPIFFEID, nil)
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode))

	tests := []struct {
		name string
		ctx  context.Context
		req  *ateapipb.RequestActorSuspendRequest
		want codes.Code
	}{
		{"unauthenticated", ateletauthtest.ContextWith(nil), suspendRequest(uid), codes.Unauthenticated},
		{"no worker ref", authed, &ateapipb.RequestActorSuspendRequest{
			Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
			ActorUid: uid,
		}, codes.InvalidArgument},
		{"atespaced worker ref", authed, &ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: testWorkerName},
			Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
			ActorUid: uid,
		}, codes.InvalidArgument},
		{"no actor ref", authed, &ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			ActorUid: uid,
		}, codes.InvalidArgument},
		{"actor ref without atespace", authed, &ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			Actor:    &ateapipb.ObjectRef{Name: suspendActor},
			ActorUid: uid,
		}, codes.InvalidArgument},
		// Without a UID the request could outlive the actor it was made about
		// and suspend whatever took its name, so an absent one is refused
		// rather than treated as "whichever is there now".
		{"no actor uid", authed, suspendRequest(""), codes.InvalidArgument},
		{"absent worker", authed, &ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Name: "5c7e2a91-3d4b-4f60-8a1c-2e9b7d0f4a63"},
			Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: suspendActor},
			ActorUid: uid,
		}, codes.NotFound},
		{"absent actor", authed, &ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			Actor:    &ateapipb.ObjectRef{Atespace: suspendAtespace, Name: "no-such-actor"},
			ActorUid: uid,
		}, codes.NotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.RequestActorSuspend(tc.ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
	if len(suspender.calls) != 0 {
		t.Errorf("a malformed or unauthorized request reached the suspend: %v", suspender.calls)
	}
}

// A golden actor is committed by the template controller once the warmup it
// timed has elapsed, and the suspend is that commit. The actor idles from the
// moment it boots, so it would ask immediately and snapshot a workload that
// never warmed up. Control.SuspendActor still serves the controller; only the
// Worker's request for one is refused.
func TestRequestActorSuspend_GoldenActorCannotSelfSuspend(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	golden := storetest.MustCreateActor(t, context.Background(), st, &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: suspendActor},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: testWorkerName},
				WorkerNamespace: "ate-system",
				WorkerPool:      "pool-1",
				WorkerPod:       "worker-pod-1",
				WorkerPodUid:    testWorkerName,
			},
		},
	})
	suspender := &fakeSuspender{}
	s := New(st, suspender, testAteletSPIFFEID, nil)

	// Everything else about the request is in order: the Worker really does
	// host this actor, so being golden is the only reason it is refused.
	_, err := s.RequestActorSuspend(
		ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)),
		&ateapipb.RequestActorSuspendRequest{
			Worker:   &ateapipb.ObjectRef{Name: testWorkerName},
			Actor:    &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: suspendActor},
			ActorUid: golden.GetMetadata().GetUid(),
		})
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %v (err %v), want FailedPrecondition", got, err)
	}
	if len(suspender.calls) != 0 {
		t.Errorf("golden actor request still reached the suspend: %v", suspender.calls)
	}
}

// The suspend is a proposal, and the control plane's refusal is the answer: a
// request that loses a race to a resume, pause, or delete comes back as what
// the workflow said, not as something the worker has to interpret.
func TestRequestActorSuspend_PassesThroughTheWorkflowsRefusal(t *testing.T) {
	st, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	seedReportedWorker(t, st, testNode, &ateapipb.WorkerResources{Actors: 1})
	actor := seedHostedActor(t, st, testWorkerName)
	suspender := &fakeSuspender{err: status.Error(codes.FailedPrecondition, "Actor is RESUMING")}
	s := New(st, suspender, testAteletSPIFFEID, nil)

	_, err := s.RequestActorSuspend(
		ateletauthtest.ContextWith(ateletauthtest.CertOn(t, testNode)),
		suspendRequest(actor.GetMetadata().GetUid()))
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %v (err %v), want FailedPrecondition", got, err)
	}
}

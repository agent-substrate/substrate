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
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth/ateletauthtest"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// fakeEgressPolicyStore adds egress policies to fakeActorStore.
type fakeEgressPolicyStore struct {
	*fakeActorStore
	policies  map[resources.ActorRef]*ateapipb.EgressPolicy
	policyErr error
}

func (f *fakeEgressPolicyStore) GetEgressPolicy(_ context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	if f.policyErr != nil {
		return nil, f.policyErr
	}
	p, ok := f.policies[actorRef]
	if !ok {
		return nil, store.ErrNotFound
	}
	return p, nil
}

const (
	egressAtespace = "team-a"
	egressActor    = "my-actor"
	egressActorUID = "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"
)

var testEgressPolicy = &ateapipb.EgressPolicy{
	Metadata: &ateapipb.ResourceMetadata{Atespace: egressAtespace, Name: resources.SingletonName},
	Rules: []*ateapipb.EgressRule{{
		Http: &ateapipb.HttpRule{Hostnames: []string{"api.example.com"}},
	}},
}

// newEgressPolicyServer serves an actor that worker-1 on node-1 hosts, with
// testEgressPolicy.
func newEgressPolicyServer() (*Server, *fakeEgressPolicyStore) {
	actors, _ := newFakeStoreWithActor(egressAtespace, egressActor, egressActorUID)
	st := &fakeEgressPolicyStore{
		fakeActorStore: actors,
		policies: map[resources.ActorRef]*ateapipb.EgressPolicy{
			{Atespace: egressAtespace, Name: egressActor}: testEgressPolicy,
		},
	}
	return New(st, &fakeSuspender{}, testAteletSPIFFEID, nil, "", nil), st
}

func egressPolicyRequest() *ateapipb.GetActorEgressPolicyRequest {
	return &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{Atespace: egressAtespace, Name: egressActor},
		Name:  resources.SingletonName,
	}
}

func TestGetActorEgressPolicy(t *testing.T) {
	s, _ := newEgressPolicyServer()
	got, err := s.GetActorEgressPolicy(ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1")), egressPolicyRequest())
	if err != nil {
		t.Fatalf("GetActorEgressPolicy: %v", err)
	}
	if diff := cmp.Diff(testEgressPolicy, got, protocmp.Transform()); diff != "" {
		t.Errorf("policy mismatch (-want +got):\n%s", diff)
	}
}

func TestGetActorEgressPolicy_Errors(t *testing.T) {
	authed := ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-1"))
	tests := []struct {
		name   string
		ctx    context.Context
		mutate func(*fakeEgressPolicyStore, *ateapipb.GetActorEgressPolicyRequest)
		want   codes.Code
	}{
		{
			name: "unauthenticated",
			ctx:  ateletauthtest.ContextWith(nil),
			want: codes.Unauthenticated,
		},
		{
			// A worker pod on the actor's node presents its own certificate,
			// not atelet's.
			name: "caller is not atelet",
			ctx: ateletauthtest.ContextWith(ateletauthtest.Cert(t, "/ns/ate-system/sa/ate-worker",
				ateletauthtest.PodIdentityOn("node-1"))),
			want: codes.PermissionDenied,
		},
		{
			name:   "missing actor",
			ctx:    authed,
			mutate: func(_ *fakeEgressPolicyStore, r *ateapipb.GetActorEgressPolicyRequest) { r.Actor = nil },
			want:   codes.InvalidArgument,
		},
		{
			name:   "name is not default",
			ctx:    authed,
			mutate: func(_ *fakeEgressPolicyStore, r *ateapipb.GetActorEgressPolicyRequest) { r.Name = "other" },
			want:   codes.InvalidArgument,
		},
		{
			// atelet enforces NOT_FOUND as deny-all.
			name: "hosted actor with no policy",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.policies = nil
			},
			want: codes.NotFound,
		},
		{
			name: "policy lookup fails",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.policyErr = errors.New("connection reset")
			},
			want: codes.Internal,
		},
		{
			name: "worker assignment lookup fails",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.assignmentErr = errors.New("connection reset")
			},
			want: codes.Internal,
		},
		// Every refusal below is NOT_FOUND, the same answer as a hosted actor
		// without a policy: a node must not be able to tell an actor elsewhere
		// from no actor at all, nor either from an actor with no policy.
		{
			name: "actor hosted on another node",
			ctx:  ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2")),
			want: codes.NotFound,
		},
		{
			name: "actor hosted on another node, no policy",
			ctx:  ateletauthtest.ContextWith(ateletauthtest.CertOn(t, "node-2")),
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.policies = nil
			},
			want: codes.NotFound,
		},
		{
			name: "actor does not exist",
			ctx:  authed,
			mutate: func(_ *fakeEgressPolicyStore, r *ateapipb.GetActorEgressPolicyRequest) {
				r.Actor.Name = "no-such-actor"
			},
			want: codes.NotFound,
		},
		{
			name: "actor not hosted anywhere",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.actors[resources.ActorRef{Atespace: egressAtespace, Name: egressActor}].Status =
					&ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED}
			},
			want: codes.NotFound,
		},
		{
			// Pause, suspend and crash release the worker before they clear the
			// actor's status, which keeps naming the worker until they do.
			name: "actor whose worker was released",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.assignments = map[string]string{}
			},
			want: codes.NotFound,
		},
		{
			name: "paused actor on this node",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.actors[resources.ActorRef{Atespace: egressAtespace, Name: egressActor}].Status =
					pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-1")
			},
			want: codes.OK,
		},
		{
			name: "paused actor on another node",
			ctx:  authed,
			mutate: func(st *fakeEgressPolicyStore, _ *ateapipb.GetActorEgressPolicyRequest) {
				st.actors[resources.ActorRef{Atespace: egressAtespace, Name: egressActor}].Status =
					pausedStatus(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "node-2")
			},
			want: codes.NotFound,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, st := newEgressPolicyServer()
			req := egressPolicyRequest()
			if tc.mutate != nil {
				tc.mutate(st, req)
			}
			_, err := s.GetActorEgressPolicy(tc.ctx, req)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

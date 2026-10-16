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

package authz

import (
	"context"
	"testing"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// testBasicFlow runs an actor's whole lifecycle as users granted access
// through an AccessPolicy, not as the bootstrap owner every other suite runs
// as. Alice, an editor, creates, resumes, suspends and deletes an actor; Bob,
// a viewer, can watch it but not change it. Each step needs the one before.
func testBasicFlow(t *testing.T, ctx context.Context, f *fixture) {
	alice := newPrincipal(t, ctx, f.clients, f.atespace, "flow-alice")
	bob := newPrincipal(t, ctx, f.clients, f.atespace, "flow-bob")
	setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("editor", alice), binding("viewer", bob))
	cleanupAtespacePolicy(t, f.clients, f.atespace)

	// Wait for the grants to reach every ateapi replica.
	expectAll(t, ctx, []call{
		{"GetActor", alice, getActor(f.actor), allowed},
		{"GetActor", bob, getActor(f.actor), allowed},
	})
	if t.Failed() {
		return
	}

	actor := &ateapipb.ObjectRef{Atespace: f.atespace, Name: "flow-actor"}
	if _, err := alice.api.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: actor.GetAtespace(), Name: actor.GetName()},
		ActorTemplate: f.template,
	}}); err != nil {
		t.Fatalf("CreateActor as alice: %v", err)
	}
	t.Cleanup(func() {
		// Best effort, in case a step below failed partway.
		_, _ = f.clients.SubstrateAPI.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: actor})
		_, _ = f.clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: actor})
	})

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, &e2e.Clients{SubstrateAPI: alice.api}, &ateapipb.ResumeActorRequest{Actor: actor}); err != nil {
		t.Fatalf("ResumeActor as alice: %v", err)
	}

	// Bob sees the running actor but can't suspend it.
	expectAll(t, ctx, []call{
		{"GetActor", bob, getActor(actor), allowed},
		{"ListActors", bob, listActors(f.atespace), allowed},
		{"SuspendActor", bob, suspendActor(actor), denied},
	})
	if t.Failed() {
		return
	}

	if _, err := alice.api.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actor}); err != nil {
		t.Fatalf("SuspendActor as alice: %v", err)
	}
	waitForActorState(t, ctx, f.clients, actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	if _, err := alice.api.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: actor}); err != nil {
		t.Fatalf("DeleteActor as alice: %v", err)
	}
}

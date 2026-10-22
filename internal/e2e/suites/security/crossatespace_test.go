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

// This file covers security invariants that specifically depend on the
// boundary between two distinct atespaces (for actor-level isolation within a
// single atespace, see actor_isolation_test.go).
package security

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// markerA is written by the actor in atespace A to verify that a same-atespace
	// clone restored from atespace-a-tag carries the source actor's durable data.
	markerA = dataMount + "/atespace-a-marker"
)

// atespaces is the pair of atespaces under test.
type atespaces struct {
	a, b string
}

// TestCrossAtespaceIsolation validates isolation between two atespaces.
func TestCrossAtespaceIsolation(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	ns := deployAtespaces(t, ctx, clients, env["BUCKET_NAME"])

	rc, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	const source = "actor-a"
	createAndResumeActor(t, ctx, clients, ns.a, source)
	sourceActor := resources.ActorRef{Atespace: ns.a, Name: source}

	writeFile(t, ctx, rc, sourceActor, markerA)
	if got := readFile(t, ctx, rc, sourceActor, markerA); got.Content != writtenByProbe {
		t.Fatalf("control: actor %s reading its OWN %s = %q (error %q), want %q",
			sourceActor, markerA, got.Content, got.Error, writtenByProbe)
	}

	// ST-4 [space]: an ATESPACE-scoped tag in atespace A must not seed an
	// actor in atespace B. Suspends `source` to snapshot it.
	t.Run("TagScopedToAtespaceCannotSeedAnotherAtespace", func(t *testing.T) {
		t.Log("Invariant ST-4 [space]: A snapshot must not seed an actor in another atespace unless the tag is published")
		testTagScopeBoundary(t, ctx, clients, rc, ns, source)
	})
}

// testTagScopeBoundary checks that an Actor cannot be seeded from a snapshot
// taken in another atespace.
//
// source is a running actor in atespace A holding markerA; it is suspended here
// to produce the snapshot the tag captures.
func testTagScopeBoundary(t *testing.T, ctx context.Context, clients *e2e.Clients, rc *e2e.RouterClient, ns atespaces, source string) {
	t.Helper()

	// The tag needs a durable snapshot to capture.
	suspendDurably(t, ctx, clients, ns.a, source)

	tagRef := &ateapipb.ObjectRef{Atespace: ns.a, Name: "atespace-a-tag"}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteTag(context.Background(), &ateapipb.DeleteTagRequest{Tag: tagRef})
	})
	// Tags outlive the fixture namespace, so an interrupted run can leave one
	// behind and wedge reruns on AlreadyExists. Clear it first, best-effort.
	_, _ = clients.SubstrateAPI.DeleteTag(ctx, &ateapipb.DeleteTagRequest{Tag: tagRef})
	tag, err := clients.SubstrateAPI.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: ns.a, Name: tagRef.GetName()},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: ns.a, Name: source},
		},
	})
	if err != nil {
		t.Fatalf("CreateTag %s/%s: %v", ns.a, tagRef.GetName(), err)
	}
	if snapshotURI(tag.GetStatus().GetSnapshot()) == "" {
		t.Fatalf("tag %s/%s has no snapshot uri, so it could seed an Actor in NO atespace and the negative case below would be vacuous",
			ns.a, tagRef.GetName())
	}

	// The boundary: the other atespace may not name this tag.
	crossRef := &ateapipb.ObjectRef{Atespace: ns.b, Name: "cross-atespace-clone"}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: crossRef})
	})
	_, err = clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ns.b, Name: crossRef.GetName()},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: ns.b, Name: templateName},
		SourceTag:     tagRef,
	}})
	switch {
	case err == nil:
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s (scope ATESPACE) succeeded — an actor can be seeded from another atespace's snapshot",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName())
	case status.Code(err) != codes.FailedPrecondition:
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s: got %v, want FailedPrecondition",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName(), err)
	case !strings.Contains(status.Convert(err).Message(), "not published outside its Atespace"):
		// Rejected by some other precondition, not the scope check.
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s was rejected by a different precondition: %q; want the tag-scope rejection",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName(), status.Convert(err).Message())
	}

	// Positive control: the same tag seeds an Actor in its own atespace and the
	// clone carries the source's durable data. Otherwise an unusable tag would
	// make the rejection above look like enforcement.
	const clone = "same-atespace-clone"
	cloneRef := &ateapipb.ObjectRef{Atespace: ns.a, Name: clone}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: cloneRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: cloneRef})
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ns.a, Name: clone},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: ns.a, Name: templateName},
		SourceTag:     tagRef,
	}}); err != nil {
		t.Fatalf("control: CreateActor %s/%s from tag %s in its own atespace: %v — the tag is unusable, so the cross-atespace rejection above proves nothing",
			ns.a, clone, tagRef.GetName(), err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: cloneRef}); err != nil {
		t.Fatalf("control: ResumeActor %s/%s: %v", ns.a, clone, err)
	}
	waitForActorState(t, ctx, clients, ns.a, clone, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if got := readFile(t, ctx, rc, resources.ActorRef{Atespace: ns.a, Name: clone}, markerA); got.Content != writtenByProbe {
		t.Fatalf("control: actor %s/%s restored from tag %s reads %s = %q (error %q), want %q — the tag carries no durable data, so the cross-atespace rejection above proves nothing",
			ns.a, clone, tagRef.GetName(), markerA, got.Content, got.Error, writtenByProbe)
	}
}

// deployAtespaces installs both atespaces and returns their names. A goes
// first: it brings the only WorkerPool, and B's golden snapshot needs a worker
// to boot on.
func deployAtespaces(t *testing.T, ctx context.Context, clients *e2e.Clients, bucket string) atespaces {
	t.Helper()
	a, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/atespace-a-pool.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/atespace-template.yaml.tmpl",
	}, bucket, "sec-a", nil)
	b, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/atespace-b-namespace.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/atespace-template.yaml.tmpl",
	}, bucket, "sec-b", nil)
	if a == b {
		t.Fatalf("both fixtures rendered to atespace %q — the fixture no longer isolates anything", a)
	}
	return atespaces{a: a, b: b}
}

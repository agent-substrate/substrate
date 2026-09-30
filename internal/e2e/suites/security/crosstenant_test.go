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

// Package security holds the end-to-end tests for Substrate's security
// boundaries. Its assertions are negative — things that must not be possible —
// so each one is paired with a positive control proving the probe was in a
// position to observe the violation whose absence it asserts. A negative test
// that has silently stopped exercising its boundary still passes, which is how
// a suite like this rots.
//
// This file covers the tenant boundary: the atespace. Two tenants are deployed
// that are identical except for their atespace.
//
// An ateom hosts one actor at a time (internal/ateomcapacity: actorsPerAteom =
// 1), so the two tenants never run together on a worker. The boundary this file
// exercises is therefore worker REUSE across time (the [time] axis of the
// invariant list): the victim tenant runs, leaves its per-actor state behind,
// and vacates the worker; the attacker tenant then reuses that exact worker and
// must find none of it. The fixture pins this with a single-worker pool (see
// fixtures/security/tenant-pool.yaml.tmpl), and the suite asserts the reuse
// happened rather than hoping for it.
package security

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/nodepath"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// tenantTemplate is the ActorTemplate name both tenants deploy; they differ
	// by atespace, not by template name.
	tenantTemplate = "tenant"

	// dataMount is where tenant-template.yaml.tmpl mounts the durable-dir
	// volume whose isolation is under test, and durableVolume is that volume's
	// name — which is also the last path segment of its per-actor mount point
	// on the worker. Keep both in sync with the manifest.
	dataMount     = "/data"
	durableVolume = "data"

	// writtenByProbe is the fixed body the probe's /writefile endpoint writes.
	// The endpoint takes no content, so the two tenants are told apart by the
	// PATH each writes, not by what is in the file.
	writtenByProbe = "written by probe"

	// markerA and markerB are named for the tenant that owns them, so a file
	// that crosses the boundary is attributable rather than a coincidence of
	// two tenants writing identical bodies at one path.
	markerA = dataMount + "/tenant-a-marker"
	markerB = dataMount + "/tenant-b-marker"

	// neverWritten is a path no actor ever creates, used to prove /readfile
	// reports a missing file as an error. Every negative assertion in this file
	// reads "the probe reported an error", so if that endpoint ever started
	// reporting misses as empty successes, all of them would pass vacuously.
	neverWritten = dataMount + "/never-written"
)

// fileResponse mirrors the probe's /readfile and /writefile responses.
type fileResponse struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	OK      string `json:"ok"`
	Error   string `json:"error"`
}

// tenants is the pair of atespaces under test: two tenants of one Substrate
// deployment that take turns on one worker.
type tenants struct {
	a, b string
}

// TestCrossTenantIsolation asserts the atespace boundary holds across worker
// reuse for the two things a tenant most needs kept from its neighbors: its
// stored data and its snapshots.
//
// The shape is victim-then-attacker on one worker: tenant B (victim) runs,
// writes its durable marker, and vacates the worker; tenant A (attacker) then
// reuses that worker and must find none of B's per-actor state. One ateom hosts
// one actor at a time (internal/ateomcapacity: actorsPerAteom = 1), so this
// sequencing is not a convenience — it is the only way two tenants ever meet on
// a worker.
//
// The fixture is deployed once for the whole test rather than per subtest.
// Deploying waits for each tenant's golden snapshot, which on the micro-VM lane
// is a cold boot plus a checkpoint, so a per-subtest fixture would dominate the
// suite's runtime for no added coverage.
func TestCrossTenantIsolation(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	ns := deployTenants(t, ctx, clients, env["BUCKET_NAME"])

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	// Victim: tenant B runs first and leaves its state on the worker. Its UID
	// and worker are captured now, while it is RUNNING, because the attacker
	// probes host paths keyed by the victim's UID and must land on the worker
	// the victim vacates.
	const victim = "tenant-b-actor"
	createAndResumeActor(t, ctx, clients, ns.b, victim)
	victimActor := resources.ActorRef{Atespace: ns.b, Name: victim}
	victimUID := actorUID(t, ctx, clients, ns.b, victim)
	victimWorker := assignedWorker(t, ctx, clients, ns.b, victim)

	writeFile(t, ctx, rc, victimActor, markerB)
	// Control: the victim must be able to read back what it just wrote. This
	// proves markerB was really committed to the victim's durable directory, so
	// the attacker's failure to read it below is the boundary holding, not the
	// marker having never existed.
	if got := readFile(t, ctx, rc, victimActor, markerB); got.Content != writtenByProbe {
		t.Fatalf("control: victim %s reading its OWN %s = %q (error %q), want %q — the probe cannot see its own durable data, so nothing below is meaningful",
			victimActor, markerB, got.Content, got.Error, writtenByProbe)
	}

	// Vacate the worker. The durable marker survives (that is what durable
	// means); the snapshot goes to object storage. The attacker will reuse this
	// worker next.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: ns.b, Name: victim},
	}); err != nil {
		t.Fatalf("SuspendActor %s/%s: %v", ns.b, victim, err)
	}
	waitForActorState(t, ctx, clients, ns.b, victim, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Attacker: tenant A takes the now-free worker.
	const attacker = "tenant-a-actor"
	createAndResumeActor(t, ctx, clients, ns.a, attacker)
	attackerActor := resources.ActorRef{Atespace: ns.a, Name: attacker}
	requireReusedWorker(t, ctx, clients, ns.a, attacker, victimWorker)

	// Controls for every negative assertion below, run before any of them. A
	// failure here is fatal: it means the probe cannot distinguish a denial from
	// a dud, so the subtests would report false passes.
	if got := readFile(t, ctx, rc, attackerActor, neverWritten); got.Error == "" {
		t.Fatalf("control: attacker %s read %s, which nothing ever created, and reported no error (content %q) — /readfile does not report misses as errors, so every negative assertion here would pass vacuously",
			attackerActor, neverWritten, got.Content)
	}
	writeFile(t, ctx, rc, attackerActor, markerA)
	if got := readFile(t, ctx, rc, attackerActor, markerA); got.Content != writtenByProbe {
		t.Fatalf("control: attacker %s reading its OWN %s = %q (error %q), want %q — the probe cannot see its own durable data",
			attackerActor, markerA, got.Content, got.Error, writtenByProbe)
	}

	// EX-4, durable mount: the attacker's /data mount must be keyed to the
	// attacker, not shared with or pointed at the worker the victim used. The
	// victim's marker provably persisted (it read it back above), so reading it
	// through the attacker's own mount would mean the durable directories are
	// shared or mis-keyed across the reuse boundary.
	t.Run("VictimDurableMarkerNotVisibleThroughAttackerMount", func(t *testing.T) {
		if got := readFile(t, ctx, rc, attackerActor, markerB); got.Error == "" {
			t.Errorf("attacker %s read %s, victim tenant %s's durable marker, through its own mount: content %q — durable-dir volumes are shared across the worker-reuse boundary",
				attackerActor, markerB, ns.b, got.Content)
		}
	})

	// EX-4, host state: the subtest above proves the attacker's MOUNT does not
	// expose the victim's data. This proves the sandbox does not expose the host
	// layout that mount is composed from — the victim's durable directory, its
	// projected identity, and its sandbox asset record — which is what a tenant
	// would reach for once its own mount is confined.
	//
	// The paths are rooted in internal/nodepath rather than being hardcoded,
	// so relocating the worker's on-disk layout moves these assertions with it
	// instead of leaving them probing paths that no longer hold anything. The
	// leaves under the per-actor root mirror cmd/atelet/internal/ateletpath,
	// which this suite cannot import; see actorHostPath.
	t.Run("VictimHostStateNotReachableFromAttackerSandbox", func(t *testing.T) {
		for _, probe := range []struct {
			what string
			path string
		}{
			{"the victim's durable marker on the host", actorHostPath(victimUID, "durable-dir", durableVolume, "tenant-b-marker")},
			{"the victim's projected actor identity", actorHostPath(victimUID, "system-info", "system-info", "actor-id")},
			{"the victim's sandbox asset record", actorHostPath(victimUID, "sandbox-assets.json")},
		} {
			if got := readFile(t, ctx, rc, attackerActor, probe.path); got.Error == "" {
				t.Errorf("attacker %s read %s (%s): content %q — the sandbox exposes the worker's per-actor host state across the reuse boundary",
					attackerActor, probe.path, probe.what, got.Content)
			}
		}

		// A write is the higher-severity direction of the same boundary:
		// reaching the victim's durable directory on the host means one tenant
		// can corrupt another's data, not merely read it.
		injected := actorHostPath(victimUID, "durable-dir", durableVolume, "injected")
		if got := writeFileAllowingFailure(t, ctx, rc, attackerActor, injected); got.Error == "" {
			t.Errorf("attacker %s wrote %s, inside victim tenant %s's durable directory on the worker — one tenant can corrupt another's stored data",
				attackerActor, injected, ns.b)
		}
	})

	// Runs last: it suspends the attacker actor to snapshot it, so anything
	// after it would find that actor no longer running.
	t.Run("TagScopedToAtespaceCannotSeedAnotherTenant", func(t *testing.T) {
		testTagScopeBoundary(t, ctx, clients, rc, ns, attacker)
	})
}

// actorHostPath is the worker-host path of a file under an actor's per-actor
// directory: nodepath.ActorsDir/<uid>/<elem...>. The leaves it is called with
// (durable-dir, system-info, sandbox-assets.json) are the ones atelet lays out
// in cmd/atelet/internal/ateletpath, which this suite cannot import.
func actorHostPath(actorUID string, elem ...string) string {
	return filepath.Join(append([]string{nodepath.ActorsDir, actorUID}, elem...)...)
}

// testTagScopeBoundary is the snapshot half of the tenant boundary: an Actor
// must not be seeded from another tenant's snapshot. A snapshot carries the
// source actor's memory, root filesystem and durable data, so loading one
// across the boundary hands over everything the source held.
//
// Substrate expresses this as the Tag's scope, enforced in ateapi's
// CreateActor. The negative case asserts the ERROR MESSAGE, not just the code:
// several unrelated preconditions on that same path also return
// FailedPrecondition, and the tag-scope check happens to run before them, so a
// test matching on the code alone would keep passing if the scope check were
// deleted outright.
//
// source is an already-running actor in tenant A holding markerA; it is
// suspended here to produce the snapshot the tag captures.
func testTagScopeBoundary(t *testing.T, ctx context.Context, clients *e2e.Clients, rc *e2e.RouterClient, ns tenants, source string) {
	t.Helper()

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: ns.a, Name: source},
	})
	if err != nil {
		t.Fatalf("SuspendActor %s/%s: %v", ns.a, source, err)
	}
	if suspended.GetActor().GetStatus().GetExternalSnapshot().GetSnapshotUri() == "" {
		t.Fatalf("suspended actor %s/%s has no external snapshot, so there is nothing for a tag to capture", ns.a, source)
	}

	tagRef := &ateapipb.ObjectRef{Atespace: ns.a, Name: "tenant-a-tag"}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteTag(context.Background(), &ateapipb.DeleteTagRequest{Tag: tagRef})
	})
	// Tags live in the ateapi store and outlive the fixture namespace, so an
	// interrupted earlier run can leak one and wedge every rerun on
	// AlreadyExists. Best-effort clear before creating.
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
	if tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
		t.Fatalf("tag %s/%s has no snapshot uri, so it could seed an Actor in NO atespace and the negative case below would be vacuous",
			ns.a, tagRef.GetName())
	}

	// The boundary: the other tenant may not name this tag.
	crossRef := &ateapipb.ObjectRef{Atespace: ns.b, Name: "cross-tenant-clone"}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{Actor: crossRef})
	})
	_, err = clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ns.b, Name: crossRef.GetName()},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: ns.b, Name: tenantTemplate},
		SourceTag:     tagRef,
	}})
	switch {
	case err == nil:
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s (scope ATESPACE) succeeded — a tenant can load another tenant's snapshot",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName())
	case status.Code(err) != codes.FailedPrecondition:
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s: got %v, want FailedPrecondition",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName(), err)
	case !strings.Contains(status.Convert(err).Message(), "not published outside its Atespace"):
		// Rejected, but by some other precondition on the same path. That is
		// not the boundary under test, and accepting it would mask the scope
		// check being removed.
		t.Errorf("CreateActor %s/%s seeded from tag %s/%s was rejected by a different precondition: %q; want the tag-scope rejection",
			ns.b, crossRef.GetName(), ns.a, tagRef.GetName(), status.Convert(err).Message())
	}

	// Positive control: the same tag seeds an Actor inside its OWN tenant, and
	// the restored actor really carries the source's durable data. Without
	// this, a tag that was simply unusable — an empty snapshot, a template
	// mismatch — would make the rejection above look like enforcement.
	const clone = "same-tenant-clone"
	cloneRef := &ateapipb.ObjectRef{Atespace: ns.a, Name: clone}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: cloneRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: cloneRef})
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: ns.a, Name: clone},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: ns.a, Name: tenantTemplate},
		SourceTag:     tagRef,
	}}); err != nil {
		t.Fatalf("control: CreateActor %s/%s from tag %s in its own atespace: %v — the tag is unusable, so the cross-tenant rejection above proves nothing",
			ns.a, clone, tagRef.GetName(), err)
	}
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: cloneRef}); err != nil {
		t.Fatalf("control: ResumeActor %s/%s: %v", ns.a, clone, err)
	}
	waitForActorState(t, ctx, clients, ns.a, clone, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	if got := readFile(t, ctx, rc, resources.ActorRef{Atespace: ns.a, Name: clone}, markerA); got.Content != writtenByProbe {
		t.Fatalf("control: actor %s/%s restored from tag %s reads %s = %q (error %q), want %q — the tag carries no durable data, so the cross-tenant rejection above proves nothing",
			ns.a, clone, tagRef.GetName(), markerA, got.Content, got.Error, writtenByProbe)
	}
}

// deployTenants installs both halves of the cross-tenant fixture and returns
// their atespaces.
//
// Order matters: tenant A brings the only WorkerPool, and building tenant B's
// golden snapshot needs a worker to boot on, so A has to be up first.
func deployTenants(t *testing.T, ctx context.Context, clients *e2e.Clients, bucket string) tenants {
	t.Helper()
	a, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/tenant-pool.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/tenant-template.yaml.tmpl",
	}, bucket, "sec-a", false)
	b, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/tenant-namespace.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/tenant-template.yaml.tmpl",
	}, bucket, "sec-b", false)
	if a == b {
		t.Fatalf("both tenants rendered to atespace %q — the fixture no longer isolates anything", a)
	}
	return tenants{a: a, b: b}
}

// requireReusedWorker fails unless the actor landed on the worker a previous
// tenant vacated.
//
// One ateom hosts one actor at a time (internal/ateomcapacity: actorsPerAteom =
// 1), so the two tenants never share a worker at once; the boundary this suite
// checks is worker REUSE. The fixture pins it with a single-worker pool, so a
// failure here means the attacker booted on some other worker the victim never
// touched — on which the victim left nothing, and the reuse boundary under test
// would go unexercised.
func requireReusedWorker(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name, priorWorker string) {
	t.Helper()
	got := assignedWorker(t, ctx, clients, atespace, name)
	if got != priorWorker {
		t.Fatalf("actor %s/%s landed on worker %q, not the worker %q the victim vacated; the fixture deploys one worker for both tenants, so the reuse boundary under test is not being exercised",
			atespace, name, got, priorWorker)
	}
	t.Logf("actor %s/%s reused worker %s", atespace, name, priorWorker)
}

func assignedWorker(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor %s/%s: %v", atespace, name, err)
	}
	worker := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	if worker == "" {
		t.Fatalf("actor %s/%s has no worker assignment while RUNNING", atespace, name)
	}
	return worker
}

func actorUID(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) string {
	t.Helper()
	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor %s/%s: %v", atespace, name, err)
	}
	uid := actor.GetMetadata().GetUid()
	if uid == "" {
		t.Fatalf("actor %s/%s has no uid", atespace, name)
	}
	return uid
}

// createAndResumeActor creates an actor from its tenant's template and resumes
// it from the golden snapshot, removing it when the test ends.
func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	// Actor records live in the ateapi store and outlive the fixture namespace,
	// so an interrupted earlier run can leak one and wedge every rerun on
	// AlreadyExists. DeleteActor requires SUSPENDED or CRASHED, hence the
	// suspend first; both are best-effort.
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: tenantTemplate},
	}}); err != nil {
		t.Fatalf("CreateActor %s/%s: %v", atespace, name, err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: ref})
		if _, err := clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: DeleteActor %s/%s failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", atespace, name, name, atespace, err)
		}
	})

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %s/%s: %v", atespace, name, err)
	}
	waitForActorState(t, ctx, clients, atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING)
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for actor %s/%s to reach state %v", atespace, name, want)
}

// writeFile writes the probe's fixed body at path and fails the test if the
// write did not land: callers use it to establish state, so a silent failure
// would turn a later negative assertion into a false pass.
func writeFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) {
	t.Helper()
	if got := writeFileAllowingFailure(t, ctx, rc, actor, path); got.OK != "true" {
		t.Fatalf("actor %s writing %s: %q", actor, path, got.Error)
	}
}

// writeFileAllowingFailure is writeFile for the assertions that EXPECT the
// write to be refused; it returns the probe's report instead of failing.
func writeFileAllowingFailure(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/writefile", path)
}

func readFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/readfile", path)
}

// probeFileOp calls one of the probe's file endpoints. A transport or status
// failure is fatal rather than reported: it means the probe never ran the
// operation at all, which is not the same as the actor being denied, and
// conflating the two is how a negative test starts passing for the wrong
// reason.
func probeFileOp(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, endpoint, path string) fileResponse {
	t.Helper()
	resp, err := rc.Get(ctx, actor, endpoint+"?path="+url.QueryEscape(path))
	if err != nil {
		t.Fatalf("GET %s for %s: %v", endpoint, actor, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s for %s: status %d, body %q", endpoint, actor, resp.StatusCode, body)
	}
	var out fileResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s for %s: %v", endpoint, actor, err)
	}
	return out
}

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
// boundaries. Every assertion here is negative: something must not be
// possible. Each is paired with a positive control showing the probe could
// have seen the violation, because a negative test that stops exercising its
// boundary keeps passing.
//
// This file covers the boundary between two atespaces whose actors are
// identical except for the atespace they run in. The victim actor runs, writes
// state, and vacates its worker; the attacker actor, in the other atespace,
// then reuses that worker and must find none of it. A single-worker pool
// (fixtures/security/atespace-a-pool.yaml.tmpl) forces the reuse, and the test
// checks that it happened.
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
	// templateName is the ActorTemplate name both atespaces deploy.
	templateName = "probe"

	// dataMount is where atespace-template.yaml.tmpl mounts the durable-dir
	// volume, and durableVolume is that volume's name, which is also the last
	// segment of its mount point on the worker. Keep both in sync with the
	// manifest.
	dataMount     = "/data"
	durableVolume = "data"

	// writtenByProbe is the fixed body the probe's /writefile endpoint writes.
	// Actors are told apart by the path they write, not by the content.
	writtenByProbe = "written by probe"

	// markerA and markerB are named for the atespace that owns them, so a file
	// seen across the boundary is attributable.
	markerA = dataMount + "/atespace-a-marker"
	markerB = dataMount + "/atespace-b-marker"

	// neverWritten is a path no actor creates. Reading it proves /readfile
	// reports a missing file as an error, which every negative assertion in
	// this file relies on.
	neverWritten = dataMount + "/never-written"
)

// fileResponse mirrors the probe's /readfile and /writefile responses.
type fileResponse struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	OK      string `json:"ok"`
	Error   string `json:"error"`
}

// atespaces is the pair of atespaces under test.
type atespaces struct {
	a, b string
}

// TestCrossAtespaceIsolation checks that the atespace boundary holds across
// worker reuse for an actor's stored data and its snapshots.
//
// The victim, in atespace B, runs, writes its durable marker, and is
// suspended. The attacker, in atespace A, then starts on the worker the victim
// left and must find none of its state.
//
// The fixture is deployed once for the whole test. Deploying waits for each
// atespace's golden snapshot, which on the micro-VM lane is a cold boot plus a
// checkpoint, so per-subtest fixtures would dominate the runtime.
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

	// Victim: the actor in atespace B runs first. Its UID and worker are recorded while it is
	// RUNNING: the attacker probes host paths keyed by that UID and must land
	// on that worker.
	const victim = "victim"
	createAndResumeActor(t, ctx, clients, ns.b, victim)
	victimActor := resources.ActorRef{Atespace: ns.b, Name: victim}
	victimUID := actorUID(t, ctx, clients, ns.b, victim)
	victimWorker := assignedWorker(t, ctx, clients, ns.b, victim)

	writeFile(t, ctx, rc, victimActor, markerB)
	// Control: the victim reads back its own marker, so the attacker's failure
	// to read it below means the boundary held, not that the marker was never
	// written.
	if got := readFile(t, ctx, rc, victimActor, markerB); got.Content != writtenByProbe {
		t.Fatalf("control: victim %s reading its OWN %s = %q (error %q), want %q — the probe cannot see its own durable data, so nothing below is meaningful",
			victimActor, markerB, got.Content, got.Error, writtenByProbe)
	}

	// Suspend the victim to free the worker. The durable marker stays on the
	// worker; the snapshot goes to object storage.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: ns.b, Name: victim},
	}); err != nil {
		t.Fatalf("SuspendActor %s/%s: %v", ns.b, victim, err)
	}
	waitForActorState(t, ctx, clients, ns.b, victim, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)

	// Attacker: the actor in atespace A takes the freed worker.
	const attacker = "attacker"
	createAndResumeActor(t, ctx, clients, ns.a, attacker)
	attackerActor := resources.ActorRef{Atespace: ns.a, Name: attacker}
	requireReusedWorker(t, ctx, clients, ns.a, attacker, victimWorker)

	// Controls for every negative assertion below. A failure is fatal: the
	// probe cannot tell a denial from a missing file, so the subtests would
	// pass for the wrong reason.
	if got := readFile(t, ctx, rc, attackerActor, neverWritten); got.Error == "" {
		t.Fatalf("control: attacker %s read %s, which nothing ever created, and reported no error (content %q) — /readfile does not report misses as errors, so every negative assertion here would pass vacuously",
			attackerActor, neverWritten, got.Content)
	}
	writeFile(t, ctx, rc, attackerActor, markerA)
	if got := readFile(t, ctx, rc, attackerActor, markerA); got.Content != writtenByProbe {
		t.Fatalf("control: attacker %s reading its OWN %s = %q (error %q), want %q — the probe cannot see its own durable data",
			attackerActor, markerA, got.Content, got.Error, writtenByProbe)
	}

	// Durable mount: the attacker's /data must be its own. The victim's
	// marker is known to exist, so reading it through the attacker's mount
	// means durable directories are shared or mis-keyed across reuse.
	t.Run("VictimDurableMarkerNotVisibleThroughAttackerMount", func(t *testing.T) {
		if got := readFile(t, ctx, rc, attackerActor, markerB); got.Error == "" {
			t.Errorf("attacker %s read %s, the victim's durable marker in atespace %s, through its own mount: content %q — durable-dir volumes are shared across the worker-reuse boundary",
				attackerActor, markerB, ns.b, got.Content)
		}
	})

	// Host state: the mount is confined (above); this checks the sandbox
	// does not expose the host directories that mount is built from. The paths
	// come from internal/nodepath so they move with the worker layout; see
	// actorHostPath.
	t.Run("VictimHostStateNotReachableFromAttackerSandbox", func(t *testing.T) {
		for _, probe := range []struct {
			what string
			path string
		}{
			{"the victim's durable marker on the host", actorHostPath(victimUID, "durable-dir", durableVolume, filepath.Base(markerB))},
			{"the victim's projected actor identity", actorHostPath(victimUID, "system-info", "system-info", "actor-id")},
			{"the victim's sandbox asset record", actorHostPath(victimUID, "sandbox-assets.json")},
		} {
			if got := readFile(t, ctx, rc, attackerActor, probe.path); got.Error == "" {
				t.Errorf("attacker %s read %s (%s): content %q — the sandbox exposes the worker's per-actor host state across the reuse boundary",
					attackerActor, probe.path, probe.what, got.Content)
			}
		}

		// A write is the more serious direction of the same boundary: reaching the
		// victim's durable directory means an actor can corrupt another atespace's data.
		injected := actorHostPath(victimUID, "durable-dir", durableVolume, "injected")
		if got := writeFileAllowingFailure(t, ctx, rc, attackerActor, injected); got.Error == "" {
			t.Errorf("attacker %s wrote %s, inside the victim's durable directory (atespace %s) on the worker — an actor can corrupt another atespace's stored data",
				attackerActor, injected, ns.b)
		}
	})

	// Tag scope. Runs last: it suspends the attacker to snapshot it.
	t.Run("TagScopedToAtespaceCannotSeedAnotherAtespace", func(t *testing.T) {
		testTagScopeBoundary(t, ctx, clients, rc, ns, attacker)
	})
}

// actorHostPath is nodepath.ActorsDir/<uid>/<elem...> on the worker host. The
// leaves (durable-dir, system-info, sandbox-assets.json) mirror
// cmd/atelet/internal/ateletpath, which this suite cannot import.
func actorHostPath(actorUID string, elem ...string) string {
	return filepath.Join(append([]string{nodepath.ActorsDir, actorUID}, elem...)...)
}

// testTagScopeBoundary checks that an Actor cannot be seeded from a snapshot
// taken in another atespace. A snapshot carries the source's memory, root filesystem
// and durable data, so loading one across the boundary hands over everything.
//
// Substrate enforces this through the Tag's scope in ateapi's CreateActor. The
// negative case matches the error message, not just the code.
//
// source is a running actor in atespace A holding markerA; it is suspended here
// to produce the snapshot the tag captures.
func testTagScopeBoundary(t *testing.T, ctx context.Context, clients *e2e.Clients, rc *e2e.RouterClient, ns atespaces, source string) {
	t.Helper()

	suspended, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: ns.a, Name: source},
	})
	if err != nil {
		t.Fatalf("SuspendActor %s/%s: %v", ns.a, source, err)
	}
	if durableSnapshotURI(suspended.GetActor().GetStatus()) == "" {
		t.Fatalf("suspended actor %s/%s has no external snapshot, so there is nothing for a tag to capture", ns.a, source)
	}

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
	}, bucket, "sec-a", false)
	b, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/atespace-b-namespace.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/atespace-template.yaml.tmpl",
	}, bucket, "sec-b", false)
	if a == b {
		t.Fatalf("both fixtures rendered to atespace %q — the fixture no longer isolates anything", a)
	}
	return atespaces{a: a, b: b}
}

// requireReusedWorker fails unless the actor landed on priorWorker. The fixture
// runs one worker for both atespaces, so any other worker means the attacker never
// touched the host the victim left its state on and the test proves nothing.
func requireReusedWorker(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name, priorWorker string) {
	t.Helper()
	got := assignedWorker(t, ctx, clients, atespace, name)
	if got != priorWorker {
		t.Fatalf("actor %s/%s landed on worker %q, not the worker %q the victim vacated; the fixture deploys one worker for both atespaces, so the reuse boundary under test is not being exercised",
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

// createAndResumeActor creates an actor from its atespace's template and resumes
// it from the golden snapshot, removing it when the test ends.
func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, name string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	// Actor records outlive the fixture namespace, so an interrupted run can
	// leave one behind and wedge reruns on AlreadyExists. DeleteActor needs
	// SUSPENDED or CRASHED, hence the suspend; both are best-effort.
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: templateName},
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
// write did not land, so a later negative assertion cannot pass on missing
// state.
func writeFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) {
	t.Helper()
	if got := writeFileAllowingFailure(t, ctx, rc, actor, path); got.OK != "true" {
		t.Fatalf("actor %s writing %s: %q", actor, path, got.Error)
	}
}

// writeFileAllowingFailure is writeFile for assertions that expect the write
// to be refused; it returns the probe's report instead of failing.
func writeFileAllowingFailure(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/writefile", path)
}

func readFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, actor resources.ActorRef, path string) fileResponse {
	t.Helper()
	return probeFileOp(t, ctx, rc, actor, "/readfile", path)
}

// probeFileOp calls one of the probe's file endpoints. A transport or HTTP
// failure is fatal rather than returned: the probe never ran the operation,
// which is not the same as the actor being denied.
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

func durableSnapshotURI(status *ateapipb.ActorStatus) string {
	var best *ateapipb.Snapshot
	for _, snap := range status.GetSnapshots() {
		if uri := snapshotURI(snap); uri != "" {
			if best == nil || snap.GetGeneration() > best.GetGeneration() {
				best = snap
			}
		}
	}
	return snapshotURI(best)
}

func snapshotURI(snap *ateapipb.Snapshot) string {
	for _, st := range snap.GetStorage() {
		if st.GetDurability() == ateapipb.SnapshotDurability_SNAPSHOT_DURABILITY_DURABLE &&
			st.GetStatus() == ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
			return st.GetObject().GetSnapshotUri()
		}
	}
	return ""
}

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

package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const probeTemplate = "probe"

// probeNamespace is the suite's own probe fixture namespace, and the atespace
// its actors live in.
var probeNamespace string

type whoamiResponse struct {
	File     string `json:"file"`
	Atespace string `json:"atespace"`
	UID      string `json:"uid"`
	Trust    string `json:"trust"`
	Hostname string `json:"hostname"`
	// Held is the actor id read through a file descriptor the probe opened at
	// startup and holds across checkpoints — the snapshot therefore carries an
	// open guest handle on a system-info file, and restore must re-bind it to
	// the regenerated file (virtiofsd find-paths / gofer re-open by path).
	Held string `json:"held"`
	// Error is the probe's file read error(s), if any, so a failed assertion
	// explains why a value was missing.
	Error string `json:"error"`
}

// TestActorIdentity_AfterRestore_IsOwnID_NotGolden is the regression gate for
// per-actor identity. The env-var approach passed unit tests and config.json
// inspection yet was broken at runtime: actors restored from the shared golden
// snapshot all reported the golden actor's ID. This test catches that by
// restoring TWO actors from one golden snapshot and asserting each observes its
// OWN id — and explicitly that it is not the golden id.
//
// It then suspends and resumes one of them: atelet wipes and regenerates the
// system-info files between suspend and resume, so the suspend-time guest
// state (the probe's startup-held fd, plus every inode the pre-suspend whoami
// indexed) must re-bind to the regenerated files at the same paths. The
// micro-VM lane enforces that the hardest: virtiofsd's find-paths migration
// re-opens recorded paths on restore, and a path that moved leaves the guest
// reference faulty (EIO under --migration-on-error=guest-error), which the
// held-fd assertion below catches — a write scheme that relocates real files
// (e.g. a timestamped-directory symlink swap) would break every held fd of
// any actor that ever touched a system-info file.
func TestActorIdentity_AfterRestore_IsOwnID_NotGolden(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	// Own the pool's contents before the fixture deploys (DeployProbe only
	// ensures a bundle EXISTS): the assertions below compare the projected
	// file against this run's CA, and rotation later replaces it again.
	wantTrust := e2e.ReplaceEgressTrustPool(t, ctx, clients, "ate-e2e-probe-trust")
	var tmpl *ateapipb.ActorTemplate
	probeNamespace, tmpl = e2e.DeployProbe(t, env["BUCKET_NAME"], "identity", e2e.WithTrustBundle())
	// The golden actor's id, for the not-golden assertion below. Coupled to
	// the reconciler's naming: the golden actor is named after the template's
	// UID (cmd/ateapi template reconciler), so a naming change there weakens
	// this check to a no-op rather than false-failing it.
	golden := tmpl.GetMetadata().GetUid()

	// Two distinct actors from the same golden snapshot.
	ids := []string{"probe-alpha", "probe-beta"}
	for _, id := range ids {
		createAndResumeActor(t, ctx, clients, id)
	}

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	seen := map[string]string{}
	seenUIDs := map[string]string{}
	for _, id := range ids {
		got := whoami(t, ctx, rc, id)

		if got.File != id {
			t.Errorf("actor %q: /run/ate/actor-id = %q, want %q (probe read error: %q)", id, got.File, id, got.Error)
		}
		if got.File == golden {
			t.Errorf("actor %q: identity is the GOLDEN snapshot id %q — restore leaked shared state", id, golden)
		}
		if other, dup := seen[got.File]; dup {
			t.Errorf("actor %q and %q both report identity %q — actors are not distinct", id, other, got.File)
		}
		seen[got.File] = id

		// The fd held open since before the golden snapshot must survive the
		// restore and read the restored actor's OWN id: system-info files are
		// regenerated at stable paths precisely so suspend-time guest handles
		// re-bind (a moved or deleted path would leave the handle faulty and
		// fail this read).
		if got.Held != id {
			t.Errorf("actor %q: id via startup-held fd = %q, want %q (probe read error: %q)", id, got.Held, id, got.Error)
		}

		if got.Atespace != probeNamespace {
			t.Errorf("actor %q: /run/ate/atespace = %q, want %q (probe read error: %q)", id, got.Atespace, probeNamespace, got.Error)
		}

		// The projected trust bundle must be this run's pool CA, as published
		// by the reconciler and sanitized by atelet (byte-identical here: the
		// reconciler emits clean CERTIFICATE blocks; junk-tolerant
		// sanitization is pinned by the resolve and pemutil unit tests).
		if got.Trust != wantTrust {
			t.Errorf("actor %q: /run/ate/trust-bundle.pem = %q, want the sanitized bundle %q (probe read error: %q)", id, got.Trust, wantTrust, got.Error)
		}

		// The projected UID must match the control plane's authoritative view
		// of this actor, and be distinct per actor even though both actors
		// were seeded from the same golden snapshot.
		actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}})
		if err != nil {
			t.Fatalf("GetActor %q: %v", id, err)
		}
		if wantUID := actor.GetMetadata().GetUid(); got.UID != wantUID {
			t.Errorf("actor %q: /run/ate/actor-uid = %q, want %q (probe read error: %q)", id, got.UID, wantUID, got.Error)
		}
		if other, dup := seenUIDs[got.UID]; dup {
			t.Errorf("actor %q and %q both report uid %q — actors are not distinct", id, other, got.UID)
		}
		seenUIDs[got.UID] = id
	}

	// Live refresh: rotate the pool while both actors RUN — no suspend, no
	// resume — and wait until each observes the new sanitized contents at
	// the same path. atelet's informer rewrites the projected file (temp +
	// rename at the stable path) for every running actor that projects the
	// bundle, and both runtimes must surface the host-side rename on their
	// next read (gVisor revalidating the bind mount through the gofer, the
	// micro-VM through virtio-fs).
	liveTrust := e2e.ReplaceEgressTrustPool(t, ctx, clients, "ate-e2e-probe-trust-live")
	for _, id := range ids {
		waitForTrust(t, ctx, rc, id, liveTrust)
	}

	// Full suspend/resume cycle of one actor (see the doc comment): the whoami
	// calls above deliberately seeded the guest state a suspend records — the
	// held fd from probe startup plus the freshly indexed file inodes — and the
	// resume regenerates every file underneath that state.
	//
	// The trust bundle is rotated first, and the suspend deliberately does
	// NOT wait for the rotation to propagate live: whichever side of the
	// suspend the live rewrite lands on, the resumed actor must observe the
	// rotated sanitized contents at the same path.
	rotatedTrust := e2e.ReplaceEgressTrustPool(t, ctx, clients, "ate-e2e-probe-trust-rotated")
	id := ids[0]
	ref := &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor %q: %v", id, err)
	}
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor %q (after suspend): %v", id, err)
	}
	waitForActorState(t, ctx, clients, id, ateapipb.ActorState_ACTOR_STATE_RUNNING)

	got := whoami(t, ctx, rc, id)
	if got.File != id {
		t.Errorf("after suspend/resume: /run/ate/actor-id = %q, want %q (probe read error: %q)", got.File, id, got.Error)
	}
	if got.Held != id {
		t.Errorf("after suspend/resume: id via startup-held fd = %q, want %q (probe read error: %q)", got.Held, id, got.Error)
	}
	if got.Atespace != probeNamespace {
		t.Errorf("after suspend/resume: /run/ate/atespace = %q, want %q (probe read error: %q)", got.Atespace, probeNamespace, got.Error)
	}
	if wantUID := seenUIDFor(t, seenUIDs, id); got.UID != wantUID {
		t.Errorf("after suspend/resume: /run/ate/actor-uid = %q, want %q (probe read error: %q)", got.UID, wantUID, got.Error)
	}
	if got.Trust != rotatedTrust {
		t.Errorf("after suspend/resume: /run/ate/trust-bundle.pem = %q, want the rotated sanitized bundle %q (probe read error: %q)", got.Trust, rotatedTrust, got.Error)
	}

	// The other actor never cycled: the second rotation must reach it live,
	// undisturbed by a sibling of the same bundle suspending and resuming.
	waitForTrust(t, ctx, rc, ids[1], rotatedTrust)
}

// waitForTrust polls the probe until its projected trust bundle equals want:
// the live-refresh chain (reconciler -> ClusterTrustBundle -> atelet informer
// -> host rewrite -> guest revalidation) exposes no completion signal to wait
// on.
func waitForTrust(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var got whoamiResponse
	var lastErr error
	for time.Now().Before(deadline) {
		if got, lastErr = tryWhoami(ctx, rc, id); lastErr == nil && got.Trust == want {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("actor %q: timed out waiting for the live-refreshed trust bundle: /run/ate/trust-bundle.pem = %q, want %q (probe read error: %q, poll error: %v)", id, got.Trust, want, got.Error, lastErr)
}

// seenUIDFor returns the UID recorded for actor id in the first phase of the
// test, so the post-resume assertion checks against the same authoritative
// value rather than a fresh lookup that could mask a UID change.
func seenUIDFor(t *testing.T, seenUIDs map[string]string, id string) string {
	t.Helper()
	for uid, actor := range seenUIDs {
		if actor == id {
			return uid
		}
	}
	t.Fatalf("no UID recorded for actor %q", id)
	return ""
}

func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, actorName string, want ateapipb.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: actorName},
		})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach state %v", actorName, want)
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	ref := &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}
	// The actor record lives in the ateapi store and outlives the fixture
	// namespace, so a failed prior run can leak it and wedge every rerun on
	// AlreadyExists. Best-effort clear it before creating (DeleteActor
	// requires SUSPENDED or CRASHED, hence the suspend first).
	_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
	_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: probeNamespace, Name: id},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: probeTemplate},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		// Suspend is best-effort: the actor may already be suspended, or may
		// never have resumed. A failed delete is only logged — the pre-create
		// clear above keeps the next run working regardless.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		if _, err := clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref}); err != nil {
			t.Logf("cleanup: DeleteActor %q failed, actor leaked (remove with: kubectl ate delete actor %s -a %s): %v", id, id, probeNamespace, err)
		}
	})

	// Resume from the golden snapshot (the restore path, not --boot).
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: probeNamespace, Name: id}}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func whoami(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id string) whoamiResponse {
	t.Helper()
	out, err := tryWhoami(ctx, rc, id)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// tryWhoami is whoami without the fatality, for polls that must ride out a
// transient router or port-forward hiccup.
func tryWhoami(ctx context.Context, rc *e2e.RouterClient, id string) (whoamiResponse, error) {
	var out whoamiResponse
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: probeNamespace, Name: id}, "/whoami")
	if err != nil {
		return out, fmt.Errorf("GET /whoami for %q: %w", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return out, fmt.Errorf("GET /whoami for %q: status %d, body %q", id, resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("decoding /whoami for %q: %w", id, err)
	}
	return out, nil
}

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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	mvmNamespace = "ate-e2e-probe-mvm"
	mvmTemplate  = "probe"
)

// TestActorIdentity_Microvm_SuspendResume_StablePaths pins the system-info
// path-stability contract on the micro-VM runtime, where it is enforced the
// hardest: every virtiofsd runs with --migration-mode find-paths, so a resume
// re-binds the guest's FUSE state by re-opening the paths recorded at
// suspend, and virtiofsd's default --migration-on-error=abort hard-fails the
// restore if any recorded path is missing. atelet regenerates the system-info
// files between suspend and resume (that is the feature: the files carry the
// resuming actor's identity), so the regenerated files must land at exactly
// the paths the guest recorded — a write scheme that moves real paths (e.g. a
// timestamped-directory symlink swap) makes any actor that ever touched a
// system-info file fail to resume.
//
// The guest state is seeded two ways before each suspend: the probe holds an
// fd on /run/ate/actor-id from startup, and the pre-suspend /whoami call
// freshly indexes all three files (find-paths records all indexed inodes, not
// just open fds).
//
// Two migrations are exercised: the restore-from-golden that creates the
// actor, and a full suspend->resume cycle of the actor itself.
func TestActorIdentity_Microvm_SuspendResume_StablePaths(t *testing.T) {
	// Micro-VM actors need the cluster to have the micro-VM deps installed
	// (KVM, cloud-hypervisor, virtiofsd, the "microvm" SandboxConfig). The
	// microvm CI job signals that the same way the demo suite detects it.
	if os.Getenv("E2E_TEMPLATE_NAMESPACE") != "ate-demo-counter-microvm" {
		t.Skip("micro-VM e2e deps not available (E2E_TEMPLATE_NAMESPACE != ate-demo-counter-microvm)")
	}

	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	deployMicrovmProbe(t, env["BUCKET_NAME"])
	waitForMicrovmGolden(t, ctx, clients)

	const id = "probe-mvm-alpha"
	createAndResumeMicrovmActor(t, ctx, clients, id)
	waitForMicrovmActorStatus(t, ctx, clients, id, ateapipb.Actor_STATUS_RUNNING)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	actor, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}})
	if err != nil {
		t.Fatalf("GetActor %q: %v", id, err)
	}
	wantUID := actor.GetMetadata().GetUid()

	// Migration #1: the actor was just restored from the golden snapshot,
	// whose guest already held an fd on actor-id from probe startup. This
	// call both asserts that re-bind worked and freshly indexes all three
	// files in the guest, seeding the state the next suspend records.
	assertOwnIdentity(t, "after restore-from-golden", whoamiMicrovm(t, ctx, rc, id), id, wantUID)

	// Migration #2: suspend and resume the actor itself. The suspend-time
	// guest state now references the system-info files both via the held fd
	// and the indexed inodes from the call above; the resume regenerates the
	// files and find-paths must re-bind every recorded path.
	if _, err := clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}}); err != nil {
		t.Fatalf("SuspendActor %q: %v", id, err)
	}
	waitForMicrovmActorStatus(t, ctx, clients, id, ateapipb.Actor_STATUS_SUSPENDED)

	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}}); err != nil {
		t.Fatalf("ResumeActor %q (after suspend): %v", id, err)
	}
	waitForMicrovmActorStatus(t, ctx, clients, id, ateapipb.Actor_STATUS_RUNNING)

	assertOwnIdentity(t, "after suspend/resume", whoamiMicrovm(t, ctx, rc, id), id, wantUID)
}

// assertOwnIdentity asserts every identity channel reports the actor's own
// values: the at-request-time file reads, the projected UID, and the read
// through the fd held open since probe startup (the guest handle a restore
// must re-bind).
func assertOwnIdentity(t *testing.T, phase string, got whoamiResponse, id, wantUID string) {
	t.Helper()
	if got.File != id {
		t.Errorf("%s: /run/ate/actor-id = %q, want %q (probe read error: %q)", phase, got.File, id, got.Error)
	}
	if got.Held != id {
		t.Errorf("%s: id via startup-held fd = %q, want %q (probe read error: %q)", phase, got.Held, id, got.Error)
	}
	if got.Atespace != mvmNamespace {
		t.Errorf("%s: /run/ate/atespace = %q, want %q (probe read error: %q)", phase, got.Atespace, mvmNamespace, got.Error)
	}
	if got.UID != wantUID {
		t.Errorf("%s: /run/ate/actor-uid = %q, want %q (probe read error: %q)", phase, got.UID, wantUID, got.Error)
	}
}

// The helpers below mirror the gVisor probe's (identity_test.go) with the
// micro-VM namespace/template; kept separate rather than parameterized so the
// two fixtures stay independently deployable and deletable.

func deployMicrovmProbe(t *testing.T, bucket string) {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	tmpl, err := os.ReadFile(filepath.Join(root, "internal/e2e/fixtures/probe/probe-microvm.yaml.tmpl"))
	if err != nil {
		t.Fatalf("reading probe-microvm manifest template: %v", err)
	}
	manifest := filepath.Join(t.TempDir(), "probe-microvm.yaml")
	rendered := strings.ReplaceAll(string(tmpl), "${BUCKET_NAME}", bucket)
	if err := os.WriteFile(manifest, []byte(rendered), 0o644); err != nil {
		t.Fatalf("writing rendered probe-microvm manifest: %v", err)
	}

	// See deployProbe for why apply goes through the pinned ko and
	// KO_CONFIG_PATH must point at the repo root.
	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if e2e.KubeContext != "" {
			delArgs = append([]string{"--context=" + e2e.KubeContext}, delArgs...)
		}
		e2e.RunCmd(t, "kubectl", delArgs...)
	})
}

func waitForMicrovmGolden(t *testing.T, ctx context.Context, clients *e2e.Clients) {
	t.Helper()
	// Micro-VM golden creation cold-boots a VM (kernel + agent + virtiofsds),
	// so allow longer than the gVisor probe's wait.
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(mvmNamespace).Get(ctx, mvmTemplate, metav1.GetOptions{})
		if err == nil {
			switch at.Status.Phase {
			case v1alpha1.PhaseReady:
				t.Logf("probe-microvm ActorTemplate ready, golden=%s", at.Status.GoldenActorID)
				return
			case v1alpha1.PhaseFailed:
				t.Fatalf("probe-microvm ActorTemplate entered PhaseFailed")
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for probe-microvm ActorTemplate to be Ready")
}

func createAndResumeMicrovmActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: mvmNamespace}}})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: mvmNamespace, Name: id},
		ActorTemplateNamespace: mvmNamespace,
		ActorTemplateName:      mvmTemplate,
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}})
	})

	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id}}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func waitForMicrovmActorStatus(t *testing.T, ctx context.Context, clients *e2e.Clients, id string, want ateapipb.Actor_Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: mvmNamespace, Name: id},
		})
		if err == nil && resp.GetStatus() == want {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for actor %q to reach status %v", id, want)
}

func whoamiMicrovm(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id string) whoamiResponse {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: mvmNamespace, Name: id}, "/whoami")
	if err != nil {
		t.Fatalf("GET /whoami for %q: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /whoami for %q: status %d, body %q", id, resp.StatusCode, body)
	}
	var out whoamiResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /whoami for %q: %v", id, err)
	}
	return out
}

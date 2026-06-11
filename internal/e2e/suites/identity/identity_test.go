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
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	probeNamespace = "ate-e2e-probe"
	probeTemplate  = "probe"
)

type whoamiResponse struct {
	File     string `json:"file"`
	Hostname string `json:"hostname"`
	// Error is the probe's identity-file read error, if any, so a failed
	// assertion explains why the ID was missing.
	Error string `json:"error"`
}

// TestActorIdentity_AfterRestore_IsOwnID_NotGolden is the regression gate for
// per-actor identity. The env-var approach passed unit tests and config.json
// inspection yet was broken at runtime: actors restored from the shared golden
// snapshot all reported the golden actor's ID. This test catches that by
// restoring TWO actors from one golden snapshot and asserting each observes its
// OWN id — and explicitly that it is not the golden id.
func TestActorIdentity_AfterRestore_IsOwnID_NotGolden(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	deployProbe(t, env["BUCKET_NAME"])
	golden := waitForGolden(t, ctx, clients)

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
	}
}

func deployProbe(t *testing.T, bucket string) {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	// Render the manifest template to a file so both apply and delete can
	// consume it without any shell involved.
	tmpl, err := os.ReadFile(filepath.Join(root, "internal/e2e/fixtures/probe/probe.yaml.tmpl"))
	if err != nil {
		t.Fatalf("reading probe manifest template: %v", err)
	}
	manifest := filepath.Join(t.TempDir(), "probe.yaml")
	rendered := strings.ReplaceAll(string(tmpl), "${BUCKET_NAME}", bucket)
	if err := os.WriteFile(manifest, []byte(rendered), 0o644); err != nil {
		t.Fatalf("writing rendered probe manifest: %v", err)
	}

	// Build/push the probe image and apply the manifest through the repo's
	// pinned ko (hack/run-tool.sh ko); CI does not install ko on PATH, and every
	// other deploy in this repo goes through this wrapper. The trailing
	// `-- --context=...` mirrors run_ko in hack/install-ate.sh: ko's apply
	// subcommand forwards args after `--` to kubectl. KO_CONFIG_PATH is
	// required because ko resolves .ko.yaml from its working directory, which
	// is the test's package dir, not the repo root; without it the build
	// silently loses defaultPlatforms (and produces amd64-only images that
	// cannot run on arm64 nodes).
	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		// Deletion needs no image build, so go straight to kubectl (matching
		// demo-counter_delete in hack/install-demo-counter.sh). `ko delete`
		// rejects this arg shape ("you may not specify resource arguments as
		// well").
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if e2e.KubeContext != "" {
			delArgs = append([]string{"--context=" + e2e.KubeContext}, delArgs...)
		}
		e2e.RunCmd(t, "kubectl", delArgs...)
	})
}

func waitForGolden(t *testing.T, ctx context.Context, clients *e2e.Clients) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(probeNamespace).Get(ctx, probeTemplate, metav1.GetOptions{})
		if err == nil {
			switch at.Status.Phase {
			case v1alpha1.PhaseReady:
				t.Logf("probe ActorTemplate ready, golden=%s", at.Status.GoldenActorID)
				return at.Status.GoldenActorID
			case v1alpha1.PhaseFailed:
				t.Fatalf("probe ActorTemplate entered PhaseFailed")
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for probe ActorTemplate to be Ready")
	return ""
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorId:                id,
		ActorTemplateNamespace: probeNamespace,
		ActorTemplateName:      probeTemplate,
	}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		// DeleteActor requires the actor to be suspended.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{ActorId: id})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{ActorId: id})
	})

	// Resume from the golden snapshot (the restore path, not --boot).
	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{ActorId: id}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func whoami(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id string) whoamiResponse {
	t.Helper()
	resp, err := rc.Get(ctx, id, "/whoami")
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

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

package sizing

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	sizingTemplate = "probe-sized"

	// The limits declared in probe-sized.yaml.tmpl. Keep these in sync with the
	// manifest: the whole point of the suite is to assert the sandbox observes
	// exactly what the ActorTemplate declared.
	wantCPU      = 2
	wantMemBytes = 512 * 1024 * 1024 // 512Mi
)

// sizingNamespace is where deploySizedProbe applies the fixture, and the
// atespace its actor lives in. Suffixed per sandbox class (see
// e2e.FixtureName) so the two lanes' fixtures never collide.
var sizingNamespace = e2e.FixtureName("ate-e2e") + "-sizing"

// resourcesResponse mirrors the /resources endpoint of the probe fixture.
type resourcesResponse struct {
	NumCPU        int    `json:"num_cpu"`
	MemTotalBytes int64  `json:"mem_total_bytes"`
	MemTotalError string `json:"mem_total_error"`
	CPUMax        string `json:"cpu_max"`
	MemoryMax     string `json:"memory_max"`
}

// TestActorSizing_SandboxObservesDeclaredLimits is the end-to-end gate for the
// resource-limits redesign: an ActorTemplate that declares spec.resources.limits
// must produce a sandbox sized to those limits. The plumbing unit tests prove
// the limits reach the OCI spec; this proves the running sandbox actually
// honors them, by resuming an actor and asking it (via the probe /resources
// endpoint) what compute envelope it sees from the inside.
//
// Both runtimes reach the declared numbers by different routes, and the
// assertions below are written to hold for either: gVisor sizes the sentry from
// the CPU quota and the cgroup memory limit, while the micro-VM sizes the guest
// VM itself (see internal/sizing).
func TestActorSizing_SandboxObservesDeclaredLimits(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	deploySizedProbe(t, env["BUCKET_NAME"])
	waitForTemplateReady(t, ctx, clients)

	const id = "sized-actor"
	createAndResumeActor(t, ctx, clients, id)

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	got := getResources(t, ctx, rc, id)
	t.Logf("sandbox /resources: num_cpu=%d mem_total_bytes=%d cpu_max=%q memory_max=%q mem_total_error=%q",
		got.NumCPU, got.MemTotalBytes, got.CPUMax, got.MemoryMax, got.MemTotalError)

	// CPU: runsc provisions the sentry's vCPU count from the CPU quota
	// (--cpu-num-from-quota) and the micro-VM boots the guest with
	// SandboxSize.VCPUs(); either way the sandbox must see exactly the declared
	// limit.
	if got.NumCPU != wantCPU {
		t.Errorf("sandbox NumCPU = %d, want %d (declared limits.cpu=%d) — sandbox not sized to actor limits", got.NumCPU, wantCPU, wantCPU)
	}

	// Memory: the sandbox must be bounded by the declared limit. Both runtimes
	// report under it — gVisor by its reserved overhead, the micro-VM by the
	// 128MiB VMM reserve held back from the guest plus what the guest kernel
	// takes — but neither may see more; a value near the node's full RAM means
	// the limit was not applied. Allow 10% headroom above the limit for
	// accounting differences, and half the limit below it for those overheads.
	if got.MemTotalError != "" {
		t.Errorf("probe could not read MemTotal: %s", got.MemTotalError)
	} else if got.MemTotalBytes > wantMemBytes*11/10 {
		t.Errorf("sandbox MemTotal = %d bytes, want <= ~%d (declared limits.memory=512Mi) — memory limit not applied", got.MemTotalBytes, wantMemBytes)
	} else if got.MemTotalBytes < wantMemBytes/2 {
		t.Errorf("sandbox MemTotal = %d bytes, unexpectedly far below the declared 512Mi limit", got.MemTotalBytes)
	}
}

func deploySizedProbe(t *testing.T, bucket string) {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	// One manifest, rendered for the sandbox class under test (mirrors the
	// identity suite).
	manifest := e2e.RenderFixtureManifest(t, "internal/e2e/fixtures/probe/probe-sized.yaml.tmpl", bucket, "sizing")

	// Build/push the probe image and apply through the repo's pinned ko. See the
	// identity suite's deployProbe for why KO_CONFIG_PATH and the trailing
	// `-- --context=...` are required.
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

func waitForTemplateReady(t *testing.T, ctx context.Context, clients *e2e.Clients) {
	t.Helper()
	deadline := time.Now().Add(e2e.TemplateReadyTimeout(t))
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(sizingNamespace).Get(ctx, sizingTemplate, metav1.GetOptions{})
		if err == nil {
			switch at.Status.Phase {
			case v1alpha1.PhaseReady:
				t.Logf("sized probe ActorTemplate ready, golden=%s", at.Status.GoldenActorID)
				return
			case v1alpha1.PhaseFailed:
				t.Fatalf("sized probe ActorTemplate entered PhaseFailed")
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for sized probe ActorTemplate to be Ready")
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, id string) {
	t.Helper()
	// CreateActor requires the atespace to exist first.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: sizingNamespace}}})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: sizingNamespace, Name: id},
		ActorTemplateNamespace: sizingNamespace,
		ActorTemplateName:      sizingTemplate,
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		// DeleteActor requires the actor to be suspended.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: sizingNamespace, Name: id}})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: sizingNamespace, Name: id}})
	})

	// Resume from the golden snapshot (the restore path, not --boot).
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: &ateapipb.ObjectRef{Atespace: sizingNamespace, Name: id}}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func getResources(t *testing.T, ctx context.Context, rc *e2e.RouterClient, id string) resourcesResponse {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: sizingNamespace, Name: id}, "/resources")
	if err != nil {
		t.Fatalf("GET /resources for %q: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /resources for %q: status %d, body %q", id, resp.StatusCode, body)
	}
	var out resourcesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /resources for %q: %v", id, err)
	}
	return out
}

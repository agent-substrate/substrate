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

package capabilities

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// defaultCapabilities mirrors atelet's default set (cmd/atelet/oci.go). It is
// written out rather than imported so that changing the default is a deliberate
// two-place edit, and so this suite fails if the default silently drifts.
var defaultCapabilities = []string{"KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"}

// capabilitiesResponse mirrors the probe's /capabilities payload.
type capabilitiesResponse struct {
	Bounding    []string `json:"bounding"`
	Effective   []string `json:"effective"`
	Permitted   []string `json:"permitted"`
	Inheritable []string `json:"inheritable"`
	Ambient     []string `json:"ambient"`
	Error       string   `json:"error"`
}

// TestActorCapabilities asserts that an ActorTemplate's
// securityContext.capabilities is actually in force inside the sandbox, as the
// kernel reports it — not merely present in the OCI spec atelet writes. atelet
// does not spawn containers, so the spec being right and the sandbox applying
// it are separate claims; only this test covers the second.
//
// It runs against whichever sandbox class E2E_SANDBOX_CLASS selects, so CI
// covers both gvisor and micro-VM from one suite.
func TestActorCapabilities(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	namespace := deployFixture(t, env["BUCKET_NAME"])

	tests := []struct {
		name     string
		template string
		// want is the exact bounding set, in kernel bit order as the probe
		// reports it.
		want []string
	}{{
		name:     "no securityContext keeps the default set",
		template: "caps-default",
		want:     defaultCapabilities,
	}, {
		// Proves both halves at once: everything default was dropped, and only
		// the named capability came back.
		name:     "drop ALL plus add yields exactly the added capability",
		template: "caps-exact",
		want:     []string{"NET_BIND_SERVICE"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			waitForGolden(t, ctx, clients, namespace, tt.template)
			actor := tt.template + "-actor"
			createAndResumeActor(t, ctx, clients, namespace, tt.template, actor)

			rc, err := e2e.NewRouterClient(ctx)
			if err != nil {
				t.Fatalf("NewRouterClient: %v", err)
			}
			defer rc.Close()

			got := probeCapabilities(t, ctx, rc, namespace, actor)
			if got.Error != "" {
				t.Fatalf("probe reported an error reading its capabilities: %s", got.Error)
			}

			// Bounding is the ceiling: no set can exceed it, so asserting it
			// exactly is what proves a dropped capability is truly gone rather
			// than merely inactive.
			assertSameCapabilities(t, "bounding", got.Bounding, tt.want)
			assertSameCapabilities(t, "effective", got.Effective, tt.want)
			assertSameCapabilities(t, "permitted", got.Permitted, tt.want)

			// Inheritable only applies on execve and would let a container that
			// drops to an unprivileged uid regain a capability (CVE-2022-24769);
			// ambient is not supported. Both must reach the guest empty.
			if len(got.Inheritable) != 0 {
				t.Errorf("inheritable = %v, want empty", got.Inheritable)
			}
			if len(got.Ambient) != 0 {
				t.Errorf("ambient = %v, want empty (ambient capabilities are not supported)", got.Ambient)
			}
		})
	}
}

// assertSameCapabilities compares two capability sets irrespective of order.
func assertSameCapabilities(t *testing.T, set string, got, want []string) {
	t.Helper()
	g := slices.Clone(got)
	w := slices.Clone(want)
	slices.Sort(g)
	slices.Sort(w)
	if !slices.Equal(g, w) {
		t.Errorf("%s capability set = %v, want %v", set, g, w)
	}
}

// deployFixture renders and applies the fixture for the sandbox class under
// test and returns the namespace it created. The namespace carries the class
// suffix so the gVisor and micro-VM lanes never share one.
func deployFixture(t *testing.T, bucket string) string {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}

	namespace := e2e.FixtureName("ate-e2e-caps")

	// One manifest, rendered for the sandbox class under test (mirrors the
	// sizing suite).
	manifest := e2e.RenderFixtureManifest(t, "internal/e2e/fixtures/capabilities/capabilities.yaml.tmpl", bucket)

	// Build/push the probe image and apply through the repo's pinned ko, as the
	// identity suite does; CI does not install ko on PATH, and KO_CONFIG_PATH is
	// required because ko resolves .ko.yaml from its working directory.
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

	return namespace
}

func waitForGolden(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, template string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		at, err := clients.SubstrateK8s.ApiV1alpha1().ActorTemplates(namespace).Get(ctx, template, metav1.GetOptions{})
		if err == nil {
			switch at.Status.Phase {
			case v1alpha1.PhaseReady:
				t.Logf("ActorTemplate %s ready, golden=%s", template, at.Status.GoldenActorID)
				return
			case v1alpha1.PhaseFailed:
				// A template whose container cannot start — for example because
				// a needed capability was dropped — lands here rather than
				// timing out, so say so plainly.
				t.Fatalf("ActorTemplate %s entered PhaseFailed; its container never became ready", template)
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for ActorTemplate %s to be Ready", template)
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, template, id string) {
	t.Helper()
	// CreateActor requires the atespace to exist first.
	_, _ = clients.SubstrateAPI.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: namespace}},
	})
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: namespace, Name: id},
		ActorTemplateNamespace: namespace,
		ActorTemplateName:      template,
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		// DeleteActor requires the actor to be suspended.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: namespace, Name: id}})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: &ateapipb.ObjectRef{Atespace: namespace, Name: id}})
	})

	if _, err := clients.SubstrateAPI.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: namespace, Name: id},
	}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}

func probeCapabilities(t *testing.T, ctx context.Context, rc *e2e.RouterClient, namespace, id string) capabilitiesResponse {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: namespace, Name: id}, "/capabilities")
	if err != nil {
		t.Fatalf("GET /capabilities for %q: %v", id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /capabilities for %q: status %d, body %q", id, resp.StatusCode, body)
	}
	var out capabilitiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /capabilities for %q: %v", id, err)
	}
	return out
}

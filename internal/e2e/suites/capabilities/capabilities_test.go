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
	"net/url"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
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
	NoNewPrivs  bool     `json:"noNewPrivs"`
	Error       string   `json:"error"`
}

// TestActorCapabilities asserts that an ActorTemplate's
// securityContext.capabilities is actually in force inside the sandbox, as the
// kernel reports it — not merely present in the OCI spec atelet writes. atelet
// does not spawn containers, so the spec being right and the sandbox applying
// it are separate claims; only this test covers the second. It also checks
// that uid 0 cannot write /proc/sys and, on microVM, that a masked path is
// empty.
//
// It runs against whichever sandbox class E2E_SANDBOX_CLASS selects, so CI
// covers both gvisor and micro-VM from one suite.
func TestActorCapabilities(t *testing.T) {
	if _, err := e2e.CheckEnv("KO_DOCKER_REPO"); err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	namespace := deployFixture(t, ctx, clients)

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
	}, {
		// The golden bake in deployFixture is what proves port 80 binds without
		// NET_BIND_SERVICE; this row checks the restored actor still answers
		// with an empty set.
		name:     "drop ALL still binds port 80",
		template: "caps-none",
		want:     nil,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actor := tt.template + "-actor"
			createAndResumeActor(t, ctx, clients, namespace, tt.template, actor)

			rc, err := e2e.NewRouterClient(t, ctx)
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
			// Neither runtime sets no_new_privileges; gVisor runs with --allow-suid.
			if got.NoNewPrivs {
				t.Error("no_new_privs = 1, want 0")
			}

			// uid 0 must not write /proc/sys. gVisor's procfs makes this file
			// 0444. On microVM the container is root in the guest kernel, so
			// this relies on the read-only path.
			wantErr := "permission denied"
			if e2e.IsMicroVM() {
				wantErr = "read-only file system"
			}
			if got := probeFile(t, ctx, rc, namespace, actor, "/writefile", "/proc/sys/kernel/domainname"); !strings.Contains(got["error"], wantErr) {
				t.Errorf("writing /proc/sys/kernel/domainname: error %q, want %q", got["error"], wantErr)
			}

			// gVisor's procfs has no /proc/interrupts. On microVM it reads as
			// empty only if the /dev/null mask is in force.
			if e2e.IsMicroVM() {
				got := probeFile(t, ctx, rc, namespace, actor, "/readfile", "/proc/interrupts")
				if got["error"] != "" || got["content"] != "" {
					t.Errorf("reading /proc/interrupts = %q, error %q, want empty content", got["content"], got["error"])
				}
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

// deployFixture installs the fixture for the sandbox class under test and
// returns its atespace (which also names the namespace it created, carrying
// the class suffix so the gVisor and micro-VM lanes never share one). Every
// template is golden-snapshotted when this returns; a template whose
// container cannot start — for example because a needed capability was
// dropped — fails the deploy with the template's error message rather than
// timing out per subtest.
func deployFixture(t *testing.T, ctx context.Context, clients *e2e.Clients) string {
	t.Helper()
	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, clients, e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/capabilities/capabilities.yaml.tmpl",
		Template: "internal/e2e/fixtures/capabilities/capabilities-templates.yaml.tmpl",
	}, "capabilities", nil)
	return atespace
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, template, id string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: namespace, Name: id},
		ActorTemplate: &ateapi.ObjectRef{Atespace: namespace, Name: template},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		// DeleteActor requires the actor to be suspended.
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: namespace, Name: id}})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: namespace, Name: id}})
	})

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: namespace, Name: id},
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

// probeFile runs the probe's /readfile or /writefile on path and returns its
// response.
func probeFile(t *testing.T, ctx context.Context, rc *e2e.RouterClient, namespace, id, endpoint, path string) map[string]string {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: namespace, Name: id}, endpoint+"?path="+url.QueryEscape(path))
	if err != nil {
		t.Fatalf("GET %s for %q: %v", endpoint, id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s for %q: status %d, body %q", endpoint, id, resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s for %q: %v", endpoint, id, err)
	}
	return out
}

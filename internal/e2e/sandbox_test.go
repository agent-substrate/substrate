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

package e2e

import (
	"bytes"
	"maps"
	"os"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"sigs.k8s.io/yaml"
)

// substrateFixtures are every fixture DeploySubstrateFixture is asked to
// deploy: the pool half rendered by RenderFixtureManifest and the substrate
// ActorTemplate half rendered with substrateTemplateSubstitutions, plus how
// many template documents the latter must yield. values fills the template's
// own placeholders, as the suite deploying it does through WithTemplateValue.
var substrateFixtures = []struct {
	manifests SubstrateFixtureManifests
	templates int
	values    map[string]string
}{
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-template.yaml.tmpl",
	}, 1, nil},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-bootfetch-template.yaml.tmpl",
	}, 1, map[string]string{
		"${BOOT_FETCH_HOST}": "origin.example",
		"${BOOT_FETCH_URL}":  "https://origin.example/healthz",
	}},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/probe/probe-sized.yaml.tmpl",
		Template: "internal/e2e/fixtures/probe/probe-sized-template.yaml.tmpl",
	}, 1, nil},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/security/atespace-a-pool.yaml.tmpl",
		Template: "internal/e2e/fixtures/security/atespace-template.yaml.tmpl",
	}, 1, nil},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/capabilities/capabilities.yaml.tmpl",
		Template: "internal/e2e/fixtures/capabilities/capabilities-templates.yaml.tmpl",
	}, 2, nil},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/testserver/websocket.yaml.tmpl",
		Template: "internal/e2e/fixtures/testserver/websocket-template.yaml.tmpl",
	}, 1, nil},
	{SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/testserver/grpcecho.yaml.tmpl",
		Template: "internal/e2e/fixtures/testserver/grpcecho-template.yaml.tmpl",
	}, 1, nil},
}

// renderPool renders a fixture's pool manifest and strict-decodes its
// WorkerPool.
//
// Strict decoding against the real API type is the point: the runtime blocks
// are injected as pre-indented text, so a placeholder that lands at the wrong
// depth yields YAML that still parses but hangs the field off the wrong parent
// — which strict mode reports as an unknown field instead of silently applying
// a WorkerPool that never gets a micro-VM worker.
func renderPool(t *testing.T, relPath string) *atev1.WorkerPool {
	t.Helper()
	raw, err := os.ReadFile(RenderFixtureManifest(t, relPath, "test-bucket", "render"))
	if err != nil {
		t.Fatalf("reading the rendered %s: %v", relPath, err)
	}

	pool := &atev1.WorkerPool{}
	for doc := range strings.SplitSeq(string(raw), "\n---\n") {
		if strings.TrimSpace(doc) == "" {
			continue
		}
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("rendered %s is not valid YAML: %v\n%s", relPath, err, doc)
		}
		if meta.Kind != "WorkerPool" {
			continue
		}
		if err := yaml.UnmarshalStrict([]byte(doc), pool); err != nil {
			t.Fatalf("rendered %s WorkerPool does not match the API type: %v\n%s", relPath, meta.Kind, doc)
		}
	}
	if pool.Name == "" {
		t.Fatalf("rendered %s is missing a WorkerPool", relPath)
	}
	return pool
}

// renderTemplates renders a fixture's substrate template manifest and
// strict-decodes its ActorTemplate documents; the protojson decode plays the
// same misplaced-placeholder tripwire renderPool's strict mode does. A
// placeholder that values does not fill fails the test, since it would reach
// the cluster as literal text.
func renderTemplates(t *testing.T, relPath string, values map[string]string) []*ateapi.ActorTemplate {
	t.Helper()
	inline, blocks := substrateTemplateSubstitutions("test-bucket", "render", nil)
	maps.Copy(inline, values)
	rendered, err := os.ReadFile(renderManifest(t, relPath, inline, blocks))
	if err != nil {
		t.Fatalf("reading the rendered %s: %v", relPath, err)
	}
	if i := bytes.Index(rendered, []byte("${")); i >= 0 {
		end := min(len(rendered), i+40)
		t.Fatalf("rendered %s keeps a placeholder: %q", relPath, rendered[i:end])
	}
	return decodeSubstrateTemplates(t, rendered)
}

// TestRenderSubstrateFixtures_GVisor pins the default rendering: every
// micro-VM block is gone, no placeholder survives, and the templates select
// their fixture's pool.
func TestRenderSubstrateFixtures_GVisor(t *testing.T) {
	t.Setenv(sandboxClassEnv, "")
	for _, fixture := range substrateFixtures {
		t.Run(fixture.manifests.Pool, func(t *testing.T) {
			pool := renderPool(t, fixture.manifests.Pool)
			if !strings.HasSuffix(pool.Spec.WorkerImage, "/cmd/ateworker-gvisor") {
				t.Errorf("WorkerPool workerImage = %q, want the gVisor worker", pool.Spec.WorkerImage)
			}
			if pool.Spec.DefaultSandboxClass() != atev1.SandboxClassGvisor {
				t.Errorf("WorkerPool runtime = class %q, want gvisor", pool.Spec.DefaultSandboxClass())
			}

			templates := renderTemplates(t, fixture.manifests.Template, fixture.values)
			if len(templates) != fixture.templates {
				t.Fatalf("rendered %s yields %d templates, want %d", fixture.manifests.Template, len(templates), fixture.templates)
			}
			for _, tmpl := range templates {
				name := tmpl.GetMetadata().GetName()
				// An inline placeholder with an empty value must substitute, not
				// delete its line: the location is what the golden snapshot needs.
				location := tmpl.GetSnapshotConfig().GetStorageLocation()
				if want := "gs://test-bucket/"; !strings.HasPrefix(location, want) {
					t.Errorf("template %s snapshot location = %q, want it to start with %q", name, location, want)
				}
				if strings.Contains(location, "-microvm") {
					t.Errorf("template %s snapshot location = %q, want no micro-VM suffix", name, location)
				}
				// The selector is what ties the template to its fixture's pool.
				for k, v := range tmpl.GetWorkerSelector().GetMatchLabels() {
					if pool.Labels[k] != v {
						t.Errorf("template %s selects %s=%s, which the pool's labels %v do not carry", name, k, v, pool.Labels)
					}
				}
			}
		})
	}
}

// TestRenderSubstrateFixtures_MicroVM pins the micro-VM rendering: the
// templates carry limits and the snapshots land under their own prefix.
func TestRenderSubstrateFixtures_MicroVM(t *testing.T) {
	t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
	for _, fixture := range substrateFixtures {
		t.Run(fixture.manifests.Pool, func(t *testing.T) {
			pool := renderPool(t, fixture.manifests.Pool)
			if !strings.HasSuffix(pool.Spec.WorkerImage, "/cmd/ateworker-microvm") {
				t.Errorf("WorkerPool workerImage = %q, want the micro-VM worker", pool.Spec.WorkerImage)
			}
			if pool.Spec.DefaultSandboxClass() != SandboxClassMicroVM {
				t.Errorf("WorkerPool runtime = class %q, want microvm", pool.Spec.DefaultSandboxClass())
			}

			templates := renderTemplates(t, fixture.manifests.Template, fixture.values)
			if len(templates) != fixture.templates {
				t.Fatalf("rendered %s yields %d templates, want %d", fixture.manifests.Template, len(templates), fixture.templates)
			}
			for _, tmpl := range templates {
				name := tmpl.GetMetadata().GetName()
				// The API rejects a template without limits.
				if tmpl.GetResources().GetLimits().GetMemoryBytes() == 0 {
					t.Errorf("template %s declares no memory limit", name)
				}
				location := tmpl.GetSnapshotConfig().GetStorageLocation()
				if want := "-microvm-render/"; !strings.HasSuffix(location, want) {
					t.Errorf("template %s snapshot location = %q, want it to end with %q", name, location, want)
				}
			}
		})
	}
}

// TestEgressFixture covers the knob the networking suite reads: the class
// picks the fixture.
func TestEgressFixture(t *testing.T) {
	t.Run("gvisor", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, "")
		if got := EgressFixture(); got.Namespace != "ate-demo-egress" || got.Name != "egress" {
			t.Errorf("EgressFixture() = %+v, want the gVisor egress demo", got)
		}
	})
	t.Run("microvm", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		if got := EgressFixture(); got.Namespace != "ate-demo-egress-microvm" || got.Name != "egress-microvm" {
			t.Errorf("EgressFixture() = %+v, want the micro-VM egress demo", got)
		}
	})
}

// TestSubstrateCounterFixture covers the knob every counter-based suite reads:
// the class picks the fixture, and the explicit environment overrides still
// win.
func TestSubstrateCounterFixture(t *testing.T) {
	t.Run("gvisor", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, "")
		got := SubstrateCounterFixture()
		want := SubstrateFixture{
			Atespace:      "ate-demo-counter",
			Name:          "counter",
			PoolNamespace: "ate-demo-counter",
			PoolName:      "counter",
			DeployWith:    "hack/install-ate-kind.sh --deploy-demo-counter",
		}
		if got != want {
			t.Errorf("SubstrateCounterFixture() = %+v, want %+v", got, want)
		}
	})
	t.Run("microvm", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		got := SubstrateCounterFixture()
		want := SubstrateFixture{
			Atespace:      "ate-demo-counter-microvm",
			Name:          "counter-microvm",
			PoolNamespace: "ate-demo-counter-microvm",
			PoolName:      "counter-microvm",
			DeployWith:    "hack/install-ate-kind.sh --deploy-demo-counter-microvm",
		}
		if got != want {
			t.Errorf("SubstrateCounterFixture() = %+v, want %+v", got, want)
		}
	})
	t.Run("explicit override wins", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		t.Setenv("E2E_SUBSTRATE_TEMPLATE_ATESPACE", "elsewhere")
		t.Setenv("E2E_SUBSTRATE_TEMPLATE_NAME", "other")
		t.Setenv("E2E_SUBSTRATE_POOL_NAMESPACE", "pool-ns")
		t.Setenv("E2E_SUBSTRATE_POOL_NAME", "pool")
		got := SubstrateCounterFixture()
		if got.Atespace != "elsewhere" || got.Name != "other" || got.PoolNamespace != "pool-ns" || got.PoolName != "pool" {
			t.Errorf("SubstrateCounterFixture() = %+v, want the environment overrides", got)
		}
	})
}

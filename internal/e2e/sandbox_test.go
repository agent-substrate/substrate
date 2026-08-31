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
	"os"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"sigs.k8s.io/yaml"
)

// fixtureManifests are every template RenderFixtureManifest is asked to render.
var fixtureManifests = []string{
	"internal/e2e/fixtures/probe/probe.yaml.tmpl",
	"internal/e2e/fixtures/probe/probe-sized.yaml.tmpl",
	"internal/e2e/fixtures/capabilities/capabilities.yaml.tmpl",
	"internal/e2e/fixtures/testserver/websocket.yaml.tmpl",
	"internal/e2e/fixtures/testserver/grpcecho.yaml.tmpl",
}

// renderFixture renders a manifest and decodes the two resources the
// assertions below care about (the third document is a Namespace).
//
// Strict decoding against the real API types is the point: the runtime blocks
// are injected as pre-indented text, so a placeholder that lands at the wrong
// depth yields YAML that still parses but hangs the field off the wrong parent
// — which strict mode reports as an unknown field instead of silently applying
// a WorkerPool that never gets a micro-VM worker.
func renderFixture(t *testing.T, relPath string) (*v1alpha1.WorkerPool, *v1alpha1.ActorTemplate) {
	t.Helper()
	return decodeFixture(t, relPath, RenderFixtureManifest(t, relPath, "test-bucket", "render"))
}

// decodeFixture strict-decodes an already-rendered manifest, for callers that
// render a fixture some way other than RenderFixtureManifest.
func decodeFixture(t *testing.T, relPath, rendered string) (*v1alpha1.WorkerPool, *v1alpha1.ActorTemplate) {
	t.Helper()
	raw, err := os.ReadFile(rendered)
	if err != nil {
		t.Fatalf("reading the rendered %s: %v", relPath, err)
	}

	pool, template := &v1alpha1.WorkerPool{}, &v1alpha1.ActorTemplate{}
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
		var into any
		switch meta.Kind {
		case "WorkerPool":
			into = pool
		case "ActorTemplate":
			into = template
		default:
			continue
		}
		if err := yaml.UnmarshalStrict([]byte(doc), into); err != nil {
			t.Fatalf("rendered %s %s does not match the API type: %v\n%s", relPath, meta.Kind, err, doc)
		}
	}
	if pool.Name == "" || template.Name == "" {
		t.Fatalf("rendered %s is missing a WorkerPool or an ActorTemplate", relPath)
	}
	return pool, template
}

// TestRenderFixtureManifest_GVisor pins the default rendering: every micro-VM
// block is gone and no placeholder survives, so the gVisor lane keeps applying
// exactly what it applied before the templates were parameterized.
func TestRenderFixtureManifest_GVisor(t *testing.T) {
	t.Setenv(sandboxClassEnv, "")
	for _, relPath := range fixtureManifests {
		t.Run(relPath, func(t *testing.T) {
			pool, template := renderFixture(t, relPath)

			if !strings.HasSuffix(pool.Spec.WorkerImage, "/cmd/ateom-gvisor") {
				t.Errorf("WorkerPool workerImage = %q, want the gVisor ateom", pool.Spec.WorkerImage)
			}
			if pool.Spec.SandboxClass != "" || pool.Spec.SandboxConfigName != "" {
				t.Errorf("WorkerPool carries micro-VM runtime fields: class=%q config=%q",
					pool.Spec.SandboxClass, pool.Spec.SandboxConfigName)
			}

			if template.Spec.SandboxClass != "" {
				t.Errorf("ActorTemplate sandboxClass = %q, want unset for gVisor", template.Spec.SandboxClass)
			}
			// An inline placeholder with an empty value must substitute, not
			// delete its line: the location is what the golden snapshot needs.
			if want := "gs://test-bucket/"; !strings.HasPrefix(template.Spec.SnapshotsConfig.Location, want) {
				t.Errorf("ActorTemplate snapshot location = %q, want it to start with %q",
					template.Spec.SnapshotsConfig.Location, want)
			}
			if strings.HasSuffix(template.Spec.SnapshotsConfig.Location, "-microvm/") {
				t.Errorf("ActorTemplate snapshot location = %q, want no micro-VM suffix",
					template.Spec.SnapshotsConfig.Location)
			}
		})
	}
}

// TestRenderFixtureManifest_MicroVM pins the micro-VM rendering: the pool names
// the cluster-wide SandboxConfig, the template matches its class, and the
// snapshots land under their own prefix.
func TestRenderFixtureManifest_MicroVM(t *testing.T) {
	t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
	for _, relPath := range fixtureManifests {
		t.Run(relPath, func(t *testing.T) {
			pool, template := renderFixture(t, relPath)

			if !strings.HasSuffix(pool.Spec.WorkerImage, "/cmd/ateom-microvm") {
				t.Errorf("WorkerPool workerImage = %q, want the micro-VM ateom", pool.Spec.WorkerImage)
			}
			if pool.Spec.SandboxClass != SandboxClassMicroVM || pool.Spec.SandboxConfigName != "microvm" {
				t.Errorf("WorkerPool runtime = class %q / config %q, want microvm / microvm",
					pool.Spec.SandboxClass, pool.Spec.SandboxConfigName)
			}

			if template.Spec.SandboxClass != SandboxClassMicroVM {
				t.Errorf("ActorTemplate sandboxClass = %q, want %q — it must match the pool's or no worker is eligible",
					template.Spec.SandboxClass, SandboxClassMicroVM)
			}
			// Undeclared limits boot the guest at the kata config default
			// (2GiB), which does not fit beside the demo pools on one kind node.
			if template.Spec.Resources.Limits.Memory().IsZero() {
				t.Errorf("ActorTemplate declares no memory limit, so the guest would boot at the kata default: %+v", template.Spec.Resources)
			}
			if want := "-microvm-render/"; !strings.HasSuffix(template.Spec.SnapshotsConfig.Location, want) {
				t.Errorf("ActorTemplate snapshot location = %q, want it to end with %q",
					template.Spec.SnapshotsConfig.Location, want)
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
			Atespace:      "ate-demo-counter-substrate",
			Name:          "counter",
			PoolNamespace: "ate-demo-counter-substrate",
			PoolName:      "counter-substrate",
			DeployWith:    "hack/install-ate-kind.sh --deploy-demo-counter-substrate",
		}
		if got != want {
			t.Errorf("SubstrateCounterFixture() = %+v, want %+v", got, want)
		}
	})
	t.Run("microvm", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		got := SubstrateCounterFixture()
		want := SubstrateFixture{
			Atespace:      "ate-demo-counter-substrate-microvm",
			Name:          "counter-microvm",
			PoolNamespace: "ate-demo-counter-substrate-microvm",
			PoolName:      "counter-substrate-microvm",
			DeployWith:    "hack/install-ate-kind.sh --deploy-demo-counter-substrate-microvm",
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

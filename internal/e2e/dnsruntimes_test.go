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

	"go.ate.dev/substrate/pkg/api/v1alpha1"
)

func TestRenderDNSRuntimesFixture(t *testing.T) {
	const (
		fakeGlibc = "registry.local/dnsruntimes-glibc@sha256:1111111111111111111111111111111111111111111111111111111111111111"
		fakeMusl  = "registry.local/dnsruntimes-musl@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	)

	t.Run("gvisor", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, "")
		pool := renderPool(t, dnsRuntimesPoolTemplate)
		if pool.Spec.DefaultSandboxClass() != v1alpha1.SandboxClassGvisor {
			t.Errorf("WorkerPool runtime = %q, want gvisor", pool.Spec.DefaultSandboxClass())
		}

		inline, blocks := dnsRuntimesTemplateSubstitutions("test-bucket", "render", fakeGlibc, fakeMusl)
		raw, err := os.ReadFile(renderManifest(t, dnsRuntimesTemplatesTemplate, inline, blocks))
		if err != nil {
			t.Fatal(err)
		}
		templates := decodeSubstrateTemplates(t, raw)
		if len(templates) != 2 {
			t.Fatalf("got %d templates, want 2", len(templates))
		}
		if got := templates[0].GetContainers()[0].GetImage(); got != fakeGlibc {
			t.Errorf("glibc template image = %q, want %q", got, fakeGlibc)
		}
		if got := templates[1].GetContainers()[0].GetImage(); got != fakeMusl {
			t.Errorf("musl template image = %q, want %q", got, fakeMusl)
		}
		for _, tmpl := range templates {
			if got := tmpl.GetSandboxConfig().GetSandboxClass(); got != "gvisor" {
				t.Errorf("template %s sandboxClass = %q, want gvisor", tmpl.GetMetadata().GetName(), got)
			}
			for k, v := range tmpl.GetWorkerSelector().GetMatchLabels() {
				if pool.Labels[k] != v {
					t.Errorf("template %s selector %s=%s does not match pool labels %v", tmpl.GetMetadata().GetName(), k, v, pool.Labels)
				}
			}
		}
	})

	t.Run("microvm", func(t *testing.T) {
		t.Setenv(sandboxClassEnv, SandboxClassMicroVM)
		pool := renderPool(t, dnsRuntimesPoolTemplate)
		if pool.Spec.DefaultSandboxClass() != SandboxClassMicroVM {
			t.Errorf("WorkerPool runtime = %q, want microvm", pool.Spec.DefaultSandboxClass())
		}

		inline, blocks := dnsRuntimesTemplateSubstitutions("test-bucket", "render", fakeGlibc, fakeMusl)
		raw, err := os.ReadFile(renderManifest(t, dnsRuntimesTemplatesTemplate, inline, blocks))
		if err != nil {
			t.Fatal(err)
		}
		templates := decodeSubstrateTemplates(t, raw)
		if len(templates) != 2 {
			t.Fatalf("got %d templates, want 2", len(templates))
		}
		for _, tmpl := range templates {
			if got := tmpl.GetSandboxConfig().GetSandboxClass(); got != SandboxClassMicroVM {
				t.Errorf("template %s sandboxClass = %q, want microvm", tmpl.GetMetadata().GetName(), got)
			}
			if !strings.HasSuffix(tmpl.GetSnapshotConfig().GetStorageLocation(), "-microvm-render/") {
				t.Errorf("template %s storageLocation = %q, want -microvm-render/ suffix", tmpl.GetMetadata().GetName(), tmpl.GetSnapshotConfig().GetStorageLocation())
			}
		}
	})
}

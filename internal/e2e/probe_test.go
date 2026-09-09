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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestProbeTemplate_TrustBundle pins the opt-in. The bundle is derived from
// one cluster-wide Secret, so a probe suite that does not ask for the
// projection must not carry it: it would otherwise fail whenever the suite
// that owns the pool finishes and takes the bundle with it.
func TestProbeTemplate_TrustBundle(t *testing.T) {
	t.Setenv(sandboxClassEnv, "")
	for _, tc := range []struct {
		name string
		cfg  probeConfig
		want bool
	}{
		{"default", probeConfig{}, false},
		{"WithTrustBundle", probeConfig{trustBundle: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Strict decoding is what proves the fragment landed at the right
			// depth: misindented, it would fail the protojson decode or parse
			// as some other field.
			inline, blocks := substrateTemplateSubstitutions("test-bucket", "render", tc.cfg.trustBundle)
			rendered, err := os.ReadFile(renderManifest(t, probeManifests.Template, inline, blocks))
			if err != nil {
				t.Fatalf("reading rendered manifest: %v", err)
			}
			templates := decodeSubstrateTemplates(t, rendered)
			if len(templates) != 1 {
				t.Fatalf("probe template manifest yields %d documents, want 1", len(templates))
			}

			var source *ateapipb.TrustBundleDataSource
			for _, vol := range templates[0].GetVolumes() {
				for _, ds := range vol.GetSystemInfo().GetDataSources() {
					if ds.GetTrustBundle() != nil {
						source = ds.GetTrustBundle()
					}
				}
			}
			if (source != nil) != tc.want {
				t.Fatalf("trustBundle data source present = %v, want %v", source != nil, tc.want)
			}
			if source == nil {
				return
			}
			// The projected name must select the bundle atecontroller
			// publishes, or actors fail closed on a name atelet rejects.
			if !strings.HasPrefix(EgressTrustBundleObjectName, source.GetName()+":") {
				t.Errorf("trustBundle name = %q, want the bundle backing %q", source.GetName(), EgressTrustBundleObjectName)
			}
			if source.GetPath() == "" {
				t.Error("trustBundle projection has no path")
			}
		})
	}
}

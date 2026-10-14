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
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestProbeTemplate_TrustBundle checks that the probe template projects
// exactly the trust bundles a suite asks for, into one file.
func TestProbeTemplate_TrustBundle(t *testing.T) {
	t.Setenv(sandboxClassEnv, "")
	for _, tc := range []struct {
		name string
		opts []ProbeOption
		want []string
	}{
		{"default", nil, nil},
		{"WithTrustBundle", []ProbeOption{WithTrustBundle()}, []string{"egress-mitm.ate.dev"}},
		{"WithSystemRoots", []ProbeOption{WithSystemRoots()}, []string{"system-roots.ate.dev"}},
		{"both", []ProbeOption{WithTrustBundle(), WithSystemRoots()}, []string{"egress-mitm.ate.dev", "system-roots.ate.dev"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg probeConfig
			for _, opt := range tc.opts {
				opt(&cfg)
			}
			// Strict decoding is what proves the fragment landed at the right
			// depth: misindented, it would fail the protojson decode or parse
			// as some other field.
			inline, blocks := substrateTemplateSubstitutions("test-bucket", "render", cfg.trustBundles)
			rendered, err := os.ReadFile(renderManifest(t, probeManifests.Template, inline, blocks))
			if err != nil {
				t.Fatalf("reading rendered manifest: %v", err)
			}
			templates := decodeSubstrateTemplates(t, rendered)
			if len(templates) != 1 {
				t.Fatalf("probe template manifest yields %d documents, want 1", len(templates))
			}

			var sources []*ateapipb.TrustBundleDataSource
			for _, vol := range templates[0].GetVolumes() {
				for _, ds := range vol.GetSystemInfo().GetDataSources() {
					if ds.GetTrustBundle() != nil {
						sources = append(sources, ds.GetTrustBundle())
					}
				}
			}
			if tc.want == nil {
				if len(sources) != 0 {
					t.Fatalf("trustBundle data sources = %v, want none", sources)
				}
				return
			}
			if len(sources) != 1 {
				t.Fatalf("got %d trustBundle data sources, want one projecting every name into one file", len(sources))
			}
			if got := sources[0].GetNames(); !slices.Equal(got, tc.want) {
				t.Errorf("trustBundle names = %q, want %q", got, tc.want)
			}
			if sources[0].GetPath() == "" {
				t.Error("trustBundle projection has no path")
			}
		})
	}
	// The egress name must select the bundle atecontroller publishes, or
	// actors fail closed on a name atelet rejects.
	if !strings.HasPrefix(EgressTrustBundleObjectName, egressTrustBundleName+":") {
		t.Errorf("egress trust bundle name %q does not select %q", egressTrustBundleName, EgressTrustBundleObjectName)
	}
}

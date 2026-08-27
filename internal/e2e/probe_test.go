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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// TestRenderProbeManifest_TrustBundle pins the opt-in. The bundle is derived
// from one cluster-wide Secret, so a probe suite that does not ask for the
// projection must not carry it: it would otherwise fail whenever the suite
// that owns the pool finishes and takes the bundle with it.
func TestRenderProbeManifest_TrustBundle(t *testing.T) {
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
			// depth: misindented, it would parse as some other field.
			_, template := decodeFixture(t, probeManifest,
				renderProbeManifest(t, "test-bucket", "render", tc.cfg))

			var source *v1alpha1.TrustBundleDataSource
			for _, vol := range template.Spec.Volumes {
				if vol.SystemInfo == nil {
					continue
				}
				for _, ds := range vol.SystemInfo.DataSources {
					if ds.TrustBundle != nil {
						source = ds.TrustBundle
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
			if !strings.HasPrefix(EgressTrustBundleObjectName, source.Name+":") {
				t.Errorf("trustBundle name = %q, want the bundle backing %q", source.Name, EgressTrustBundleObjectName)
			}
			if source.Path == "" {
				t.Error("trustBundle projection has no path")
			}
		})
	}
}

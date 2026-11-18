// Copyright 2026 The Agent Substrate Authors
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

package counter

import (
	"os"
	"strings"
	"testing"

	"go.ate.dev/substrate/cmd/ate-setup/internal/demos"
	"go.ate.dev/substrate/cmd/ate-setup/internal/demos/demotest"
	"go.ate.dev/substrate/internal/resources"
)

// TestExternalVolumeRenders covers the substitution branch of the counter
// template, where the external-volume placeholders carry multi-line values
// instead of being dropped, and checks the result still parses strictly as
// the ActorTemplate it is created as. The drop branch is covered by the
// sweep test in the demos package.
func TestExternalVolumeRenders(t *testing.T) {
	for _, tc := range []struct {
		name                string
		externalVolumeClass string
		wantEVC             string
	}{
		{
			name:                "unset",
			externalVolumeClass: "",
			wantEVC:             "name: csi-nfs",
		},
		{
			name:                "explicit",
			externalVolumeClass: "csi-hostpath",
			wantEVC:             "name: csi-hostpath",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The external volume class is a setting, so steer it the way an
			// operator would rather than by reaching into the demo. Unset, not
			// blanked, for the case that wants the default: an exported empty
			// value is a class name the operator asked for.
			if tc.externalVolumeClass == "" {
				t.Setenv("EXTERNAL_VOLUME_CLASS", "x")
				_ = os.Unsetenv("EXTERNAL_VOLUME_CLASS")
			} else {
				t.Setenv("EXTERNAL_VOLUME_CLASS", tc.externalVolumeClass)
			}
			e := demotest.Env(t)
			d := &demo{}

			manifest, err := demos.Render(e, template, d.externalVolumeValues(e), nil)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			demotest.AssertRenderedActorTemplate(t, manifest, resources.ActorTemplateRef{Atespace: namespace, Name: "counter"})

			for _, want := range []string{
				"--validate-existing-file-path=/external-data/test.txt",
				"mountPath: /external-data",
				"external:",
				"template:",
				"externalVolumeClass:",
				tc.wantEVC,
			} {
				if !strings.Contains(string(manifest), want) {
					t.Errorf("rendered manifest is missing %q", want)
				}
			}
		})
	}
}

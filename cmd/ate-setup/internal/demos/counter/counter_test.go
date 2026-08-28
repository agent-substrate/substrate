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

package counter

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/demotest"
)

// TestExternalVolumeRenders covers the substitution branch of the counter
// template, where the external-volume placeholders carry multi-line values
// instead of being dropped. The drop branch is covered by the sweep test in the
// demos package.
func TestExternalVolumeRenders(t *testing.T) {
	e := demotest.Env(t)
	d := &demo{}

	manifest, err := demos.Render(e, template, d.externalVolumeValues(e), nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	demotest.AssertRendered(t, manifest)

	for _, want := range []string{
		"--validate-existing-file-path=/external-data/test.txt",
		"mountPath: /external-data",
		"externalVolumeTemplate:",
		"storageClassName: standard",
	} {
		if !strings.Contains(string(manifest), want) {
			t.Errorf("rendered manifest is missing %q", want)
		}
	}
}

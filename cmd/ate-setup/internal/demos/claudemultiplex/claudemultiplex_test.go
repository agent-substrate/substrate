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

package claudemultiplex

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/demotest"
)

// TestRenderManifests covers this demo's manifests, which the shared render
// test skips because the agent templates carry deploy-time placeholders
// (WORKLOAD_IMAGE, ANTHROPIC_API_KEY). The pool manifest must render with no
// extra values at all — Delete relies on that on a cluster with no
// credentials in the environment.
func TestRenderManifests(t *testing.T) {
	e := demotest.Env(t)

	pool, err := demos.Render(e, poolManifest, nil, nil)
	if err != nil {
		t.Fatalf("Render(%s): %v", poolManifest, err)
	}
	demotest.AssertRendered(t, pool)

	values := map[string]string{
		"ANTHROPIC_API_KEY": "test-api-key",
		// A digest-shaped stand-in for the buildx-pushed workload image.
		"WORKLOAD_IMAGE": "registry.example.dev/claude-multiplex-demo-workload@sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	for _, tmpl := range agentTemplates() {
		manifest, err := demos.Render(e, tmpl.Manifest, values, nil)
		if err != nil {
			t.Fatalf("Render(%s): %v", tmpl.Manifest, err)
		}
		demotest.AssertRenderedActorTemplate(t, manifest, tmpl.Ref)
	}
}

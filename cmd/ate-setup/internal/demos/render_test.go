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

// The test lives in demos_test rather than demos so that it can import the
// demo packages, which import this one.
package demos_test

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos"
	_ "github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/all"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/demos/demotest"
)

// TestDemoTemplatesRender guards against drift between the demo templates and
// the placeholder sets in this package: a new ${PLACEHOLDER} in a template that
// nothing substitutes or drops would otherwise only show up as an apply-time
// YAML error against a real cluster.
//
// Substrate-shaped demos are checked manifest by manifest: the pool manifest
// must decode as a Kubernetes stream, and each protojson ActorTemplate
// manifest must strictly parse into the message it is created as.
func TestDemoTemplatesRender(t *testing.T) {
	e := demotest.Env(t)

	render := func(t *testing.T, relPath string) []byte {
		t.Helper()
		manifest, err := demos.Render(e, relPath, nil, demos.ExternalVolumePlaceholders)
		if err != nil {
			t.Fatalf("Render(%s): %v", relPath, err)
		}
		return manifest
	}

	covered := 0
	for _, demo := range demos.All() {
		switch d := demo.(type) {
		case interface{ SubstrateDemo() *demos.Substrate }:
			covered++
			t.Run(demo.Name(), func(t *testing.T) {
				s := d.SubstrateDemo()
				demotest.AssertRendered(t, render(t, s.WorkerPoolManifest))
				for _, tmpl := range s.Templates {
					demotest.AssertRenderedActorTemplate(t, render(t, tmpl.Manifest), tmpl.Ref)
				}
			})
		case interface{ TemplatePath() string }:
			covered++
			t.Run(demo.Name(), func(t *testing.T) {
				demotest.AssertRendered(t, render(t, d.TemplatePath()))
			})
		default:
			// demo-claude-code-multiplex has its own placeholders, and is
			// covered by its own package's test.
		}
	}
	if want := len(demos.All()) - 1; covered != want {
		t.Errorf("covered %d demo templates, want %d", covered, want)
	}
}

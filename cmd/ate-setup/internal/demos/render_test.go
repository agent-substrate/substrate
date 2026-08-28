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
func TestDemoTemplatesRender(t *testing.T) {
	e := demotest.Env(t)

	covered := 0
	for _, demo := range demos.All() {
		templated, ok := demo.(interface{ TemplatePath() string })
		if !ok {
			// demo-claude-code-multiplex has its own placeholders, and is
			// covered by its own package's test.
			continue
		}
		covered++
		t.Run(demo.Name(), func(t *testing.T) {
			manifest, err := demos.Render(e, templated.TemplatePath(), nil, demos.ExternalVolumePlaceholders)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			demotest.AssertRendered(t, manifest)
		})
	}
	if want := len(demos.All()) - 1; covered != want {
		t.Errorf("covered %d demo templates, want %d", covered, want)
	}
}

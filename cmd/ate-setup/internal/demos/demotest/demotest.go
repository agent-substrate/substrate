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

// Package demotest provides the fixtures the demo packages share in their
// tests: an Env that needs no cluster, and the assertion that a rendered
// template is a complete manifest.
package demotest

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
	"github.com/agent-substrate/substrate/internal/resources"
)

// Env builds an Env with no cluster connection. The render paths only read
// configuration, so they can be exercised without a kubeconfig.
func Env(t *testing.T) *steps.Env {
	t.Helper()
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	return &steps.Env{Cfg: &config.Config{Root: root, BucketName: "ate-snapshots"}}
}

// AssertRendered checks that every placeholder in a rendered template was
// resolved and that the result is still parseable as a Kubernetes manifest
// stream.
func AssertRendered(t *testing.T, manifest []byte) {
	t.Helper()

	assertNoPlaceholders(t, manifest)
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		t.Fatalf("DecodeManifestBytes: %v", err)
	}
	if len(objs) == 0 {
		t.Error("rendered manifest decoded to no objects")
	}
}

// AssertRenderedActorTemplate checks that a rendered protojson ActorTemplate
// manifest has every placeholder resolved and strictly parses into the
// ateapipb.ActorTemplate it will be created as, and that it names the given
// (atespace, name). ko:// image references are left as written: they are
// plain strings to the proto, resolved only at deploy time.
func AssertRenderedActorTemplate(t *testing.T, manifest []byte, want resources.ActorTemplateRef) {
	t.Helper()

	assertNoPlaceholders(t, manifest)
	template, err := steps.ActorTemplateFromManifest(manifest)
	if err != nil {
		t.Fatalf("ActorTemplateFromManifest: %v", err)
	}
	if got := resources.ActorTemplateRefFromActorTemplate(template); got != want {
		t.Errorf("manifest names template %s, want %s", got, want)
	}
}

// assertNoPlaceholders checks that no non-comment line still contains a
// ${PLACEHOLDER}.
func assertNoPlaceholders(t *testing.T, manifest []byte) {
	t.Helper()
	for i, line := range strings.Split(string(manifest), "\n") {
		// Comments are left as written; some of them mention placeholders as
		// documentation rather than as substitution points.
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.Contains(line, "${") {
			t.Errorf("line %d still contains a placeholder: %s", i+1, line)
		}
	}
}

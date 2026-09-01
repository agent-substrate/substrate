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

package images_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// testSource is the repo/tag every rewrite test resolves against.
var testSource = images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}

func TestPrebuiltResolveBytes(t *testing.T) {
	tests := []struct {
		name  string
		src   images.Source
		in    string
		want  string
		error string
	}{
		{
			name: "bare scalar",
			in:   "        image: ko://github.com/agent-substrate/substrate/cmd/ateapi\n",
			want: "        image: example.com/substrate/ateapi:v1.2.3\n",
		},
		{
			name: "double quoted",
			in:   `image: "ko://github.com/agent-substrate/substrate/cmd/atelet"`,
			want: `image: "example.com/substrate/atelet:v1.2.3"`,
		},
		{
			name: "single quoted",
			in:   `image: 'ko://github.com/agent-substrate/substrate/cmd/atenet'`,
			want: `image: 'example.com/substrate/atenet:v1.2.3'`,
		},
		{
			name: "flow sequence",
			in:   `images: [ko://github.com/agent-substrate/substrate/demos/counter, other]`,
			want: `images: [example.com/substrate/counter:v1.2.3, other]`,
		},
		{
			// The reference is a plain CRD field here, not a pod spec, which is
			// why the rewrite cannot be driven off a Kubernetes schema.
			name: "CRD field",
			in:   "spec:\n  workerImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor\n",
			want: "spec:\n  workerImage: example.com/substrate/ateom-gvisor:v1.2.3\n",
		},
		{
			name: "nested package maps to its image name",
			in:   "image: ko://github.com/agent-substrate/substrate/demos/multi-template/fspersist\n",
			want: "image: example.com/substrate/fspersist:v1.2.3\n",
		},
		{
			name: "two references on one line",
			in:   "a: ko://github.com/agent-substrate/substrate/cmd/ateapi, b: ko://github.com/agent-substrate/substrate/demos/egress\n",
			want: "a: example.com/substrate/ateapi:v1.2.3, b: example.com/substrate/egress:v1.2.3\n",
		},
		{
			// The demo templates carry "# ko:// image reference" as prose. The
			// regex requires a character after the prefix so those survive.
			name: "prose mention is left alone",
			in:   "        # ko:// image reference for the workload\n",
			want: "        # ko:// image reference for the workload\n",
		},
		{
			name: "nothing to rewrite",
			in:   "apiVersion: v1\nkind: ConfigMap\n",
			want: "apiVersion: v1\nkind: ConfigMap\n",
		},
		{
			// benchmarking/workloads/manifests templates the sandbox class into
			// the import path. It has to fail rather than resolve to something.
			//
			// The reported reference stops at the "}", which ends a YAML flow
			// mapping and so terminates the match. That costs a character in the
			// message and nothing else: the truncation cannot collide with a
			// listed package, because every one of those ends before the "$".
			name:  "unexpanded placeholder",
			in:    "image: ko://github.com/agent-substrate/substrate/cmd/ateom-${SANDBOX_CLASS}\n",
			error: "cmd/ateom-${SANDBOX_CLASS has no published image",
		},
		{
			// A reference extending a known one must not inherit its image. A
			// literal replacement would leave "-sidecar" dangling on the tag.
			name:  "reference extending a known one",
			in:    "image: ko://github.com/agent-substrate/substrate/cmd/atelet-sidecar\n",
			error: "cmd/atelet-sidecar has no published image",
		},
		{
			// ko accepts a "./"-relative import path in a manifest as readily as
			// a full one, and would publish this as "atelet". Nothing in the tree
			// spells a reference this way, so it is rejected rather than mapped:
			// only the packages named in full can be known to have been published.
			name:  "relative import path",
			in:    "image: ko://./cmd/atelet\n",
			error: "ko://./cmd/atelet has no published image",
		},
		{
			// ko would map this onto its own "atelet" image, because it names
			// images by the last path element alone. Matching the whole reference
			// is what keeps another module's package out of this repo's registry
			// path.
			name:  "another module ending in a known name",
			in:    "image: ko://github.com/example/other/cmd/atelet\n",
			error: "ko://github.com/example/other/cmd/atelet has no published image",
		},
		{
			name:  "outside the module",
			in:    "image: ko://github.com/example/other/cmd/thing\n",
			error: "ko://github.com/example/other/cmd/thing has no published image",
		},
		{
			name:  "unknown component",
			in:    "image: ko://github.com/agent-substrate/substrate/cmd/notacomponent\n",
			error: "cmd/notacomponent has no published image",
		},
		{
			// The e2e fixtures are not part of an install, so a manifest
			// reaching the installer with one is a mistake worth reporting.
			name:  "e2e fixture is not installable",
			in:    "image: ko://github.com/agent-substrate/substrate/internal/e2e/fixtures/probe\n",
			error: "e2e/fixtures/probe has no published image",
		},
		{
			// Every unmappable reference is named, not just the first.
			name:  "all failures are reported",
			in:    "a: ko://github.com/agent-substrate/substrate/cmd/one\nb: ko://github.com/agent-substrate/substrate/cmd/two\n",
			error: "cmd/two has no published image",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.src
			if src.Repo == "" {
				src = testSource
			}
			got, err := images.NewPrebuilt(src).ResolveBytes(context.Background(), []byte(tc.in))

			if tc.error != "" {
				if err == nil {
					t.Fatalf("ResolveBytes() = %q, want an error containing %q", got, tc.error)
				}
				if !strings.Contains(err.Error(), tc.error) {
					t.Errorf("ResolveBytes() error = %v, want it to contain %q", err, tc.error)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveBytes() error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("ResolveBytes() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// Every unmappable reference is reported at once, so a manifest with several
// problems takes one install attempt to diagnose rather than several.
func TestPrebuiltReportsEveryFailure(t *testing.T) {
	in := "a: ko://github.com/agent-substrate/substrate/cmd/ateom-${SANDBOX_CLASS}\n" +
		"b: ko://github.com/example/other/cmd/thing\n" +
		"c: ko://github.com/agent-substrate/substrate/cmd/nope\n"

	_, err := images.NewPrebuilt(testSource).ResolveBytes(context.Background(), []byte(in))
	if err == nil {
		t.Fatal("ResolveBytes() = nil error, want failures for all three references")
	}
	for _, want := range []string{"SANDBOX_CLASS", "github.com/example/other", "cmd/nope"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

// A repeated bad reference is one problem, not one per occurrence.
func TestPrebuiltDeduplicatesFailures(t *testing.T) {
	ref := "image: ko://github.com/agent-substrate/substrate/cmd/nope\n"
	_, err := images.NewPrebuilt(testSource).ResolveBytes(context.Background(), []byte(ref+ref+ref))
	if err == nil {
		t.Fatal("ResolveBytes() = nil error, want a failure")
	}
	if n := strings.Count(err.Error(), "cmd/nope has no published image"); n != 1 {
		t.Errorf("reported the same reference %d times, want 1:\n%v", n, err)
	}
}

// Resolving the control plane manifests must produce a manifest that still
// parses and holds no unresolved reference. This is the closest a unit test
// gets to the install itself.
func TestResolveEveryInstallManifest(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	installDir := filepath.Join(root, "manifests", "ate-install")

	entries, err := os.ReadDir(installDir)
	if err != nil {
		t.Fatalf("listing %s: %v", installDir, err)
	}

	resolver := images.NewPrebuilt(testSource)

	// The directory as a whole is what a plain GKE install applies.
	paths := []string{installDir}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".yaml" {
			paths = append(paths, filepath.Join(installDir, entry.Name()))
		}
	}

	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			out, err := resolver.ResolvePath(context.Background(), p)
			if err != nil {
				t.Fatalf("ResolvePath() = %v", err)
			}
			if refs := koRefPattern.FindAllString(string(out), -1); len(refs) > 0 {
				t.Errorf("unresolved references survived: %v", refs)
			}
			if _, err := kube.DecodeManifestBytes(out); err != nil {
				t.Errorf("the resolved manifest no longer parses: %v", err)
			}
		})
	}
}

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
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

// testSource is the repo/tag every rewrite test resolves against.
var testSource = images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}

// What a resolved reference carries beyond its tag. Fixed rather than derived
// from the reference so the expected output below reads as a literal.
const (
	testDigest   = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	digestSuffix = "@" + testDigest
)

// stubRegistry stands in for the registry the installer would query: it answers
// every lookup with testDigest, or with err, and records what was asked for.
type stubRegistry struct {
	calls map[string]int
	err   error
}

func newStubRegistry() *stubRegistry { return &stubRegistry{calls: make(map[string]int)} }

// digest is the [images.Digester] a test passes to [images.NewPrebuilt].
func (s *stubRegistry) digest(_ context.Context, ref string) (string, error) {
	s.calls[ref]++
	if s.err != nil {
		return "", fmt.Errorf("resolving %s to a digest: %w", ref, s.err)
	}
	return testDigest, nil
}

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
			want: "        image: example.com/substrate/ateapi:v1.2.3" + digestSuffix + "\n",
		},
		{
			name: "double quoted",
			in:   `image: "ko://github.com/agent-substrate/substrate/cmd/atelet"`,
			want: `image: "example.com/substrate/atelet:v1.2.3` + digestSuffix + `"`,
		},
		{
			name: "single quoted",
			in:   `image: 'ko://github.com/agent-substrate/substrate/cmd/atenet'`,
			want: `image: 'example.com/substrate/atenet:v1.2.3` + digestSuffix + `'`,
		},
		{
			name: "flow sequence",
			in:   `images: [ko://github.com/agent-substrate/substrate/demos/counter, other]`,
			want: `images: [example.com/substrate/counter:v1.2.3` + digestSuffix + `, other]`,
		},
		{
			// The reference is a plain CRD field here, not a pod spec, which is
			// why the rewrite cannot be driven off a Kubernetes schema.
			name: "CRD field",
			in:   "spec:\n  workerImage: ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor\n",
			want: "spec:\n  workerImage: example.com/substrate/ateom-gvisor:v1.2.3" + digestSuffix + "\n",
		},
		{
			name: "nested package maps to its image name",
			in:   "image: ko://github.com/agent-substrate/substrate/demos/multi-template/fspersist\n",
			want: "image: example.com/substrate/fspersist:v1.2.3" + digestSuffix + "\n",
		},
		{
			name: "two references on one line",
			in:   "a: ko://github.com/agent-substrate/substrate/cmd/ateapi, b: ko://github.com/agent-substrate/substrate/demos/egress\n",
			want: "a: example.com/substrate/ateapi:v1.2.3" + digestSuffix + ", b: example.com/substrate/egress:v1.2.3" + digestSuffix + "\n",
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
			got, err := images.NewPrebuilt(src, newStubRegistry().digest).
				ResolveBytes(context.Background(), []byte(tc.in))

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

	_, err := images.NewPrebuilt(testSource, newStubRegistry().digest).
		ResolveBytes(context.Background(), []byte(in))
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
	_, err := images.NewPrebuilt(testSource, newStubRegistry().digest).
		ResolveBytes(context.Background(), []byte(ref+ref+ref))
	if err == nil {
		t.Fatal("ResolveBytes() = nil error, want a failure")
	}
	if n := strings.Count(err.Error(), "cmd/nope has no published image"); n != 1 {
		t.Errorf("reported the same reference %d times, want 1:\n%v", n, err)
	}
}

// A tag is resolved once per image, however many manifests name it, and the
// answer is reused across calls: an install applies several manifests through
// one resolver, and a registry round trip each time would be waste.
func TestPrebuiltLooksUpEachImageOnce(t *testing.T) {
	registry := newStubRegistry()
	resolver := images.NewPrebuilt(testSource, registry.digest)

	in := "a: ko://github.com/agent-substrate/substrate/cmd/ateapi\n" +
		"b: ko://github.com/agent-substrate/substrate/cmd/atelet\n" +
		"c: ko://github.com/agent-substrate/substrate/cmd/ateapi\n"
	for range 2 {
		if _, err := resolver.ResolveBytes(context.Background(), []byte(in)); err != nil {
			t.Fatalf("ResolveBytes() error = %v", err)
		}
	}

	want := map[string]int{
		"example.com/substrate/ateapi:v1.2.3": 1,
		"example.com/substrate/atelet:v1.2.3": 1,
	}
	if !maps.Equal(registry.calls, want) {
		t.Errorf("registry lookups = %v, want %v", registry.calls, want)
	}
}

// A registry that cannot answer fails the install, and says so for every image
// it was asked about. Falling back to the bare tag would produce manifests that
// admission rejects, and a partially pinned manifest would be worse still.
func TestPrebuiltReportsDigestFailures(t *testing.T) {
	registry := newStubRegistry()
	registry.err = errors.New("UNAUTHORIZED")

	in := "a: ko://github.com/agent-substrate/substrate/cmd/ateapi\n" +
		"b: ko://github.com/agent-substrate/substrate/cmd/atelet\n"
	got, err := images.NewPrebuilt(testSource, registry.digest).
		ResolveBytes(context.Background(), []byte(in))
	if err == nil {
		t.Fatalf("ResolveBytes() = %q, want an error", got)
	}
	for _, want := range []string{
		"resolving example.com/substrate/ateapi:v1.2.3 to a digest: UNAUTHORIZED",
		"resolving example.com/substrate/atelet:v1.2.3 to a digest: UNAUTHORIZED",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

// resolvedRef matches a rewritten reference in testSource's repository.
var resolvedRef = regexp.MustCompile(regexp.QuoteMeta(testSource.Repo) + `/[^\s"',\]}]+`)

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

	resolver := images.NewPrebuilt(testSource, newStubRegistry().digest)

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
			// Every reference has to name a digest. ActorTemplate images,
			// image volume references, and a SandboxConfig's pauseImage all
			// carry the CEL rule self.contains('@'), so an unpinned reference
			// is not applied at all, it is rejected by admission.
			for _, ref := range resolvedRef.FindAllString(string(out), -1) {
				if !strings.Contains(ref, "@sha256:") {
					t.Errorf("%s is not pinned to a digest", ref)
				}
			}
			if _, err := kube.DecodeManifestBytes(out); err != nil {
				t.Errorf("the resolved manifest no longer parses: %v", err)
			}
		})
	}
}

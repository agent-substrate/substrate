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

package images

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// An allowlist entry that is not a component publishes nothing and hides that
// the component it meant was renamed or removed.
func TestReleasedAreComponents(t *testing.T) {
	seen := make(map[string]bool, len(released))
	for _, pkg := range released {
		if !slices.Contains(Components, pkg) {
			t.Errorf("released names %s, which is not in Components", pkg)
		}
		if seen[pkg] {
			t.Errorf("released lists %s twice", pkg)
		}
		seen[pkg] = true
	}
}

// Only a release tag that makes a valid reference passes. A comma would be
// split by ko into two tags, and a stray tag would publish a full image set to
// the release repository.
func TestCheckReleaseTag(t *testing.T) {
	for _, tag := range []string{"v1.2.3", "v0.5.0-rc", "v0.5.0-rc.1", "v0.5.0-pre", "v0.5.0-pre.4", "v10.0.0-alpha.2-x"} {
		if err := CheckReleaseTag("ghcr.io/agent-substrate/substrate", tag); err != nil {
			t.Errorf("CheckReleaseTag(%q) = %v, want nil", tag, err)
		}
	}
	for _, tag := range []string{"v1-test", "vnext", "v1.2.3,latest", "1.2.3", "v1.2", "v1.2.3-", "v1.2.3foo", ""} {
		if err := CheckReleaseTag("ghcr.io/agent-substrate/substrate", tag); err == nil {
			t.Errorf("CheckReleaseTag(%q) = nil, want an error", tag)
		}
	}
	if err := CheckReleaseTag("not a repo", "v1.2.3"); err == nil {
		t.Error("CheckReleaseTag() with an invalid repository = nil, want an error")
	}
}

// A tag that names an earlier push passes a platform check, so the digest is
// compared too.
func TestStaleTags(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	repo := strings.TrimPrefix(srv.URL, "http://") + "/substrate"

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	arm64 := v1.Platform{OS: "linux", Architecture: "arm64"}
	pushIndex(t, repo+"/current:v1", amd64)
	pushIndex(t, repo+"/stale:v1", amd64)
	current := indexDigest(t, repo+"/current:v1")
	// What the run pushed for "stale": a different index, never tagged.
	pushed := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
		Add: empty.Image, Descriptor: v1.Descriptor{Platform: &arm64},
	})
	pushedDigest, err := pushed.Digest()
	if err != nil {
		t.Fatal(err)
	}

	got, err := StaleTags(t.Context(),
		[]string{repo + "/current:v1", repo + "/stale:v1", repo + "/missing:v1"},
		[]string{repo + "/current:v1@" + current, repo + "/stale@" + pushedDigest.String()},
	)
	if err != nil {
		t.Fatalf("StaleTags() error = %v", err)
	}
	if len(got) != 2 || !strings.Contains(got[0], "stale:v1 names") || !strings.Contains(got[1], "missing:v1: nothing was pushed") {
		t.Errorf("StaleTags() = %q, want the stale and the missing tag", got)
	}
}

// indexDigest is the digest ref's tag names in the test registry.
func indexDigest(t *testing.T, ref string) string {
	t.Helper()
	d, err := RemoteDigest(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func parseRef(t *testing.T, ref string) name.Reference {
	t.Helper()
	parsed, err := name.ParseReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// pushIndex pushes an index to ref with one empty image per platform.
func pushIndex(t *testing.T, ref string, platforms ...v1.Platform) {
	t.Helper()
	var index v1.ImageIndex = empty.Index
	for _, p := range platforms {
		index = mutate.AppendManifests(index, mutate.IndexAddendum{
			Add:        empty.Image,
			Descriptor: v1.Descriptor{Platform: &p},
		})
	}
	if err := remote.WriteIndex(parseRef(t, ref), index); err != nil {
		t.Fatal(err)
	}
}

// The check decides whether a release is rebuilt, so each way an image can
// fall short has to be reported, and a complete image must not be.
func TestCheckPublished(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	repo := strings.TrimPrefix(srv.URL, "http://") + "/substrate"

	amd64 := v1.Platform{OS: "linux", Architecture: "amd64"}
	arm64v8 := v1.Platform{OS: "linux", Architecture: "arm64", Variant: "v8"}
	pushIndex(t, repo+"/complete:v1", amd64, arm64v8)
	pushIndex(t, repo+"/amd64-only:v1", amd64)
	if err := remote.Write(parseRef(t, repo+"/single:v1"), empty.Image); err != nil {
		t.Fatal(err)
	}
	// ko pushes a plain image, not an index, when a build targets one platform.
	config, err := empty.Image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config.OS, config.Architecture = "linux", "amd64"
	plain, err := mutate.ConfigFile(empty.Image, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(parseRef(t, repo+"/plain-amd64:v1"), plain); err != nil {
		t.Fatal(err)
	}

	refs := []string{
		repo + "/complete:v1",
		repo + "/amd64-only:v1",
		repo + "/single:v1",
		repo + "/plain-amd64:v1",
		repo + "/complete:v2",
		repo + "/absent:v1",
	}
	got, err := CheckPublished(t.Context(), refs, []string{"linux/amd64", "linux/arm64"})
	if err != nil {
		t.Fatalf("CheckPublished() error = %v", err)
	}
	want := []Incomplete{
		{Ref: repo + "/amd64-only:v1", Missing: []string{"linux/arm64"}},
		{Ref: repo + "/single:v1", Missing: []string{"linux/amd64", "linux/arm64"}},
		{Ref: repo + "/plain-amd64:v1", Missing: []string{"linux/arm64"}},
		{Ref: repo + "/complete:v2", Missing: []string{"linux/amd64", "linux/arm64"}},
		{Ref: repo + "/absent:v1", Missing: []string{"linux/amd64", "linux/arm64"}},
	}
	if !slices.EqualFunc(got, want, func(a, b Incomplete) bool {
		return a.Ref == b.Ref && slices.Equal(a.Missing, b.Missing)
	}) {
		t.Errorf("CheckPublished() = %v, want %v", got, want)
	}

	// A single-platform release is complete as a plain image.
	got, err = CheckPublished(t.Context(), []string{repo + "/plain-amd64:v1"}, []string{"linux/amd64"})
	if err != nil || len(got) != 0 {
		t.Errorf("CheckPublished() for one platform = %v, %v; want complete", got, err)
	}
}

// A registry that cannot answer says nothing about whether an image exists.
// Reporting it as missing would get published images rebuilt and replaced.
func TestCheckPublishedFailsWhenTheRegistryCannotAnswer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	ref := strings.TrimPrefix(srv.URL, "http://") + "/substrate/ateapi:v1"

	if got, err := CheckPublished(t.Context(), []string{ref}, []string{"linux/amd64"}); err == nil {
		t.Errorf("CheckPublished() = %v, nil; want an error", got)
	}
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/ko"
)

// TestKoRunnerPrebuilt covers the one resolver that cannot build. A pre-built
// install has no ko runner behind imageResolver, so a step that needs to build
// has to say so rather than dereference what is not there.
func TestKoRunnerPrebuilt(t *testing.T) {
	e := &Env{Cfg: &config.Config{
		Root:   t.TempDir(),
		Images: images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"},
	}}

	runner, err := e.koRunner()
	if err == nil {
		t.Fatalf("koRunner() = %v, nil; want an error under --image-repo", runner)
	}
	// The message has to name the flag: it is the thing the caller passed and
	// the thing they have to drop.
	if !strings.Contains(err.Error(), "--image-repo") {
		t.Errorf("koRunner() error = %q; want it to name --image-repo", err)
	}
}

// Without a registry ko would fall back to its own default, so the release
// command refuses before building anything.
func TestPublishReleaseImagesNeedsARegistry(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: t.TempDir()}}

	err := e.PublishReleaseImages(t.Context(), io.Discard, PublishReleaseOptions{})
	if err == nil || !strings.Contains(err.Error(), "KO_DOCKER_REPO") {
		t.Errorf("PublishReleaseImages() error = %v, want one naming KO_DOCKER_REPO", err)
	}
}

// Rerunning a release must not replace images that are already published, so
// a complete set is left alone. The Root has no ko tool, so reaching the build
// would fail this test.
func TestPublishReleaseImagesSkipsACompleteRelease(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	repo := strings.TrimPrefix(srv.URL, "http://") + "/substrate"
	t.Setenv("VERSION", "v1.2.3")

	index := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: empty.Image, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
		mutate.IndexAddendum{Add: empty.Image, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
	)
	for _, ref := range releaseImageRefs(repo, "v1.2.3") {
		parsed, err := name.ParseReference(ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := remote.WriteIndex(parsed, index); err != nil {
			t.Fatal(err)
		}
	}

	e := &Env{Cfg: &config.Config{
		Root:               t.TempDir(),
		KODockerRepo:       repo,
		KODefaultPlatforms: "linux/amd64,linux/arm64",
	}}
	// A digests file from an earlier run must not survive a run that built
	// nothing: the release workflow would attest its contents as this run's.
	digestsFile := filepath.Join(t.TempDir(), "release-images.txt")
	if err := os.WriteFile(digestsFile, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := e.PublishReleaseImages(t.Context(), io.Discard, PublishReleaseOptions{DigestsFile: digestsFile}); err != nil {
		t.Fatalf("PublishReleaseImages() error = %v, want nil with every image published", err)
	}
	if _, err := os.Stat(digestsFile); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("os.Stat(digests file) error = %v, want it removed when nothing was built", err)
	}

	// Listing writes the digest each tag names, in the form the release
	// workflow verifies attestations from.
	if err := e.PublishReleaseImages(t.Context(), io.Discard, PublishReleaseOptions{ListPublished: true, DigestsFile: digestsFile}); err != nil {
		t.Fatalf("PublishReleaseImages(ListPublished) error = %v", err)
	}
	got, err := os.ReadFile(digestsFile)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := index.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, ref := range releaseImageRefs(repo, "v1.2.3") {
		want = append(want, digest.Hex+"  "+strings.TrimSuffix(ref, ":v1.2.3"))
	}
	slices.Sort(want)
	if string(got) != strings.Join(want, "\n")+"\n" {
		t.Errorf("digests file:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}

// The digests file lists each image once, by repository and sha256, sorted,
// from either reference form ko prints.
func TestWriteDigestsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bin", "release-images.txt")
	refs := []string{
		"example.com/substrate/atelet:v1@sha256:" + strings.Repeat("b", 64),
		"example.com/substrate/ateapi@sha256:" + strings.Repeat("a", 64),
	}
	if err := writeDigestsFile(path, refs); err != nil {
		t.Fatalf("writeDigestsFile() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("a", 64) + "  example.com/substrate/ateapi\n" + strings.Repeat("b", 64) + "  example.com/substrate/atelet\n"
	if string(got) != want {
		t.Errorf("digests file = %q, want %q", got, want)
	}
	if err := writeDigestsFile(path, []string{"example.com/substrate/ateapi:v1"}); err == nil {
		t.Error("writeDigestsFile() = nil for a reference without a digest, want an error")
	}
}

// A stray tag or a value ko would split publishes nothing, with --overwrite as
// well as without: the check runs before the registry is read or ko is
// located, and the Root has no ko tool.
func TestPublishReleaseImagesRejectsANonReleaseTag(t *testing.T) {
	for _, version := range []string{"v1-test", "v1.2.3,latest", "dev"} {
		t.Run(version, func(t *testing.T) {
			t.Setenv("VERSION", version)
			e := &Env{Cfg: &config.Config{Root: t.TempDir(), KODockerRepo: "example.com/substrate"}}
			err := e.PublishReleaseImages(t.Context(), io.Discard, PublishReleaseOptions{Overwrite: true})
			if err == nil || !strings.Contains(err.Error(), "not a release tag") {
				t.Errorf("PublishReleaseImages() error = %v, want one saying %q is not a release tag", err, version)
			}
		})
	}
}

// The release covers every released component and envoy-dataplane, each under
// the name and tag that `deploy --image-repo --image-tag` looks up, and nothing
// releases leave out.
func TestReleaseImageRefs(t *testing.T) {
	refs := releaseImageRefs("example.com/substrate/", "v1")
	if want := len(images.ReleaseComponents()) + len(images.ReleaseDockerfileImages()); len(refs) != want {
		t.Fatalf("releaseImageRefs() returned %d refs, want %d", len(refs), want)
	}
	for _, want := range []string{
		"example.com/substrate/ateapi:v1",
		"example.com/substrate/envoy-dataplane:v1",
		"example.com/substrate/kubernetes-secrets:v1",
		"example.com/substrate/objectstore-plugin:v1",
	} {
		if !slices.Contains(refs, want) {
			t.Errorf("releaseImageRefs() = %v, want %s", refs, want)
		}
	}
	for _, unwanted := range []string{
		"example.com/substrate/counter:v1",
	} {
		if slices.Contains(refs, unwanted) {
			t.Errorf("releaseImageRefs() = %v, want no %s", refs, unwanted)
		}
	}
}

// The release names each binary for its platform, and publishes no Windows
// build.
func TestReleaseBinaryNames(t *testing.T) {
	var got []string
	for _, platform := range releaseBinaryPlatforms() {
		got = append(got, releaseBinaryName(platform))
	}
	want := []string{"kubectl-ate-darwin-amd64", "kubectl-ate-darwin-arm64", "kubectl-ate-linux-amd64", "kubectl-ate-linux-arm64"}
	if !slices.Equal(got, want) {
		t.Errorf("release binaries = %v, want %v", got, want)
	}
}

// The checksum file names the binary without its directory, in the form
// `shasum -a 256 -c` reads, so the check works wherever both are downloaded.
func TestWriteChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubectl-ate-linux-amd64")
	if err := os.WriteFile(path, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	sum, err := writeChecksum(path)
	if err != nil {
		t.Fatalf("writeChecksum() error = %v", err)
	}
	if sum != path+".sha256" {
		t.Errorf("writeChecksum() = %q, want %q", sum, path+".sha256")
	}
	got, err := os.ReadFile(sum)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("binary"))
	if want := hex.EncodeToString(digest[:]) + "  kubectl-ate-linux-amd64\n"; string(got) != want {
		t.Errorf("checksum file = %q, want %q", got, want)
	}
}

// A binary for a version that is not a release tag is never built.
func TestPublishReleaseBinariesRejectsANonReleaseTag(t *testing.T) {
	t.Setenv("VERSION", "dev")
	e := &Env{Cfg: &config.Config{Root: t.TempDir()}}
	err := e.PublishReleaseBinaries(t.Context(), io.Discard, t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "not a release tag") {
		t.Errorf("PublishReleaseBinaries() error = %v, want one saying dev is not a release tag", err)
	}
}

// A release binary reports the release version and the commit it was built
// from. internal/version discards a stamped Version without a stamped Commit,
// which is what this guards. Only the host platform is built, to keep the test
// to one compile.
func TestPublishReleaseBinaries(t *testing.T) {
	root := repoRoot(t)
	commit, _, ok := ko.CommitStamp(root)
	if !ok {
		t.Fatalf("%s has no commit checked out", root)
	}
	platform := runtime.GOOS + "/" + runtime.GOARCH
	t.Setenv("VERSION", "v1.2.3")

	out := t.TempDir()
	e := &Env{Cfg: &config.Config{Root: root}}
	if err := e.publishReleaseBinaries(t.Context(), io.Discard, out, []string{platform}); err != nil {
		t.Fatalf("publishReleaseBinaries() error = %v", err)
	}

	binary := filepath.Join(out, releaseBinaryName(platform))
	if _, err := os.Stat(binary + ".sha256"); err != nil {
		t.Errorf("no checksum beside the binary: %v", err)
	}
	version, err := exec.Command(binary, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("%s --version: %v\n%s", binary, err, version)
	}
	if want := "version v1.2.3 commit=" + commit; !strings.Contains(string(version), want) {
		t.Errorf("%s --version = %q, want it to contain %q", binary, version, want)
	}
}

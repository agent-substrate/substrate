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
	"io"
	"log"
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

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
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
	if err := e.PublishReleaseImages(t.Context(), io.Discard, PublishReleaseOptions{}); err != nil {
		t.Errorf("PublishReleaseImages() error = %v, want nil with every image published", err)
	}
}

// The release covers every component and envoy-dataplane, each under the name
// and tag that `deploy --image-repo --image-tag` looks up.
func TestReleaseImageRefs(t *testing.T) {
	refs := releaseImageRefs("example.com/substrate/", "v1")
	if len(refs) != len(images.Components)+1 {
		t.Fatalf("releaseImageRefs() returned %d refs, want %d", len(refs), len(images.Components)+1)
	}
	for _, want := range []string{"example.com/substrate/ateapi:v1", "example.com/substrate/envoy-dataplane:v1"} {
		if !slices.Contains(refs, want) {
			t.Errorf("releaseImageRefs() = %v, want %s", refs, want)
		}
	}
}

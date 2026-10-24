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

package cmd

import (
	"io"
	stdlog "log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

// The root command's hook prints the configuration report to stdout, so the
// digests have to arrive through the file. This runs the command tree.
func TestPublishReleaseImagesListPublished(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(stdlog.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	repo := strings.TrimPrefix(srv.URL, "http://") + "/substrate"
	index := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: empty.Image, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}})
	digest, err := index.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, pkg := range images.ReleaseComponents() {
		names = append(names, images.ImageName(pkg))
	}
	for _, img := range images.ReleaseDockerfileImages() {
		names = append(names, img.Name)
	}
	var want []string
	for _, n := range names {
		image := repo + "/" + n
		ref, err := name.ParseReference(image + ":v1.2.3")
		if err != nil {
			t.Fatal(err)
		}
		if err := remote.WriteIndex(ref, index); err != nil {
			t.Fatal(err)
		}
		want = append(want, digest.Hex+"  "+image)
	}
	slices.Sort(want)

	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(os.Stdout) })
	t.Setenv("VERSION", "v1.2.3")
	digestsFile := filepath.Join(t.TempDir(), "release-images.txt")
	if err := runRoot(t, "publish", "release-images", "--list-published", "--digests-file", digestsFile, "--ko-docker-repo", repo, "--no-dev-env"); err != nil {
		t.Fatalf("publish release-images --list-published: %v", err)
	}
	got, err := os.ReadFile(digestsFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != strings.Join(want, "\n")+"\n" {
		t.Errorf("digests file:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
}

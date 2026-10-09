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

	refs := []string{
		repo + "/complete:v1",
		repo + "/amd64-only:v1",
		repo + "/single:v1",
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
		{Ref: repo + "/complete:v2", Missing: []string{"linux/amd64", "linux/arm64"}},
		{Ref: repo + "/absent:v1", Missing: []string{"linux/amd64", "linux/arm64"}},
	}
	if !slices.EqualFunc(got, want, func(a, b Incomplete) bool {
		return a.Ref == b.Ref && slices.Equal(a.Missing, b.Missing)
	}) {
		t.Errorf("CheckPublished() = %v, want %v", got, want)
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

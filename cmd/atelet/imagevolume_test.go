//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"archive/tar"
	"bytes"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// imageVolumeTestRegistry starts an in-memory OCI registry. Its 127.0.0.1 host
// makes the image cache treat it as a local registry and pull over plain HTTP.
func imageVolumeTestRegistry(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return u.Host
}

func singleFileLayer(t *testing.T, path, body string) v1.Layer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o755, Size: int64(len(body))}); err != nil {
		t.Fatalf("tar.WriteHeader: %v", err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatalf("tar.Write: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar.Close: %v", err)
	}
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
	})
	if err != nil {
		t.Fatalf("tarball.LayerFromOpener: %v", err)
	}
	return l
}

func pushTestImage(t *testing.T, ref string, layers ...v1.Layer) {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, layers...)
	if err != nil {
		t.Fatalf("mutate.AppendLayers: %v", err)
	}
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatalf("name.ParseReference(%q): %v", ref, err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatalf("remote.Write(%q): %v", ref, err)
	}
}

func newImageVolumeStore(t *testing.T) *imagecache.Store {
	t.Helper()
	s, err := imagecache.New(t.TempDir())
	if err != nil {
		t.Fatalf("imagecache.New: %v", err)
	}
	return s
}

// A mounted image volume records its layers for ateom to compose, and its
// digest so the cache GC can protect them.
func TestResolveImageVolumes_RecordsLayersAndDigest(t *testing.T) {
	host := imageVolumeTestRegistry(t)
	ref := host + "/agent:v1"
	pushTestImage(t, ref, singleFileLayer(t, "payload-binary", "binary"))

	volumes := []*ateletpb.Volume{{
		Name:   "agent",
		Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: ref}},
	}}
	mounts := []*ateletpb.VolumeMount{{Name: "agent", MountPath: "/ate"}}

	got, err := resolveImageVolumes(t.Context(), newImageVolumeStore(t), volumes, mounts)
	if err != nil {
		t.Fatalf("resolveImageVolumes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "agent" {
		t.Fatalf("resolveImageVolumes = %+v, want one entry named %q", got, "agent")
	}
	if len(got[0].Layers) != 1 {
		t.Errorf("layers = %v, want 1", got[0].Layers)
	}
	if !strings.HasPrefix(got[0].ImageDigest, "sha256:") {
		t.Errorf("image digest = %q, want a sha256 digest", got[0].ImageDigest)
	}
	// The returned path is a layer directory; the binary lives under its fs/ subtree.
	if _, err := os.Stat(filepath.Join(got[0].Layers[0], "fs", "payload-binary")); err != nil {
		t.Errorf("recorded path is not a layer directory: %v", err)
	}
}

// Multi-layer image volumes produce one entry with layers in bottom-most-first order.
func TestResolveImageVolumes_MultiLayer(t *testing.T) {
	host := imageVolumeTestRegistry(t)
	ref := host + "/agent:multi"
	pushTestImage(t, ref,
		singleFileLayer(t, "base", "one"),
		singleFileLayer(t, "payload-binary", "binary"),
	)

	volumes := []*ateletpb.Volume{{
		Name:   "agent",
		Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: ref}},
	}}
	mounts := []*ateletpb.VolumeMount{{Name: "agent", MountPath: "/ate"}}

	got, err := resolveImageVolumes(t.Context(), newImageVolumeStore(t), volumes, mounts)
	if err != nil {
		t.Fatalf("resolveImageVolumes: %v", err)
	}
	if len(got) != 1 || len(got[0].Layers) != 2 {
		t.Fatalf("resolveImageVolumes = %+v, want one entry with 2 layers", got)
	}
	for i, want := range []string{"base", "payload-binary"} {
		if _, err := os.Stat(filepath.Join(got[0].Layers[i], "fs", want)); err != nil {
			t.Errorf("layer %d does not hold %q: %v", i, want, err)
		}
	}
}

// An image volume no container mounts is never pulled, so a bad reference on an
// unused volume cannot fail the actor.
func TestResolveImageVolumes_UnmountedVolumeNotPulled(t *testing.T) {
	volumes := []*ateletpb.Volume{{
		Name:   "agent",
		Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{Reference: "127.0.0.1:1/nope@sha256:abc"}},
	}}

	got, err := resolveImageVolumes(t.Context(), newImageVolumeStore(t), volumes, nil)
	if err != nil {
		t.Fatalf("resolveImageVolumes: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("resolveImageVolumes = %+v, want empty", got)
	}
}

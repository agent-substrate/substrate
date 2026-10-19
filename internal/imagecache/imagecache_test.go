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

package imagecache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	erofs "github.com/erofs/go-erofs"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// newTestRegistry starts an in-memory OCI registry and returns its host
// ("127.0.0.1:<port>", which the store treats as a local registry and pulls
// over plain HTTP).
func newTestRegistry(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	return newWrappedRegistry(t, func(h http.Handler) http.Handler { return h })
}

// newWrappedRegistry starts an in-memory OCI registry behind wrap.
func newWrappedRegistry(t *testing.T, wrap func(http.Handler) http.Handler) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(wrap(registry.New(registry.Logger(log.New(io.Discard, "", 0)))))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return srv, u.Host
}

func layerFromEntries(t *testing.T, entries []tarEntry) v1.Layer {
	t.Helper()
	b := buildTar(t, entries)
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	})
	if err != nil {
		t.Fatalf("tarball.LayerFromOpener: %v", err)
	}
	return l
}

func pushImage(t *testing.T, ref string, cfg v1.Config, layers ...v1.Layer) v1.Image {
	t.Helper()
	img, err := mutate.ConfigFile(empty.Image, &v1.ConfigFile{Config: cfg})
	if err != nil {
		t.Fatalf("mutate.ConfigFile: %v", err)
	}
	img, err = mutate.AppendLayers(img, layers...)
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
	return img
}

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	s, err := New(t.TempDir(), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestRetryBackoffFor pins the per-registry split: well-known communal
// registries get the patient fleet-throttling backoff, everything else —
// registries the deployment has its own quota with, including local ones —
// retries fast (their pulls sit on the Run/Restore critical path).
func TestRetryBackoffFor(t *testing.T) {
	for registry, want := range map[string]remote.Backoff{
		"gcr.io":                              dedicatedRegistryBackoff,
		"us.gcr.io":                           dedicatedRegistryBackoff,
		"us-docker.pkg.dev":                   dedicatedRegistryBackoff,
		"123.dkr.ecr.us-east-1.amazonaws.com": dedicatedRegistryBackoff,
		"harbor.example.com":                  dedicatedRegistryBackoff,
		"127.0.0.1:5000":                      dedicatedRegistryBackoff,
		"registry.k8s.io":                     sharedRegistryBackoff,
		"docker.io":                           sharedRegistryBackoff,
		"index.docker.io":                     sharedRegistryBackoff,
		"quay.io":                             sharedRegistryBackoff,
		"ghcr.io":                             sharedRegistryBackoff,
	} {
		if got := retryBackoffFor(registry); got != want {
			t.Errorf("retryBackoffFor(%q) = %+v, want %+v", registry, got, want)
		}
	}
}

// TestEnsureImage_RetriesRateLimit proves a pull survives transient 429s.
// go-containerregistry's retry transport, configured with the per-registry
// backoff picked by retryBackoffFor, covers every request including the /v2/
// auth ping — the first request a throttling registry rejects (as
// registry.k8s.io does per source IP).
func TestEnsureImage_RetriesRateLimit(t *testing.T) {
	origBackoff := dedicatedRegistryBackoff
	dedicatedRegistryBackoff = remote.Backoff{Duration: time.Millisecond, Factor: 2.0, Jitter: 0.1, Steps: 4}
	t.Cleanup(func() { dedicatedRegistryBackoff = origBackoff })

	// throttle > 0 makes the registry reject the next request with a 429;
	// it stays at zero until the test image is pushed.
	var throttle, rejections atomic.Int32
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if throttle.Add(-1) >= 0 {
			rejections.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}

	ref := u.Host + "/test/pause:1"
	pushImage(t, ref, v1.Config{}, layerFromEntries(t, []tarEntry{
		{name: "pause", typeflag: tar.TypeReg, body: "pause"},
	}))
	throttle.Store(2)

	s := newTestStore(t)
	if _, err := s.EnsureImage(context.Background(), ref); err != nil {
		t.Fatalf("EnsureImage through a throttling registry: %v", err)
	}
	if got := rejections.Load(); got != 2 {
		t.Errorf("registry rejected %d requests, want 2 (the throttling was not exercised)", got)
	}
}

func TestEnsureImage_TagPullAndDigestHit(t *testing.T) {
	srv, host := newTestRegistry(t)
	ref := host + "/test/app:latest"

	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: "#!/sh\n"},
	})
	top := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
		{name: "bin/.wh.sh", typeflag: tar.TypeReg},
		{name: "app/.wh..wh..opq", typeflag: tar.TypeReg},
	})
	pushImage(t, ref, v1.Config{Env: []string{"FOO=bar"}}, base, top)

	store := newTestStore(t)
	ctx := context.Background()

	// Tag refs must be cacheable: they resolve to a digest via one HEAD.
	img, err := store.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage(tag): %v", err)
	}
	if len(img.LayerDirs) != 2 {
		t.Fatalf("LayerDirs = %v, want 2 entries", img.LayerDirs)
	}
	if !slices.Equal(img.Config.Env, []string{"FOO=bar"}) {
		t.Errorf("Config.Env = %v, want [FOO=bar]", img.Config.Env)
	}

	// Bottom-first order: the base layer's file is in LayerDirs[0].
	if _, err := os.Stat(filepath.Join(img.LayerDirs[0], layerFSDirName, "bin/sh")); err != nil {
		t.Errorf("base layer content missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(img.LayerDirs[1], layerFSDirName, "app/main")); err != nil {
		t.Errorf("top layer content missing: %v", err)
	}

	// The top layer's whiteout state is recorded beside its tree, not in it.
	wh, err := readWhiteouts(img.LayerDirs[1])
	if err != nil {
		t.Fatalf("readWhiteouts: %v", err)
	}
	if want := []string{"bin/sh"}; !slices.Equal(wh.Whiteouts, want) {
		t.Errorf("whiteouts = %v, want %v", wh.Whiteouts, want)
	}
	if want := []string{"app"}; !slices.Equal(wh.Opaques, want) {
		t.Errorf("opaques = %v, want %v", wh.Opaques, want)
	}
	if _, err := os.Lstat(filepath.Join(img.LayerDirs[1], layerFSDirName, "bin/.wh.sh")); err == nil {
		t.Errorf("whiteout entry written into the layer tree")
	}

	// A digest-pinned ref must now hit the cache with zero registry traffic:
	// kill the registry and pull again through a fresh store on the same root
	// (which also exercises startup recovery over a populated pool).
	srv.Close()
	store2, err := New(store.root)
	if err != nil {
		t.Fatalf("New(existing root): %v", err)
	}
	img2, err := store2.EnsureImage(ctx, host+"/test/app@"+img.Digest.String())
	if err != nil {
		t.Fatalf("EnsureImage(digest, registry down): %v", err)
	}
	if !slices.Equal(img2.LayerDirs, img.LayerDirs) {
		t.Errorf("cache hit LayerDirs = %v, want %v", img2.LayerDirs, img.LayerDirs)
	}
	if !slices.Equal(img2.Config.Env, img.Config.Env) {
		t.Errorf("cache hit Config.Env = %v, want %v", img2.Config.Env, img.Config.Env)
	}
}

// Two images sharing a base layer must share its unpacked tree: the pool
// holds three layer dirs, not four.
func TestEnsureImage_SharedLayersDeduplicated(t *testing.T) {
	_, host := newTestRegistry(t)

	shared := layerFromEntries(t, []tarEntry{
		{name: "lib/", typeflag: tar.TypeDir},
		{name: "lib/base.so", typeflag: tar.TypeReg, body: "base"},
	})
	topA := layerFromEntries(t, []tarEntry{{name: "a", typeflag: tar.TypeReg, body: "a"}})
	topB := layerFromEntries(t, []tarEntry{{name: "b", typeflag: tar.TypeReg, body: "b"}})
	pushImage(t, host+"/test/a:latest", v1.Config{}, shared, topA)
	pushImage(t, host+"/test/b:latest", v1.Config{}, shared, topB)

	store := newTestStore(t)
	ctx := context.Background()

	imgA, err := store.EnsureImage(ctx, host+"/test/a:latest")
	if err != nil {
		t.Fatalf("EnsureImage(a): %v", err)
	}
	imgB, err := store.EnsureImage(ctx, host+"/test/b:latest")
	if err != nil {
		t.Fatalf("EnsureImage(b): %v", err)
	}

	if imgA.LayerDirs[0] != imgB.LayerDirs[0] {
		t.Errorf("shared base layer not deduplicated: %q vs %q", imgA.LayerDirs[0], imgB.LayerDirs[0])
	}
	entries, err := os.ReadDir(store.layersDir())
	if err != nil {
		t.Fatalf("ReadDir(layers): %v", err)
	}
	if len(entries) != 3 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("layer pool holds %d dirs (%v), want 3 (shared base stored once)", len(entries), names)
	}
}

// Startup recovery must sweep temp dirs orphaned by a crash mid-unpack, and
// reject a cache root with an unknown layout version.
func TestNew_RecoveryAndVersioning(t *testing.T) {
	root := t.TempDir()
	s, err := New(root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	orphan := filepath.Join(s.layersDir(), ".tmp-deadbeef-123")
	if err := os.MkdirAll(orphan, 0o700); err != nil {
		t.Fatalf("planting orphan: %v", err)
	}
	recOrphan := filepath.Join(s.manifestsDir(), ".deadbeef.json.tmp-456")
	if err := os.WriteFile(recOrphan, []byte("{"), 0o600); err != nil {
		t.Fatalf("planting record orphan: %v", err)
	}
	record := filepath.Join(s.manifestsDir(), "deadbeef.json")
	if err := os.WriteFile(record, []byte("{}"), 0o600); err != nil {
		t.Fatalf("planting record: %v", err)
	}
	if _, err := New(root); err != nil {
		t.Fatalf("New(recovery): %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphaned temp dir survived recovery")
	}
	if _, err := os.Stat(recOrphan); !os.IsNotExist(err) {
		t.Errorf("orphaned manifest temp file survived recovery")
	}
	if _, err := os.Stat(record); err != nil {
		t.Errorf("completed manifest record swept by recovery: %v", err)
	}

	if err := os.WriteFile(filepath.Join(root, versionFileName), []byte("99\n"), 0o600); err != nil {
		t.Fatalf("writing version marker: %v", err)
	}
	if _, err := New(root); err == nil {
		t.Errorf("New accepted an unknown layout version")
	}
}

func TestIsLocalRegistry(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{ref: "localhost/foo", want: true},
		{ref: "localhost:5001/foo", want: true},
		{ref: "127.0.0.1/foo", want: true},
		{ref: "127.0.0.1:5001/foo", want: true},
		{ref: "127.0.0.2/foo", want: true},
		{ref: "127.0.0.2:8080/foo", want: true},
		{ref: "kind-registry/foo", want: false},
		{ref: "kind-registry:5000/foo", want: false},
		{ref: "my-registry.local/foo", want: false},
		{ref: "my-registry.local:8080/foo", want: false},
		{ref: "gcr.io/foo", want: false},
		{ref: "example.com/foo", want: false},
		{ref: "foo", want: false},
		{ref: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := isLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("isLocalRegistry(%q) = %v; want %v", tt.ref, got, tt.want)
			}
		})
	}
}

func TestRewriteLocalRegistry(t *testing.T) {
	s := &Store{localhostRegistryReplacement: "kind-registry:5000"}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "localhost/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:5001/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1:3000/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "kind-registry/foo", want: "kind-registry/foo"},
		{ref: "kind-registry:5000/foo", want: "kind-registry:5000/foo"},
		{ref: "my-registry.local/foo", want: "my-registry.local/foo"},
		{ref: "gcr.io/foo", want: "gcr.io/foo"},
		{ref: "foo", want: "foo"},
		{ref: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := s.rewriteLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("rewriteLocalRegistry(%q) = %q; want %q", tt.ref, got, tt.want)
			}
		})
	}
}

func TestEnsureImage_EROFSLayers(t *testing.T) {
	_, host := newTestRegistry(t)
	ref := host + "/test/erofs:latest"
	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: strings.Repeat("s", 9000)},
	})
	top := layerFromEntries(t, []tarEntry{
		{name: "app/", typeflag: tar.TypeDir},
		{name: "app/main", typeflag: tar.TypeReg, mode: 0o755, body: "main"},
		{name: "bin/.wh.sh", typeflag: tar.TypeReg},
	})
	pushImage(t, ref, v1.Config{}, base, top)
	ctx := context.Background()

	// Form{} writes no EROFS image and reports EROFS false.
	plain := newTestStore(t)
	img, err := plain.EnsureImage(ctx, ref)
	if err != nil {
		t.Fatalf("EnsureImage (plain): %v", err)
	}
	if img.EROFS {
		t.Error("plain store reported EROFS")
	}
	if _, err := os.Stat(LayerEROFSPath(img.LayerDirs[0])); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("plain store wrote %s: %v", layerEROFSFileName, err)
	}

	store := newTestStore(t)
	img, err = store.EnsureImageAs(ctx, ref, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs: %v", err)
	}
	if !img.EROFS {
		t.Error("EROFS = false, want true")
	}
	for i, dir := range img.LayerDirs {
		f, err := os.Open(LayerEROFSPath(dir))
		if err != nil {
			t.Fatalf("layer %d: %v", i, err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size()%erofsImageAlign != 0 {
			t.Errorf("layer %d image size %d not a multiple of %d", i, fi.Size(), erofsImageAlign)
		}
		fsys, err := erofs.Open(f)
		if err != nil {
			t.Fatalf("layer %d: erofs.Open: %v", i, err)
		}
		switch i {
		case 0:
			if got, err := fs.ReadFile(fsys, "bin/sh"); err != nil || len(got) != 9000 {
				t.Errorf("layer 0 bin/sh: len=%d err=%v", len(got), err)
			}
		case 1:
			if got, err := fs.ReadFile(fsys, "app/main"); err != nil || string(got) != "main" {
				t.Errorf("layer 1 app/main = %q, %v", got, err)
			}
			if fi, err := fs.Lstat(fsys, "bin/sh"); err != nil || fi.Mode()&fs.ModeCharDevice == 0 {
				t.Errorf("layer 1 bin/sh whiteout: %v, %v", fi, err)
			}
		}
		if d, err := LayerEROFSDigest(dir); err != nil || !strings.HasPrefix(d, "sha256:") {
			t.Errorf("layer %d digest = %q, %v", i, d, err)
		}
		// The recorded size is the image's, and no fs/ tree is unpacked.
		n, ok, err := recordedLayerSize(dir)
		if err != nil || !ok {
			t.Fatalf("layer %d size: ok=%v err=%v", i, ok, err)
		}
		if n != fi.Size() {
			t.Errorf("layer %d recorded size %d, want image size %d", i, n, fi.Size())
		}
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("layer %d fs/ should not be unpacked for an EROFS consumer: %v", i, err)
		}
	}

	// A fresh store over the same pool hits without touching the registry.
	store2, err := New(store.root)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	img2, err := store2.EnsureImageAs(ctx, host+"/test/erofs@"+img.Digest.String(), Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs (hit): %v", err)
	}
	if !img2.EROFS || !slices.Equal(img2.LayerDirs, img.LayerDirs) {
		t.Errorf("hit = %+v, want EROFS with the same layer dirs", img2)
	}

	// A Form{} pull adds fs/ beside the image in the same layer
	// directories, and whiteouts.json then lists the layer's whiteouts.
	img3, err := store2.EnsureImageAs(ctx, ref, Form{})
	if err != nil {
		t.Fatalf("EnsureImageAs(fs/) over an EROFS pool: %v", err)
	}
	if img3.EROFS || !slices.Equal(img3.LayerDirs, img.LayerDirs) {
		t.Errorf("fs/ pull = %+v, want the same layer dirs in fs/ form", img3)
	}
	for i, dir := range img.LayerDirs {
		if _, err := os.Stat(filepath.Join(dir, layerFSDirName)); err != nil {
			t.Errorf("layer %d has no fs/ tree after the fs/ pull: %v", i, err)
		}
		if _, err := os.Stat(LayerEROFSPath(dir)); err != nil {
			t.Errorf("layer %d lost its EROFS image: %v", i, err)
		}
	}
	wh, err := readWhiteouts(img.LayerDirs[1])
	if err != nil || !slices.Contains(wh.Whiteouts, "bin/sh") {
		t.Errorf("layer 1 whiteouts after the fs/ unpack = %+v, %v; want bin/sh recorded", wh, err)
	}
}

// blobCounter counts blob GETs per digest on a test registry, to prove what
// a pull did and did not download.
type blobCounter struct {
	mu   sync.Mutex
	gets map[string]int
}

func (c *blobCounter) reset() { c.mu.Lock(); c.gets = map[string]int{}; c.mu.Unlock() }
func (c *blobCounter) of(t *testing.T, l v1.Layer) int {
	t.Helper()
	d, err := l.Digest()
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets[d.String()]
}

// newBlobCountingRegistry is newTestRegistry with a blobCounter in front.
func newBlobCountingRegistry(t *testing.T) (host string, blobs *blobCounter) {
	t.Helper()
	blobs = &blobCounter{gets: map[string]int{}}
	inner := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if i := strings.LastIndex(r.URL.Path, "/blobs/"); r.Method == http.MethodGet && i >= 0 {
			blobs.mu.Lock()
			blobs.gets[r.URL.Path[i+len("/blobs/"):]]++
			blobs.mu.Unlock()
		}
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parsing registry URL: %v", err)
	}
	return u.Host, blobs
}

// A pull in the other form of a layer already in the pool builds that form
// beside it: one blob download, no second layer directory.
func TestEnsureImageAs_AddsMissingFormBeside(t *testing.T) {
	host, blobs := newBlobCountingRegistry(t)
	base := layerFromEntries(t, []tarEntry{
		{name: "bin/", typeflag: tar.TypeDir},
		{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: "sh"},
	})
	onlyB := layerFromEntries(t, []tarEntry{{name: "b", typeflag: tar.TypeReg, body: "b"}})
	refA, refB := host+"/test/a:latest", host+"/test/b:latest"
	pushImage(t, refA, v1.Config{}, base, layerFromEntries(t, []tarEntry{{name: "a", typeflag: tar.TypeReg, body: "a"}}))
	pushImage(t, refB, v1.Config{}, base, onlyB)
	store := newTestStore(t)
	ctx := context.Background()
	imgA, err := store.EnsureImageAs(ctx, refA, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs(EROFS): %v", err)
	}
	blobs.reset()
	imgB, err := store.EnsureImage(ctx, refB)
	if err != nil {
		t.Fatalf("EnsureImage(fs/) over an EROFS base layer: %v", err)
	}
	if imgB.EROFS || imgB.LayerDirs[0] != imgA.LayerDirs[0] {
		t.Fatalf("fs/ pull = %+v, want fs/ form sharing base layer dir %q", imgB, imgA.LayerDirs[0])
	}
	// The shared base was downloaded once more for its fs/ tree, b's own
	// layer once.
	if got, gotB := blobs.of(t, base), blobs.of(t, onlyB); got != 1 || gotB != 1 {
		t.Errorf("downloads for the fs/ pull: base %d, b %d; want 1 and 1", got, gotB)
	}
	for _, p := range []string{LayerEROFSPath(imgA.LayerDirs[0]), filepath.Join(imgA.LayerDirs[0], layerFSDirName)} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("base layer missing %s after both pulls: %v", p, err)
		}
	}
	// And the other way round: b's own layer is fs/-only until an EROFS
	// consumer asks; the base already has both.
	blobs.reset()
	imgB2, err := store.EnsureImageAs(ctx, refB, Form{EROFS: true})
	if err != nil {
		t.Fatalf("EnsureImageAs(EROFS) over an fs/ pool: %v", err)
	}
	if !imgB2.EROFS || blobs.of(t, base) != 0 || blobs.of(t, onlyB) != 1 {
		t.Errorf("EROFS pull over fs/ = %+v with base %d, b %d downloads; want EROFS form, 0 and 1", imgB2, blobs.of(t, base), blobs.of(t, onlyB))
	}
}

// Two consumers pulling the same layer in different forms at the same time
// both get theirs: the second joins or follows the first's flight and then
// runs its own for the missing form.
func TestEnsureImageAs_ConcurrentForms(t *testing.T) {
	_, host := newTestRegistry(t)
	base := layerFromEntries(t, []tarEntry{{name: "bin/sh", typeflag: tar.TypeReg, mode: 0o755, body: strings.Repeat("s", 20000)}})
	refA, refB := host+"/test/a:latest", host+"/test/b:latest"
	pushImage(t, refA, v1.Config{}, base, layerFromEntries(t, []tarEntry{{name: "a", typeflag: tar.TypeReg, body: "a"}}))
	pushImage(t, refB, v1.Config{}, base, layerFromEntries(t, []tarEntry{{name: "b", typeflag: tar.TypeReg, body: "b"}}))
	store := newTestStore(t)
	ctx := context.Background()
	for range 5 {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		imgs := make([]*Image, 2)
		for i, pull := range []struct {
			ref  string
			form Form
		}{{refA, Form{EROFS: true}}, {refB, Form{}}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				imgs[i], errs[i] = store.EnsureImageAs(ctx, pull.ref, pull.form)
			}()
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("pull %d: %v", i, err)
			}
		}
		if !imgs[0].EROFS || imgs[1].EROFS || imgs[0].LayerDirs[0] != imgs[1].LayerDirs[0] {
			t.Fatalf("pulls = %+v, %+v; want EROFS and fs/ forms sharing the base layer dir", imgs[0], imgs[1])
		}
		dir := imgs[0].LayerDirs[0]
		if !layerComplete(dir, true) || !layerComplete(dir, false) {
			t.Fatalf("base layer dir %q lacks a form after concurrent pulls", dir)
		}
		// Fresh pool each round so the race is real every time.
		var err error
		if store, err = New(t.TempDir()); err != nil {
			t.Fatal(err)
		}
	}
}

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
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// bufferedResponse buffers a response so a test registry can alter it.
type bufferedResponse struct {
	h    http.Header
	code int
	buf  bytes.Buffer
}

func (r *bufferedResponse) Header() http.Header         { return r.h }
func (r *bufferedResponse) WriteHeader(code int)        { r.code = code }
func (r *bufferedResponse) Write(p []byte) (int, error) { return r.buf.Write(p) }

// newTamperingRegistry serves blobs as pushed, except that GETs of the blob
// whose hex is in *tamper get byte 4 flipped. That is the gzip header MTIME,
// which no gzip check covers, so the blob still decompresses to the same
// content and only the compressed digest check can notice.
func newTamperingRegistry(t *testing.T, tamper *string) string {
	t.Helper()
	return newWrappedRegistry(t, func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || *tamper == "" || !strings.HasSuffix(r.URL.Path, "/blobs/sha256:"+*tamper) {
				inner.ServeHTTP(w, r)
				return
			}
			rec := &bufferedResponse{h: http.Header{}, code: http.StatusOK}
			inner.ServeHTTP(rec, r)
			b := rec.buf.Bytes()
			b[4] ^= 0xff
			for k, v := range rec.h {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.code)
			w.Write(b)
		})
	})
}

// pushLyingImage pushes an image whose only layer is content but whose
// config claims claimed's diffID.
func pushLyingImage(t *testing.T, ref string, content, claimed v1.Layer) {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, content)
	if err != nil {
		t.Fatal(err)
	}
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cf = cf.DeepCopy()
	d, err := claimed.DiffID()
	if err != nil {
		t.Fatal(err)
	}
	cf.RootFS.DiffIDs = []v1.Hash{d}
	if img, err = mutate.ConfigFile(img, cf); err != nil {
		t.Fatal(err)
	}
	tag, err := name.ParseReference(ref, name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatal(err)
	}
}

func libLayers(t *testing.T) (good, evil v1.Layer) {
	t.Helper()
	good = layerFromEntries(t, []tarEntry{{name: "lib", typeflag: tar.TypeReg, mode: 0o644, body: "good"}})
	evil = layerFromEntries(t, []tarEntry{{name: "lib", typeflag: tar.TypeReg, mode: 0o644, body: "EVIL"}})
	return good, evil
}

func readLib(t *testing.T, layerDir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(layerDir, layerFSDirName, "lib"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// assertPoolEmpty fails on any entry in the layer pool. Unlike
// layerDirsOnDisk it counts .tmp-* dirs too: a rejected unpack must leave
// nothing behind.
func assertPoolEmpty(t *testing.T, s *Store) {
	t.Helper()
	entries, err := os.ReadDir(s.layersDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("unexpected entry in layer pool: %s", e.Name())
	}
}

// A blob whose bytes differ from the manifest digest is rejected, even
// though it decompresses to the claimed diffID.
func TestPullRejectsBlobNotMatchingManifestDigest(t *testing.T) {
	var tamper string
	host := newTamperingRegistry(t, &tamper)
	good, _ := libLayers(t)
	ref := host + "/test/tampered:latest"
	pushImage(t, ref, v1.Config{}, good)
	d, err := good.Digest()
	if err != nil {
		t.Fatal(err)
	}
	tamper = d.Hex

	s := newTestStore(t)
	_, err = s.EnsureImage(context.Background(), ref)
	if err == nil || !strings.Contains(err.Error(), "error verifying sha256 checksum") {
		t.Fatalf("EnsureImage error = %v, want a compressed digest mismatch", err)
	}
	assertPoolEmpty(t, s)
}

// An image claiming another layer's diffID cannot plant its content under
// that diffID for a later image to reuse.
func TestPullRejectsContentNotMatchingClaimedDiffID(t *testing.T) {
	_, host := newTestRegistry(t)
	good, evil := libLayers(t)
	evilRef, goodRef := host+"/test/evil:latest", host+"/test/good:latest"
	pushLyingImage(t, evilRef, evil, good)
	pushImage(t, goodRef, v1.Config{}, good)

	s := newTestStore(t)
	if _, err := s.EnsureImage(context.Background(), evilRef); !errors.Is(err, errDiffIDMismatch) {
		t.Fatalf("lying pull error = %v, want errDiffIDMismatch", err)
	}
	assertPoolEmpty(t, s)

	img, err := s.EnsureImage(context.Background(), goodRef)
	if err != nil {
		t.Fatal(err)
	}
	if got := readLib(t, img.LayerDirs[0]); got != "good" {
		t.Errorf("good image's lib = %q, want %q", got, "good")
	}
}

// A content mismatch is the image's, so it is not re-downloaded once per
// candidate credential.
func TestDiffIDMismatchNotRetriedPerCredential(t *testing.T) {
	good, evil := libLayers(t)
	evilBlob, err := evil.Digest()
	if err != nil {
		t.Fatal(err)
	}
	var gets atomic.Int32
	host := newWrappedRegistry(t, func(inner http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/"+evilBlob.String()) {
				gets.Add(1)
			}
			inner.ServeHTTP(w, r)
		})
	})
	ref := host + "/test/evil:latest"
	pushLyingImage(t, ref, evil, good)

	kc := &candidateKeychain{auths: []authn.Authenticator{
		authn.FromConfig(authn.AuthConfig{Username: "a", Password: "a"}),
		authn.FromConfig(authn.AuthConfig{Username: "b", Password: "b"}),
	}}
	s := newTestStore(t, WithKeychain(kc))
	if _, err := s.EnsureImage(context.Background(), ref); !errors.Is(err, errDiffIDMismatch) {
		t.Fatalf("EnsureImage error = %v, want errDiffIDMismatch", err)
	}
	if n := gets.Load(); n != 1 {
		t.Errorf("lying blob downloaded %d times, want 1", n)
	}
}

// gatedLayer holds a layer flight open: its uncompressed stream closes
// started, blocks until release is closed, then fails with err if set.
type gatedLayer struct {
	v1.Layer
	started, release chan struct{}
	err              error
}

func newGatedLayer(l v1.Layer, err error) gatedLayer {
	return gatedLayer{Layer: l, started: make(chan struct{}), release: make(chan struct{}), err: err}
}

func (g gatedLayer) Uncompressed() (io.ReadCloser, error) {
	close(g.started)
	<-g.release
	if g.err != nil {
		return nil, g.err
	}
	return g.Layer.Uncompressed()
}

// openCountingLayer counts opens of its uncompressed stream.
type openCountingLayer struct {
	v1.Layer
	opens *atomic.Int32
}

func (c openCountingLayer) Uncompressed() (io.ReadCloser, error) {
	c.opens.Add(1)
	return c.Layer.Uncompressed()
}

// joinFlight runs ensureLayer for leader, then for joiner once the leader's
// flight is open, and releases the leader only after the joiner has joined
// its flight. Call inside a synctest bubble.
func joinFlight(t *testing.T, s *Store, diffID v1.Hash, leader gatedLayer, joiner v1.Layer) (leaderErr error, joinerDir string, joinerErr error) {
	t.Helper()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := s.ensureLayer(t.Context(), diffID, leader)
		leaderDone <- err
	}()
	<-leader.started

	joinerDone := make(chan struct{})
	go func() {
		joinerDir, joinerErr = s.ensureLayer(t.Context(), diffID, joiner)
		close(joinerDone)
	}()
	// Every goroutine is now blocked: the leader on release, the joiner
	// waiting on the leader's flight.
	synctest.Wait()
	close(leader.release)
	leaderErr = <-leaderDone
	<-joinerDone
	return leaderErr, joinerDir, joinerErr
}

// A lying pull holds its own flight, so an honest pull of the same diffID
// completes without waiting on it or inheriting its error.
func TestLyingPullDoesNotFailConcurrentHonestPull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestStore(t)
		good, evil := libLayers(t)
		diffID, err := good.DiffID()
		if err != nil {
			t.Fatal(err)
		}
		lying := newGatedLayer(evil, nil)
		lyingDone := make(chan error, 1)
		go func() {
			_, err := s.ensureLayer(t.Context(), diffID, lying)
			lyingDone <- err
		}()
		<-lying.started

		// The lying flight is still held open.
		dir, err := s.ensureLayer(t.Context(), diffID, good)
		if err != nil {
			t.Fatalf("honest ensureLayer: %v", err)
		}
		if got := readLib(t, dir); got != "good" {
			t.Errorf("honest layer's lib = %q, want %q", got, "good")
		}

		close(lying.release)
		if err := <-lyingDone; !errors.Is(err, errDiffIDMismatch) {
			t.Fatalf("lying ensureLayer error = %v, want errDiffIDMismatch", err)
		}
		if got := readLib(t, dir); got != "good" {
			t.Errorf("after the lying pull, lib = %q, want %q", got, "good")
		}
	})
}

// Pulls of the same blob still share one flight, and its result.
func TestSameBlobPullsShareOneFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newTestStore(t)
		good, _ := libLayers(t)
		diffID, err := good.DiffID()
		if err != nil {
			t.Fatal(err)
		}
		outage := errors.New("registry unavailable")
		var opens atomic.Int32
		_, _, err = joinFlight(t, s, diffID, newGatedLayer(good, outage), openCountingLayer{Layer: good, opens: &opens})
		if !errors.Is(err, outage) {
			t.Errorf("joiner error = %v, want the leader's %v", err, outage)
		}
		if n := opens.Load(); n != 0 {
			t.Errorf("joiner opened its own layer %d times, want 0", n)
		}
	})
}

// The hash covers the whole uncompressed stream, while the returned length
// covers only what the consumer read.
func TestReadVerifiedLayerDrainsStream(t *testing.T) {
	good, evil := libLayers(t)
	diffID, err := good.DiffID()
	if err != nil {
		t.Fatal(err)
	}
	readTen := func(r io.Reader) error {
		_, err := io.CopyN(io.Discard, r, 10)
		return err
	}
	n, err := readVerifiedLayer(good, diffID, readTen)
	if err != nil {
		t.Fatalf("readVerifiedLayer(good): %v", err)
	}
	if n != 10 {
		t.Errorf("length = %d, want 10", n)
	}
	if _, err := readVerifiedLayer(evil, diffID, readTen); !errors.Is(err, errDiffIDMismatch) {
		t.Errorf("readVerifiedLayer(evil) error = %v, want errDiffIDMismatch", err)
	}
}

// Bytes after the tar end marker are hashed but never land on disk, so they
// must not count toward the size GC accounts for.
func TestRecordedSizeExcludesTrailingBytes(t *testing.T) {
	tarBytes := buildTar(t, []tarEntry{{name: "lib", typeflag: tar.TypeReg, mode: 0o644, body: "good"}})
	padded := append(bytes.Clone(tarBytes), make([]byte, 1<<20)...)
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(padded)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	diffID, err := layer.DiffID()
	if err != nil {
		t.Fatal(err)
	}
	s := newTestStore(t)
	dir, err := s.ensureLayer(context.Background(), diffID, layer)
	if err != nil {
		t.Fatalf("ensureLayer: %v", err)
	}
	size, ok, err := recordedLayerSize(dir)
	if err != nil || !ok {
		t.Fatalf("recordedLayerSize = %d, %v, %v", size, ok, err)
	}
	if size <= 0 || size > int64(len(tarBytes)) {
		t.Errorf("recorded size = %d, want in (0, %d]", size, len(tarBytes))
	}
}

// A pool written before layers were verified is refused rather than reused.
func TestNewRefusesUnverifiedLayoutV1(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, versionFileName), []byte("1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err == nil || !strings.Contains(err.Error(), `layout version "1"`) {
		t.Fatalf("New error = %v, want a layout version refusal", err)
	}
}

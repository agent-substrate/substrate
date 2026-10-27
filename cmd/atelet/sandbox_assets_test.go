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

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/filecache"
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
)

func TestExtractTarArchive(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		compress  func(io.Writer) io.WriteCloser
		wantError bool
		errMsg    string
	}{
		{
			name: ".tar.gz format",
			url:  "https://example.com/gvisor.tar.gz",
			compress: func(w io.Writer) io.WriteCloser {
				return gzip.NewWriter(w)
			},
			wantError: false,
		},
		{
			name: ".tgz format",
			url:  "https://example.com/gvisor.tgz",
			compress: func(w io.Writer) io.WriteCloser {
				return gzip.NewWriter(w)
			},
			wantError: false,
		},
		{
			name:      ".tar.bz2 format",
			url:       "https://example.com/gvisor.tar.bz2",
			compress:  nil,
			wantError: false,
		},
		{
			name: ".tar.zst format",
			url:  "https://example.com/gvisor.tar.zst",
			compress: func(w io.Writer) io.WriteCloser {
				zw, _ := zstd.NewWriter(w) // no options, cannot fail
				return zw
			},
			wantError: false,
		},
		{
			name: ".tar.zstd format",
			url:  "https://example.com/gvisor.tar.zstd",
			compress: func(w io.Writer) io.WriteCloser {
				zw, _ := zstd.NewWriter(w) // no options, cannot fail
				return zw
			},
			wantError: false,
		},
		{
			name: ".tar format (uncompressed)",
			url:  "https://example.com/gvisor.tar",
			compress: func(w io.Writer) io.WriteCloser {
				return nopWriteCloser{w}
			},
			wantError: false,
		},
		{
			name:      "unsupported format",
			url:       "https://example.com/gvisor.zip",
			compress:  nil,
			wantError: true,
			errMsg:    "unsupported archive format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			archivePath := filepath.Join(tempDir, "archive")
			destDir := filepath.Join(tempDir, "dest")

			if tt.compress != nil {
				f, err := os.Create(archivePath)
				if err != nil {
					t.Fatalf("failed to create archive: %v", err)
				}

				cw := tt.compress(f)
				tw := tar.NewWriter(cw)

				hdr := &tar.Header{
					Name: "test.txt",
					Mode: 0644,
					Size: int64(len("hello world")),
				}
				if err := tw.WriteHeader(hdr); err != nil {
					t.Fatalf("failed to write tar header: %v", err)
				}
				if _, err := tw.Write([]byte("hello world")); err != nil {
					t.Fatalf("failed to write tar content: %v", err)
				}

				tw.Close()
				cw.Close()
				f.Close()
			} else if !tt.wantError && strings.Contains(tt.url, ".tar.bz2") {
				f, err := os.Create(archivePath)
				if err != nil {
					t.Fatalf("failed to create archive: %v", err)
				}
				f.Write([]byte("invalid bzip2 data"))
				f.Close()

				err = extractTarArchive(context.Background(), archivePath, tt.url, destDir)
				if err == nil {
					t.Fatal("expected error for invalid bzip2 data, got nil")
				}
				if strings.Contains(err.Error(), "unsupported archive format") {
					t.Errorf("expected bzip2 read error, got: %v", err)
				}
				return
			}

			err := extractTarArchive(context.Background(), archivePath, tt.url, destDir)
			if (err != nil) != tt.wantError {
				t.Fatalf("extractTarArchive() error = %v, wantError = %v", err, tt.wantError)
			}

			if tt.wantError {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errMsg)
				}
				return
			}

			content, err := os.ReadFile(filepath.Join(destDir, "test.txt"))
			if err != nil {
				t.Fatalf("failed to read extracted file: %v", err)
			}
			if string(content) != "hello world" {
				t.Errorf("extracted content = %q, want %q", content, "hello world")
			}
		})
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}

// newTestAssetCacheAt opens an asset cache under root/cache. Destinations a
// test links into should live under the same root, on the cache's
// filesystem.
func newTestAssetCacheAt(t *testing.T, root string, opts ...filecache.Option) *filecache.Store {
	t.Helper()
	store, err := filecache.New(filepath.Join(root, "cache"), opts...)
	if err != nil {
		t.Fatalf("filecache.New: %v", err)
	}
	return store
}

// newTestAssetCache opens an asset cache in a fresh temp dir.
func newTestAssetCache(t *testing.T) *filecache.Store {
	t.Helper()
	return newTestAssetCacheAt(t, t.TempDir())
}

// cachedAssets returns the number of assets published in the cache under
// root (see newTestAssetCacheAt): one directory per asset under entries/.
func cachedAssets(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "cache", "entries"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// useTempAssetScratch points the scratch dir of fetches without an actor
// at a temp dir. Not parallel-safe: the path is process-global.
func useTempAssetScratch(t *testing.T) {
	t.Helper()
	orig := ateletpath.SandboxAssetScratchDir
	ateletpath.SandboxAssetScratchDir = t.TempDir()
	t.Cleanup(func() { ateletpath.SandboxAssetScratchDir = orig })
}

var stageSeq struct {
	sync.Mutex
	n int
}

// stageTestAsset stages entry through s into a fresh destination under
// root (the cache's filesystem) and returns the staged path.
func stageTestAsset(t *testing.T, s *Atelet, root string, entry assetEntry) (string, error) {
	t.Helper()
	stageSeq.Lock()
	stageSeq.n++
	n := stageSeq.n
	stageSeq.Unlock()
	dir := filepath.Join(root, "consumer", strconv.Itoa(n))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return s.stageAsset(t.Context(), entry, filepath.Join(dir, "asset"))
}

// gvisorReleaseTarball builds an uncompressed gVisor release tarball: runsc
// next to a gvisor-bin/ helper, as the real release ships them.
func gvisorReleaseTarball(t *testing.T) []byte {
	t.Helper()
	return gvisorReleaseTarballWith(t, "runsc bytes")
}

// gvisorReleaseTarballWith is gvisorReleaseTarball with the given runsc
// contents, for releases that must differ in digest.
func gvisorReleaseTarballWith(t *testing.T, runsc string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, f := range []struct {
		name, body string
		mode       int64
	}{
		{"runsc", runsc, 0o755},
		{"gvisor-bin/containerd-shim", "shim bytes", 0o755},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: f.name, Mode: f.mode, Size: int64(len(f.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(f.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// gvisorTestRelease is a gVisor release tarball with the asset entry that
// names it: the one asset a gvisor sandbox record carries.
type gvisorTestRelease struct {
	tarball []byte
	entry   assetEntry
}

func newGVisorTestRelease(t *testing.T) gvisorTestRelease {
	t.Helper()
	tarball := gvisorReleaseTarball(t)
	return gvisorTestRelease{
		tarball: tarball,
		entry:   assetEntry{URL: "gs://test-bucket/gvisor.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(tarball))},
	}
}

// stagedRunsc is where an actor's staged copy of the release's runsc sits
// under its sandbox assets dir.
func stagedRunsc(actorUID string) string {
	return filepath.Join(ateletpath.SandboxAssetsDir(actorUID), gvisorAssetName, "runsc")
}

// TestStageGVisorRelease stages the release tree through the asset cache:
// one download and extraction serves every actor, each gets its own
// directory of links to the cached files, and the tarball itself is not
// published with them.
func TestStageGVisorRelease(t *testing.T) {
	tarball := gvisorReleaseTarball(t)
	entry := assetEntry{URL: "gs://gvisor/release.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(tarball))}
	root := t.TempDir()
	var gets atomic.Int32
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: tarball, gets: &gets}}

	dst1 := filepath.Join(root, "actor-1", gvisorAssetName)
	if err := os.MkdirAll(filepath.Dir(dst1), 0o755); err != nil {
		t.Fatal(err)
	}
	runsc1, err := s.stageGVisorRelease(t.Context(), entry, dst1)
	if err != nil {
		t.Fatalf("stageGVisorRelease: %v", err)
	}
	if runsc1 != filepath.Join(dst1, "runsc") {
		t.Errorf("runsc path = %q, want it inside %q", runsc1, dst1)
	}
	for rel, want := range map[string]string{"runsc": "runsc bytes", "gvisor-bin/containerd-shim": "shim bytes"} {
		got, err := os.ReadFile(filepath.Join(dst1, rel))
		if err != nil || string(got) != want {
			t.Errorf("%s = %q, %v; want %q", rel, got, err, want)
		}
		fi, err := os.Stat(filepath.Join(dst1, rel))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o555 {
			t.Errorf("%s mode = %v, want 0555", rel, fi.Mode().Perm())
		}
	}
	if _, err := os.Lstat(filepath.Join(dst1, ".gvisor-release-download")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the downloaded tarball was staged with the tree (lstat err = %v)", err)
	}

	// A second actor links the same cached tree: no second download.
	dst2 := filepath.Join(root, "actor-2", gvisorAssetName)
	if err := os.MkdirAll(filepath.Dir(dst2), 0o755); err != nil {
		t.Fatal(err)
	}
	runsc2, err := s.stageGVisorRelease(t.Context(), entry, dst2)
	if err != nil {
		t.Fatalf("stageGVisorRelease (hit): %v", err)
	}
	fi1, err1 := os.Stat(runsc1)
	fi2, err2 := os.Stat(runsc2)
	if err1 != nil || err2 != nil || !os.SameFile(fi1, fi2) {
		t.Errorf("the two actors' runsc are not one inode (%v, %v)", err1, err2)
	}
	if gets.Load() != 1 {
		t.Errorf("release downloaded %d times, want 1", gets.Load())
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("cache holds %d assets, want 1", n)
	}

	// Re-staging onto the first actor's existing tree keeps it.
	again, err := s.stageGVisorRelease(t.Context(), entry, dst1)
	if err != nil || again != runsc1 {
		t.Errorf("stageGVisorRelease onto a staged tree = %q, %v; want %q", again, err, runsc1)
	}

	// A tarball without runsc is rejected and nothing is published.
	var noRunsc bytes.Buffer
	tw := tar.NewWriter(&noRunsc)
	if err := tw.WriteHeader(&tar.Header{Name: "README", Mode: 0o644, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	bad := assetEntry{URL: "gs://gvisor/bad.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(noRunsc.Bytes()))}
	s = &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: noRunsc.Bytes()}}
	if _, err := s.stageGVisorRelease(t.Context(), bad, filepath.Join(root, "actor-3", gvisorAssetName)); err == nil || !strings.Contains(err.Error(), "no runsc binary") {
		t.Errorf("stageGVisorRelease of a tarball without runsc = %v, want the missing-runsc error", err)
	}
	if n := cachedAssets(t, root); n != 1 {
		t.Errorf("cache holds %d assets after the rejected tarball, want 1", n)
	}
}

// TestEnsureSandboxAssetsConcurrentSameActor pins that two calls for one
// actor at once, as a Checkpoint racing a Terminate, both succeed with the
// same paths: the loser of each link finds the winner's complete result.
func TestEnsureSandboxAssetsConcurrentSameActor(t *testing.T) {
	tarball, kernel := gvisorReleaseTarball(t), []byte("kernel bytes")
	root := t.TempDir()
	storage := &bucketStorage{objects: map[string][]byte{"gvisor/release.tar": tarball, "assets/vmlinux": kernel}}
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: storage}
	// A tree asset and a single-file asset, so both link-out paths race.
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets: map[string]assetEntry{
			gvisorAssetName: {URL: "gs://gvisor/release.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(tarball))},
			"kata-kernel":   {URL: "gs://assets/vmlinux", SHA256: fmt.Sprintf("%x", sha256.Sum256(kernel))},
		},
	}
	dstDir := filepath.Join(root, "actor", "sandbox-assets")

	const n = 8
	var wg sync.WaitGroup
	results := make([]map[string]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { results[i], errs[i] = s.ensureSandboxAssets(t.Context(), rec, dstDir) })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		if results[i][gvisorAssetName] != filepath.Join(dstDir, gvisorAssetName, "runsc") || results[i]["kata-kernel"] != filepath.Join(dstDir, "kata-kernel") {
			t.Errorf("call %d paths = %v", i, results[i])
		}
	}
	if _, err := os.Stat(filepath.Join(dstDir, gvisorAssetName, "gvisor-bin", "containerd-shim")); err != nil {
		t.Errorf("staged tree incomplete: %v", err)
	}
	siblings, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(siblings) != 2 {
		t.Errorf("sandbox assets dir holds %d entries, want the 2 assets only", len(siblings))
	}
}

// TestStageAssetFetchErrorWrappingErrExistIsNotStaged pins that only an
// existing destination counts as staged by a concurrent call: a fetcher
// failing with an error that happens to wrap ErrExist still fails the
// stage, rather than reporting success with nothing at the destination.
func TestStageAssetFetchErrorWrappingErrExistIsNotStaged(t *testing.T) {
	root := t.TempDir()
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{err: fmt.Errorf("object already there: %w", fs.ErrExist)}}
	entry := assetEntry{URL: "gs://bucket/runsc", SHA256: strings.Repeat("d", 64)}
	dst := filepath.Join(root, "actor", "runsc")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := s.stageAsset(t.Context(), entry, dst); err == nil {
		t.Fatal("stageAsset reported success with nothing staged")
	}
	if _, err := os.Lstat(dst); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("destination exists after a failed fetch (lstat err = %v)", err)
	}
}

// TestEnsureSandboxAssetsRejectsBadAssetNames pins that an asset name that
// is not one plain path element is refused before anything is fetched or
// staged. "." is the case that slips past Base and IsLocal: it would make
// the assets dir itself the destination, which staged would then report as
// present without a fetch.
func TestEnsureSandboxAssetsRejectsBadAssetNames(t *testing.T) {
	content := []byte("kernel bytes")
	sha := fmt.Sprintf("%x", sha256.Sum256(content))
	for _, name := range []string{".", "..", "", "kata/kernel", "/kata-kernel", "./kata-kernel"} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			root := t.TempDir()
			var gets atomic.Int32
			s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: content, gets: &gets}}
			dstDir := filepath.Join(root, "actor", "sandbox-assets")
			rec := &sandboxAssetsRecord{SandboxClass: "microvm", Assets: map[string]assetEntry{name: {URL: "gs://assets/vmlinux", SHA256: sha}}}
			_, err := s.ensureSandboxAssets(t.Context(), rec, dstDir)
			if apierror.Code(err) != codes.InvalidArgument {
				t.Fatalf("ensureSandboxAssets with asset name %q = %v (code %s), want %s", name, err, apierror.Code(err), codes.InvalidArgument)
			}
			if gets.Load() != 0 {
				t.Errorf("asset fetched %d times for a refused name", gets.Load())
			}
			if entries, _ := os.ReadDir(dstDir); len(entries) != 0 {
				t.Errorf("refused name left %d entries in the assets dir", len(entries))
			}
		})
	}

	// A plain name is staged as the file it names.
	root := t.TempDir()
	s := &Atelet{sandboxAssetCache: newTestAssetCacheAt(t, root), anonGCSClient: fakeObjectStorage{data: content}}
	dstDir := filepath.Join(root, "actor", "sandbox-assets")
	paths, err := s.ensureSandboxAssets(t.Context(), &sandboxAssetsRecord{SandboxClass: "microvm", Assets: map[string]assetEntry{"kata-kernel": {URL: "gs://assets/vmlinux", SHA256: sha}}}, dstDir)
	if err != nil {
		t.Fatalf("ensureSandboxAssets: %v", err)
	}
	if fi, err := os.Stat(paths["kata-kernel"]); err != nil || !fi.Mode().IsRegular() || paths["kata-kernel"] != filepath.Join(dstDir, "kata-kernel") {
		t.Errorf("kata-kernel staged at %q (%v), want a regular file at %s", paths["kata-kernel"], err, filepath.Join(dstDir, "kata-kernel"))
	}
}

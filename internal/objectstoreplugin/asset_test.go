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

package objectstoreplugin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// countingObjects counts reads, so a test can tell that a request was refused
// before anything was read.
type countingObjects struct {
	*memObjects
	gets atomic.Int64
}

func (c *countingObjects) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.memObjects.GetObject(ctx, bucket, object)
}

// assetFixture is an AssetProvider served on a real Unix socket, as the
// sidecar serves it, with a write root and a separate staging directory.
type assetFixture struct {
	client  objectstorev1.AssetProviderClient
	backend *countingObjects
	root    string
	staging string
}

func newAssetFixture(t *testing.T) *assetFixture {
	t.Helper()
	f := &assetFixture{
		backend: &countingObjects{memObjects: newMemObjects()},
		root:    t.TempDir(),
		staging: t.TempDir(),
	}
	plugin, err := NewAssetPlugin(f.backend, f.root, f.staging)
	if err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("", "assetplug")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sock := filepath.Join(sockDir, "plugin.sock")
	lis, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	objectstorev1.RegisterAssetProviderServer(srv, plugin)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := Dial(sock, ReadyWait)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	f.client = objectstorev1.NewAssetProviderClient(conn)
	return f
}

// put stores content at bucket/object and returns its sha256.
func (f *assetFixture) put(t *testing.T, object string, content []byte) string {
	t.Helper()
	if err := f.backend.PutObject(t.Context(), testBucket, object, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(content))
}

// writeFile creates the caller's file below root with content and returns
// its path.
func (f *assetFixture) writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(f.root, "static-files", "asset-download-1")
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// assertContent fails unless path holds want.
func assertContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Errorf("%s = %q, want %q", path, got, want)
	}
}

// assertStagingEmpty fails if a download was left in the staging directory.
func (f *assetFixture) assertStagingEmpty(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(f.staging)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("staging directory holds %d entries after the call, want none", len(entries))
	}
}

const untouched = "the caller's original bytes"

func TestFetchAsset(t *testing.T) {
	content := []byte("micro-vm kernel bytes")

	t.Run("happy path", func(t *testing.T) {
		f := newAssetFixture(t)
		for _, scheme := range []string{"gs", "s3"} {
			sum := f.put(t, "kata-assets/vmlinux", content)
			dst := f.writeFile(t, untouched)
			_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
				AssetUri:  scheme + "://" + testBucket + "/kata-assets/vmlinux",
				Sha256:    sum,
				WritePath: dst,
				MaxBytes:  int64(len(content)),
			})
			if err != nil {
				t.Fatalf("%s: FetchAsset: %v", scheme, err)
			}
			assertContent(t, dst, string(content))
			f.assertStagingEmpty(t)
		}
	})

	t.Run("digest mismatch leaves write_path untouched", func(t *testing.T) {
		f := newAssetFixture(t)
		f.put(t, "kata-assets/vmlinux", content)
		dst := f.writeFile(t, untouched)
		_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
			AssetUri:  "gs://" + testBucket + "/kata-assets/vmlinux",
			Sha256:    strings.Repeat("a", 64),
			WritePath: dst,
			MaxBytes:  1 << 20,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("FetchAsset with a wrong sha256 = %v, want %s", err, codes.FailedPrecondition)
		}
		assertContent(t, dst, untouched)
		f.assertStagingEmpty(t)
	})

	t.Run("max_bytes is enforced", func(t *testing.T) {
		f := newAssetFixture(t)
		sum := f.put(t, "kata-assets/vmlinux", content)
		dst := f.writeFile(t, untouched)
		_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
			AssetUri:  "gs://" + testBucket + "/kata-assets/vmlinux",
			Sha256:    sum,
			WritePath: dst,
			MaxBytes:  int64(len(content)) - 1,
		})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("FetchAsset of an object over max_bytes = %v, want %s", err, codes.FailedPrecondition)
		}
		assertContent(t, dst, untouched)
		f.assertStagingEmpty(t)
	})

	t.Run("not found", func(t *testing.T) {
		f := newAssetFixture(t)
		dst := f.writeFile(t, untouched)
		_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
			AssetUri:  "gs://" + testBucket + "/kata-assets/missing",
			Sha256:    strings.Repeat("a", 64),
			WritePath: dst,
			MaxBytes:  1 << 20,
		})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("FetchAsset of a missing object = %v, want %s", err, codes.NotFound)
		}
		assertContent(t, dst, untouched)
		f.assertStagingEmpty(t)
	})

	t.Run("write_path must be an existing regular file below root", func(t *testing.T) {
		f := newAssetFixture(t)
		sum := f.put(t, "kata-assets/vmlinux", content)
		outside := filepath.Join(t.TempDir(), "outside")
		if err := os.WriteFile(outside, []byte(untouched), 0o600); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(f.root, "dir")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(f.root, "link")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name, path string
			want       codes.Code
		}{
			{"outside root", outside, codes.InvalidArgument},
			{"escapes root", filepath.Join(f.root, "..", filepath.Base(outside)), codes.InvalidArgument},
			{"relative", "static-files/x", codes.InvalidArgument},
			{"missing", filepath.Join(f.root, "missing"), codes.FailedPrecondition},
			{"directory", dir, codes.FailedPrecondition},
			{"symlink to a file outside root", link, codes.FailedPrecondition},
		} {
			_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
				AssetUri:  "gs://" + testBucket + "/kata-assets/vmlinux",
				Sha256:    sum,
				WritePath: tc.path,
				MaxBytes:  1 << 20,
			})
			if status.Code(err) != tc.want {
				t.Errorf("%s: FetchAsset = %v, want %s", tc.name, err, tc.want)
			}
		}
		assertContent(t, outside, untouched)
		if _, err := os.Lstat(filepath.Join(f.root, "missing")); !os.IsNotExist(err) {
			t.Errorf("FetchAsset created the missing write path (lstat err = %v)", err)
		}
		if n := f.backend.gets.Load(); n != 0 {
			t.Errorf("rejected requests read %d objects, want 0", n)
		}
	})

	t.Run("invalid requests", func(t *testing.T) {
		f := newAssetFixture(t)
		sum := f.put(t, "kata-assets/vmlinux", content)
		dst := f.writeFile(t, untouched)
		for _, tc := range []struct {
			name, uri, sum string
			max            int64
		}{
			{"https scheme", "https://storage.example.com/" + testBucket + "/kata-assets/vmlinux", sum, 1 << 20},
			{"no object", "gs://" + testBucket, sum, 1 << 20},
			{"no bucket", "gs:///kata-assets/vmlinux", sum, 1 << 20},
			{"query", "gs://" + testBucket + "/kata-assets/vmlinux?sig=secret", sum, 1 << 20},
			{"user info", "gs://usr0:secret@" + testBucket + "/kata-assets/vmlinux", sum, 1 << 20},
			{"fragment", "gs://" + testBucket + "/kata-assets/vmlinux#x", sum, 1 << 20},
			{"dot segment", "gs://" + testBucket + "/kata-assets/../kata-assets/vmlinux", sum, 1 << 20},
			{"empty segment", "gs://" + testBucket + "/kata-assets//vmlinux", sum, 1 << 20},
			{"trailing slash", "gs://" + testBucket + "/kata-assets/", sum, 1 << 20},
			{"padded segment", "gs://" + testBucket + "/kata-assets /vmlinux", sum, 1 << 20},
			{"backslash", "gs://" + testBucket + `/kata-assets\vmlinux`, sum, 1 << 20},
			{"upper-case sha256", "gs://" + testBucket + "/kata-assets/vmlinux", strings.ToUpper(sum), 1 << 20},
			{"short sha256", "gs://" + testBucket + "/kata-assets/vmlinux", sum[:63], 1 << 20},
			{"zero max_bytes", "gs://" + testBucket + "/kata-assets/vmlinux", sum, 0},
		} {
			_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
				AssetUri: tc.uri, Sha256: tc.sum, WritePath: dst, MaxBytes: tc.max,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("%s: FetchAsset = %v, want %s", tc.name, err, codes.InvalidArgument)
			}
			for _, secret := range []string{"secret", "usr0"} {
				if strings.Contains(status.Convert(err).Message(), secret) {
					t.Errorf("%s: error %q repeats the URI's credentials", tc.name, err)
				}
			}
		}
		assertContent(t, dst, untouched)
		if n := f.backend.gets.Load(); n != 0 {
			t.Errorf("invalid requests read %d objects, want 0", n)
		}
	})
}

// TestFetchAssetRefusesSnapshots seeds snapshot and tag objects in the same
// bucket as the assets, and asks for each with its correct sha256: the plugin
// must refuse every one before reading it.
func TestFetchAssetRefusesSnapshots(t *testing.T) {
	f := newAssetFixture(t)
	snapshotObjects := []string{
		testPrefix + "/manifest.json",
		testPrefix + "/checkpoint.img.zstd",
		"root/atespaces/team-a/tags/tag1/manifest.json",
		"root/atespaces/team-a/tags/tag1/checkpoint.img.zstd",
		// A snapshot at the bucket root, and the bare snapshot and tag
		// prefixes, in case a backend stores an object under that name.
		"atespaces/team-a/actors/uid1/snapshots/snap1/manifest.json",
		testPrefix,
		"root/atespaces/team-a/tags/tag1",
	}
	sums := map[string]string{}
	for _, object := range snapshotObjects {
		sums[object] = f.put(t, object, []byte("snapshot bytes of "+object))
	}
	assetSum := f.put(t, "kata-assets/vmlinux", []byte("kernel"))

	dst := f.writeFile(t, untouched)
	for _, scheme := range []string{"gs", "s3"} {
		for _, object := range snapshotObjects {
			uri := scheme + "://" + testBucket + "/" + object
			_, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
				AssetUri:  uri,
				Sha256:    sums[object],
				WritePath: dst,
				MaxBytes:  1 << 20,
			})
			if status.Code(err) != codes.PermissionDenied {
				t.Errorf("FetchAsset(%s) = %v, want %s", uri, err, codes.PermissionDenied)
			}
		}
	}
	assertContent(t, dst, untouched)
	if n := f.backend.gets.Load(); n != 0 {
		t.Errorf("refused requests read %d objects, want 0", n)
	}

	// Assets in the same bucket are still served, including names that
	// share a segment with the snapshot layout.
	for _, object := range []string{"kata-assets/vmlinux", "atespaces/readme", "root/atespaces/team-a/actors/uid1"} {
		sum := assetSum
		if object != "kata-assets/vmlinux" {
			sum = f.put(t, object, []byte("asset "+object))
		}
		if _, err := f.client.FetchAsset(t.Context(), &objectstorev1.FetchAssetRequest{
			AssetUri:  "gs://" + testBucket + "/" + object,
			Sha256:    sum,
			WritePath: dst,
			MaxBytes:  1 << 20,
		}); err != nil {
			t.Errorf("FetchAsset(%s) = %v, want success", object, err)
		}
	}
}

func TestNewAssetPluginRejectsStagingBelowRoot(t *testing.T) {
	root := t.TempDir()
	for _, staging := range []string{root, filepath.Join(root, "staging")} {
		if _, err := NewAssetPlugin(newMemObjects(), root, staging); err == nil {
			t.Errorf("NewAssetPlugin(root=%s, staging=%s) succeeded, want an error", root, staging)
		}
	}
	for _, tc := range []struct{ root, staging string }{{"relative", t.TempDir()}, {root, "relative"}} {
		if _, err := NewAssetPlugin(newMemObjects(), tc.root, tc.staging); err == nil {
			t.Errorf("NewAssetPlugin(root=%s, staging=%s) succeeded, want an error", tc.root, tc.staging)
		}
	}
	if _, err := NewAssetPlugin(newMemObjects(), root, t.TempDir()); err != nil {
		t.Errorf("NewAssetPlugin with a separate staging directory: %v", err)
	}
}

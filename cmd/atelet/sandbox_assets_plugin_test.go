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
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/objectstoreplugin"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/pluginsocket"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/pkg/objectstorage"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	testAssetBucket = "cluster-bucket"
	testPluginSock  = "/run/objectstore-plugin/node.sock"
)

// bucketStorage is an in-memory object store keyed by "bucket/object" that
// counts reads.
type bucketStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    atomic.Int64
}

func (b *bucketStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	b.gets.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", objectstorage.ErrObjectNotFound, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *bucketStorage) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[bucket+"/"+object] = data
	return nil
}

// countingAssetClient counts FetchAsset calls before passing them on.
type countingAssetClient struct {
	objectstorev1.AssetProviderClient
	calls atomic.Int64
}

func (c *countingAssetClient) FetchAsset(ctx context.Context, in *objectstorev1.FetchAssetRequest, opts ...grpc.CallOption) (*objectstorev1.FetchAssetResponse, error) {
	c.calls.Add(1)
	return c.AssetProviderClient.FetchAsset(ctx, in, opts...)
}

// assetHerderFixture is an Atelet whose assets come from the in-tree
// plugin on an in-memory bucket, with an anonymous client that cannot read
// that bucket.
type assetHerderFixture struct {
	herder  *Atelet
	bucket  *bucketStorage
	anon    *bucketStorage
	plugin  *countingAssetClient
	staging string
}

// newAssetHerderFixture's plugin serves assets of scheme ("gs" or "s3"), as
// the in-tree plugin on that backend does.
func newAssetHerderFixture(t *testing.T, scheme string) *assetHerderFixture {
	t.Helper()
	origDir, origCap := nodepath.StaticFilesDir, maxAssetBytes
	t.Cleanup(func() { nodepath.StaticFilesDir, maxAssetBytes = origDir, origCap })
	root := t.TempDir()
	nodepath.StaticFilesDir = filepath.Join(root, "static-files")
	if err := os.MkdirAll(nodepath.StaticFilesDir, 0o700); err != nil {
		t.Fatal(err)
	}
	f := &assetHerderFixture{
		bucket:  &bucketStorage{objects: map[string][]byte{}},
		anon:    &bucketStorage{objects: map[string][]byte{}},
		staging: t.TempDir(),
	}
	plugin, err := objectstoreplugin.NewAssetPlugin(f.bucket, scheme, root, f.staging, maxAssetBytes)
	if err != nil {
		t.Fatal(err)
	}
	f.plugin = &countingAssetClient{AssetProviderClient: objectstoreplugintest.AssetClient(plugin)}
	f.herder = &Atelet{anonGCSClient: f.anon, assetPlugin: f.plugin, pluginSocket: testPluginSock}
	return f
}

// put stores content in the cluster bucket and returns its sha256.
func (f *assetHerderFixture) put(t *testing.T, object string, content []byte) string {
	t.Helper()
	if err := f.bucket.PutObject(t.Context(), testAssetBucket, object, bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(content))
}

// assertNotCached fails if fetching left anything in the static-files cache:
// neither the cache file nor a temp file.
func assertNotCached(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(nodepath.StaticFilesDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("failed fetch left %s in the static-files cache", e.Name())
	}
}

// TestFetchAssetThroughPlugin covers assets atelet cannot read anonymously:
// they come from the object-store plugin, and atelet still verifies what it
// caches.
func TestFetchAssetThroughPlugin(t *testing.T) {
	content := []byte("micro-vm kernel bytes")

	t.Run("non-gs scheme goes straight to the plugin", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		sum := f.put(t, "kata-assets/vmlinux", content)
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: sum})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
		if n := f.anon.gets.Load(); n != 0 {
			t.Errorf("an s3:// asset was tried anonymously %d times", n)
		}
	})

	t.Run("gs scheme served by the plugin is not read anonymously", func(t *testing.T) {
		f := newAssetHerderFixture(t, "gs")
		sum := f.put(t, "kata-assets/vmlinux", content)
		f.anon.objects[testAssetBucket+"/kata-assets/vmlinux"] = content
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "gs://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: sum})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
		if f.anon.gets.Load() != 0 || f.plugin.calls.Load() != 1 {
			t.Errorf("anonymous reads = %d, plugin calls = %d; want 0 and 1", f.anon.gets.Load(), f.plugin.calls.Load())
		}
	})

	t.Run("gs asset on an S3 plugin is read anonymously without touching S3", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		// An S3 bucket of the same name, which on AWS could be anyone's.
		if err := f.bucket.PutObject(t.Context(), "gvisor", "release.tar", strings.NewReader("someone else's bytes")); err != nil {
			t.Fatal(err)
		}
		f.anon.objects["gvisor/release.tar"] = content
		sum := fmt.Sprintf("%x", sha256.Sum256(content))
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "gs://gvisor/release.tar", SHA256: sum})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
		if f.bucket.gets.Load() != 0 || f.anon.gets.Load() != 1 {
			t.Errorf("S3 reads = %d, anonymous reads = %d; want 0 and 1", f.bucket.gets.Load(), f.anon.gets.Load())
		}
	})

	t.Run("public gs asset the plugin does not have is read anonymously", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.anon.objects["gvisor/release.tar"] = content
		sum := fmt.Sprintf("%x", sha256.Sum256(content))
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "gs://gvisor/release.tar", SHA256: sum})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
		if f.anon.gets.Load() != 1 || f.plugin.calls.Load() != 1 {
			t.Errorf("anonymous reads = %d, plugin calls = %d; want 1 each", f.anon.gets.Load(), f.plugin.calls.Load())
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		sum := f.put(t, "kata-assets/vmlinux", content)
		cached := ateletpath.AssetPath(sum)
		if err := os.WriteFile(cached, content, 0o755); err != nil {
			t.Fatal(err)
		}
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: sum})
		if err != nil || path != cached {
			t.Fatalf("fetchAsset = %q, %v; want %q", path, err, cached)
		}
		if n := f.plugin.calls.Load(); n != 0 {
			t.Errorf("plugin called %d times on a cache hit", n)
		}
	})

	t.Run("sha256 mismatch", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.put(t, "kata-assets/vmlinux", content)
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: strings.Repeat("a", 64)})
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("fetchAsset with a wrong sha256 = %v (code %s), want %s", err, apierror.Code(err), codes.FailedPrecondition)
		}
		assertNotCached(t)
	})

	t.Run("size cap", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		sum := f.put(t, "kata-assets/vmlinux", content)
		maxAssetBytes = int64(len(content)) - 1
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: sum})
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("fetchAsset over the cap = %v (code %s), want %s", err, apierror.Code(err), codes.FailedPrecondition)
		}
		assertNotCached(t)
	})

	t.Run("not found", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/missing", SHA256: strings.Repeat("a", 64)})
		// Not NotFound: ate-api-server reads NotFound from Terminate as
		// "already terminated".
		if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "s3://"+testAssetBucket+"/kata-assets/missing") {
			t.Fatalf("fetchAsset of a missing object = %v (code %s), want %s naming the URL", err, apierror.Code(err), codes.FailedPrecondition)
		}
		assertNotCached(t)
	})

	t.Run("snapshot in the same bucket is refused", func(t *testing.T) {
		f := newAssetHerderFixture(t, "gs")
		sum := f.put(t, "root/atespaces/team-a/actors/uid1/snapshots/snap1/checkpoint.img.zstd", []byte("another actor's memory"))
		f.anon.objects[testAssetBucket+"/root/atespaces/team-a/actors/uid1/snapshots/snap1/checkpoint.img.zstd"] = []byte("another actor's memory")
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "gs://" + testAssetBucket + "/root/atespaces/team-a/actors/uid1/snapshots/snap1/checkpoint.img.zstd", SHA256: sum})
		if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "snapshot location") {
			t.Fatalf("fetchAsset of a snapshot file = %v (code %s), want %s about the snapshot location", err, apierror.Code(err), codes.FailedPrecondition)
		}
		if n := f.bucket.gets.Load(); n != 0 {
			t.Errorf("the plugin read %d objects for a refused request", n)
		}
		if n := f.anon.gets.Load(); n != 0 {
			t.Errorf("a refused snapshot file was tried anonymously %d times", n)
		}
		assertNotCached(t)
	})

	t.Run("connection error is Unavailable", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		conn, err := pluginsocket.Dial(filepath.Join(t.TempDir(), "missing.sock"), 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		f.herder.assetPlugin = objectstorev1.NewAssetProviderClient(conn)
		_, err = f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: strings.Repeat("a", 64)})
		if apierror.Code(err) != codes.Unavailable {
			t.Fatalf("fetchAsset with the plugin down = %v (code %s), want %s", err, apierror.Code(err), codes.Unavailable)
		}
		assertNotCached(t)
	})

	t.Run("plugin without AssetProvider", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		sockDir, err := os.MkdirTemp("", "atelet-assets")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })
		sock := filepath.Join(sockDir, "node.sock")
		lis, err := pluginsocket.Listen(sock)
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		healthpb.RegisterHealthServer(srv, health.NewServer())
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		conn, err := pluginsocket.Dial(sock, pluginsocket.ReadyWait)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		f.herder.assetPlugin = objectstorev1.NewAssetProviderClient(conn)
		f.herder.pluginSocket = sock
		_, err = f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: strings.Repeat("a", 64)})
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Fatalf("fetchAsset with an old plugin = %v (code %s), want %s", err, apierror.Code(err), codes.FailedPrecondition)
		}
		for _, want := range []string{"s3://" + testAssetBucket + "/kata-assets/vmlinux", sock, "objectstore.v1.AssetProvider"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	})
}

// fakeAssetClient writes data, if set, into write_path and returns err,
// standing in for a third-party plugin.
type fakeAssetClient struct {
	data []byte
	err  error
}

func (c fakeAssetClient) FetchAsset(_ context.Context, in *objectstorev1.FetchAssetRequest, _ ...grpc.CallOption) (*objectstorev1.FetchAssetResponse, error) {
	if c.data != nil {
		if err := os.WriteFile(in.GetWritePath(), c.data, 0o600); err != nil {
			return nil, err
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	return &objectstorev1.FetchAssetResponse{}, nil
}

// TestFetchAssetAnonymousFallback covers when atelet reads an asset
// anonymously after the plugin fails: only for gs:// URLs, and only when the
// plugin cannot serve the asset rather than refuses it.
func TestFetchAssetAnonymousFallback(t *testing.T) {
	content := []byte("gvisor release bytes")
	sum := fmt.Sprintf("%x", sha256.Sum256(content))
	const (
		object    = "gvisor/releases/runsc"
		gsURL     = "gs://" + object
		pluginMsg = "plugin text SECRET-SIG"
	)
	pluginErr := func(code codes.Code) error { return status.Error(code, pluginMsg) }

	// Pinned here rather than read from anonymousFallbackCodes, so dropping a
	// code from production fails this test.
	for _, code := range []codes.Code{codes.NotFound, codes.InvalidArgument, codes.Unimplemented, codes.Unavailable, codes.Internal} {
		t.Run(code.String()+" falls back", func(t *testing.T) {
			logs := captureLogs(t)
			f := newAssetHerderFixture(t, "s3")
			f.anon.objects[object] = content
			// A partial write the plugin leaves behind is replaced.
			f.herder.assetPlugin = fakeAssetClient{data: []byte("partial plugin write, longer than the asset"), err: pluginErr(code)}
			path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: gsURL, SHA256: sum})
			if err != nil {
				t.Fatalf("fetchAsset: %v", err)
			}
			if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
				t.Errorf("cached bytes = %q, want %q", got, content)
			}
			if n := f.anon.gets.Load(); n != 1 {
				t.Errorf("anonymous reads = %d, want 1", n)
			}
			if !strings.Contains(logs.String(), `"code":"`+code.String()+`"`) || strings.Contains(logs.String(), "SECRET") {
				t.Errorf("logs do not carry the plugin code %s alone:\n%s", code, logs)
			}
			// A plugin that failed, rather than declined the URL, must stay
			// visible even though the anonymous read succeeded.
			wantLevel := `"level":"INFO"`
			if code == codes.Internal || code == codes.Unavailable {
				wantLevel = `"level":"WARN"`
			}
			var fallbackLine string
			for l := range strings.Lines(logs.String()) {
				if strings.Contains(l, "downloading it anonymously") {
					fallbackLine = l
				}
			}
			if !strings.Contains(fallbackLine, wantLevel) {
				t.Errorf("fallback log line is not at %s:\n%s", wantLevel, fallbackLine)
			}
		})

		t.Run(code.String()+" then anonymous failure reports both", func(t *testing.T) {
			logs := captureLogs(t)
			f := newAssetHerderFixture(t, "s3")
			f.herder.assetPlugin = fakeAssetClient{err: pluginErr(code)}
			_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: gsURL, SHA256: sum})
			if err == nil {
				t.Fatal("fetchAsset succeeded")
			}
			if n := f.anon.gets.Load(); n != 1 {
				t.Errorf("anonymous reads = %d, want 1", n)
			}
			// The status ate-api-server receives carries the apierror's own
			// message only, so both descriptions must be in it.
			st, _ := apierror.FromError(err)
			wantCode := codes.FailedPrecondition
			if code == codes.Unavailable {
				wantCode = codes.Unavailable
			} else if code == codes.Internal {
				wantCode = codes.Internal
			}
			if st.Code() != wantCode {
				t.Errorf("code = %s, want %s (%v)", st.Code(), wantCode, err)
			}
			for _, want := range []string{gsURL, "object-store plugin", "anonymous download failed", "object not found"} {
				if !strings.Contains(st.Message(), want) {
					t.Errorf("status message %q does not carry %q", st.Message(), want)
				}
			}
			for name, text := range map[string]string{"error": err.Error(), "logs": logs.String()} {
				if strings.Contains(text, "SECRET") {
					t.Errorf("%s carries the plugin's status text:\n%s", name, text)
				}
			}
			assertNotCached(t)
		})

		for _, u := range []string{"s3://" + object, "https://storage.example/" + object} {
			t.Run(code.String()+" "+u+" does not fall back", func(t *testing.T) {
				f := newAssetHerderFixture(t, "s3")
				f.anon.objects[object] = content
				f.herder.assetPlugin = fakeAssetClient{err: pluginErr(code)}
				if _, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: u, SHA256: sum}); err == nil {
					t.Fatal("fetchAsset succeeded")
				}
				if n := f.anon.gets.Load(); n != 0 {
					t.Errorf("anonymous reads = %d, want 0", n)
				}
				assertNotCached(t)
			})
		}
	}

	for _, code := range []codes.Code{codes.PermissionDenied, codes.FailedPrecondition, codes.Unknown, codes.ResourceExhausted, codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String()+" does not fall back", func(t *testing.T) {
			f := newAssetHerderFixture(t, "s3")
			f.anon.objects[object] = content
			f.herder.assetPlugin = fakeAssetClient{err: pluginErr(code)}
			_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: gsURL, SHA256: sum})
			if err == nil {
				t.Fatal("fetchAsset succeeded")
			}
			if n := f.anon.gets.Load(); n != 0 {
				t.Errorf("anonymous reads = %d, want 0", n)
			}
			if strings.Contains(err.Error(), "anonymous") || strings.Contains(err.Error(), "SECRET") {
				t.Errorf("error %q mentions an anonymous read or the plugin's text", err)
			}
			assertNotCached(t)
		})
	}

	t.Run("canceled request does not fall back", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.anon.objects[object] = content
		ctx, cancel := context.WithCancel(t.Context())
		f.herder.assetPlugin = cancelingAssetClient{cancel: cancel, err: pluginErr(codes.Unavailable)}
		_, err := f.herder.fetchAsset(ctx, assetEntry{URL: gsURL, SHA256: sum})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("fetchAsset = %v, want context.Canceled", err)
		}
		if n := f.anon.gets.Load(); n != 0 {
			t.Errorf("anonymous reads = %d, want 0", n)
		}
		assertNotCached(t)
	})

	t.Run("no plugin reads gs anonymously", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.anon.objects[object] = content
		f.herder.assetPlugin = nil
		path, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: gsURL, SHA256: sum})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		if got, _ := os.ReadFile(path); !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
		if n := f.anon.gets.Load(); n != 1 {
			t.Errorf("anonymous reads = %d, want 1", n)
		}
	})

	t.Run("no plugin, gs anonymous failure", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.herder.assetPlugin = nil
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: gsURL, SHA256: sum})
		if err == nil || !strings.Contains(err.Error(), "object not found") || strings.Contains(err.Error(), "object-store plugin") {
			t.Fatalf("fetchAsset = %v, want the anonymous read's error alone", err)
		}
		assertNotCached(t)
	})

	t.Run("no plugin, non-gs is refused", func(t *testing.T) {
		f := newAssetHerderFixture(t, "s3")
		f.anon.objects[object] = content
		f.herder.assetPlugin = nil
		_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + object, SHA256: sum})
		if apierror.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no object-store plugin") {
			t.Fatalf("fetchAsset = %v (code %s), want %s naming the missing plugin", err, apierror.Code(err), codes.FailedPrecondition)
		}
		if n := f.anon.gets.Load(); n != 0 {
			t.Errorf("anonymous reads = %d, want 0", n)
		}
		assertNotCached(t)
	})
}

// cancelingAssetClient cancels the caller's request, then fails with err.
type cancelingAssetClient struct {
	cancel context.CancelFunc
	err    error
}

func (c cancelingAssetClient) FetchAsset(context.Context, *objectstorev1.FetchAssetRequest, ...grpc.CallOption) (*objectstorev1.FetchAssetResponse, error) {
	c.cancel()
	return nil, c.err
}

// TestFetchAssetVerifiesPluginOutput checks that atelet does not trust a
// plugin's success: it re-checks the size cap and sha256 of what it caches.
func TestFetchAssetVerifiesPluginOutput(t *testing.T) {
	content := []byte("micro-vm kernel bytes")
	sum := fmt.Sprintf("%x", sha256.Sum256(content))
	for _, tc := range []struct {
		name string
		data []byte
		cap  int64
	}{
		{"wrong bytes", []byte("a snapshot's bytes"), 1 << 20},
		{"over the cap", content, int64(len(content)) - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAssetHerderFixture(t, "s3")
			maxAssetBytes = tc.cap
			f.herder.assetPlugin = fakeAssetClient{data: tc.data}
			if _, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "s3://" + testAssetBucket + "/kata-assets/vmlinux", SHA256: sum}); err == nil {
				t.Fatal("fetchAsset accepted bytes the plugin wrote without checking them")
			}
			assertNotCached(t)
		})
	}
}

// captureLogs routes the default logger, with atelet's redaction handler, to
// the returned buffer until the test ends.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })
	serverboot.InitLoggerWithWriter(&buf)
	return &buf
}

// TestFetchAssetKeepsCredentialsOutOfErrors checks that neither a signed
// asset URL nor a plugin's status text reaches atelet's logs or the error
// atelet returns to ate-api-server, which becomes the actor's crash message.
// The plugin texts include credentials that are not URLs, which no scrubbing
// of URLs would catch.
func TestFetchAssetKeepsCredentialsOutOfErrors(t *testing.T) {
	const secret = "SECRET-SIG"
	signed := "https://usr0:pw-" + secret + "@acct.blob.example/assets/vmlinux?sig=" + secret
	sum := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name     string
		plugin   objectstorev1.AssetProviderClient
		wantCode codes.Code
	}{
		{"in-tree plugin rejects the URL", nil, codes.Internal},
		{"plugin quotes the URL", fakeAssetClient{err: status.Error(codes.PermissionDenied, "denied "+signed)}, codes.FailedPrecondition},
		{"plugin quotes the URL in the query only", fakeAssetClient{err: status.Error(codes.NotFound, "get https://acct.blob.example/assets/vmlinux?sig="+secret+": 404")}, codes.FailedPrecondition},
		{"plugin fails with credentials in its text", fakeAssetClient{err: status.Error(codes.Internal, "AccountKey="+secret)}, codes.Internal},
		{"plugin text is a bearer token", fakeAssetClient{err: status.Error(codes.FailedPrecondition, "Authorization: Bearer "+secret)}, codes.FailedPrecondition},
		{"plugin unavailable", fakeAssetClient{err: status.Error(codes.Unavailable, "dial "+signed)}, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			f := newAssetHerderFixture(t, "s3")
			if tc.plugin != nil {
				f.herder.assetPlugin = tc.plugin
			}
			// atelet's server interceptor logs the handler's error and decides
			// what ate-api-server receives.
			_, err := ateinterceptors.InternalServerUnaryInterceptor(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"},
				func(ctx context.Context, _ any) (any, error) {
					return f.herder.fetchAsset(ctx, assetEntry{URL: signed, SHA256: sum})
				})
			if err == nil {
				t.Fatal("fetchAsset succeeded")
			}
			if tc.plugin != nil {
				if got := status.Code(err); got != tc.wantCode {
					t.Errorf("code = %s (%v), want %s", got, err, tc.wantCode)
				}
			}
			for name, text := range map[string]string{"error": err.Error(), "logs": logs.String()} {
				for _, leak := range []string{secret, "usr0", "sig=", "Bearer", "AccountKey"} {
					if strings.Contains(text, leak) {
						t.Errorf("%s carries %q:\n%s", name, leak, text)
					}
				}
			}
			if !strings.Contains(err.Error(), "https://acct.blob.example/assets/vmlinux") {
				t.Errorf("error %q does not name the asset as scheme://host/path", err)
			}
		})
	}
}

// TestFetchAssetLogsPluginCodeAndRedactedURL checks what the log does carry
// when the plugin fails: its code and the asset as scheme://host/path.
func TestFetchAssetLogsPluginCodeAndRedactedURL(t *testing.T) {
	logs := captureLogs(t)
	f := newAssetHerderFixture(t, "s3")
	f.herder.assetPlugin = fakeAssetClient{err: status.Error(codes.PermissionDenied, "denied SECRET")}
	_, err := f.herder.fetchAsset(t.Context(), assetEntry{URL: "https://acct.blob.example/assets/vmlinux?sig=SECRET", SHA256: strings.Repeat("a", 64)})
	if err == nil {
		t.Fatal("fetchAsset succeeded")
	}
	var line string
	for l := range strings.Lines(logs.String()) {
		if strings.Contains(l, "Object-store plugin failed to fetch sandbox asset") {
			line = l
		}
	}
	if line == "" {
		t.Fatalf("no plugin failure log line:\n%s", logs)
	}
	for _, want := range []string{`"url":"https://acct.blob.example/assets/vmlinux"`, `"code":"PermissionDenied"`} {
		if !strings.Contains(line, want) {
			t.Errorf("log line missing %s:\n%s", want, line)
		}
	}
	if strings.Contains(line, "SECRET") || strings.Contains(line, "denied") {
		t.Errorf("log line carries the plugin's status text:\n%s", line)
	}
}

func TestRejectAteletStorageEnv(t *testing.T) {
	t.Setenv("ATE_STORAGE_BACKEND", "")
	os.Unsetenv("ATE_STORAGE_BACKEND")
	if err := rejectStorageEnv(); err != nil {
		t.Errorf("rejectStorageEnv() with the variable unset = %v", err)
	}
	for _, v := range []string{"s3", "gcs", ""} {
		os.Setenv("ATE_STORAGE_BACKEND", v)
		if err := rejectStorageEnv(); err == nil || !strings.Contains(err.Error(), "objectstore-plugin sidecar") {
			t.Errorf("rejectStorageEnv() with %q = %v, want an error naming the sidecar", v, err)
		}
	}
}

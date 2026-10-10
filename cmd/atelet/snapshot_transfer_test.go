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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// newPluginHerder returns an AteomHerder whose snapshots go through the
// object-store plugin backed by store.
func newPluginHerder(t *testing.T, store objectstorage.ObjectStorage) *AteomHerder {
	t.Helper()
	// Test directories come from t.TempDir, so admit any absolute path.
	plugin, err := objectstoreplugin.NewNodePlugin(store, "/")
	if err != nil {
		t.Fatal(err)
	}
	return &AteomHerder{
		snapshotPlugin:     objectstoreplugintest.NodeClient(plugin),
		snapshotScratchDir: t.TempDir(),
	}
}

// orderedObjectStorage records the order objects are stored in.
type orderedObjectStorage struct {
	mu   sync.Mutex
	puts []string
	data map[string][]byte
}

func (o *orderedObjectStorage) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.puts = append(o.puts, object)
	if o.data == nil {
		o.data = map[string][]byte{}
	}
	o.data[bucket+"/"+object] = b
	return nil
}

func (o *orderedObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.data[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", objectstorage.ErrObjectNotFound, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func TestUploadSnapshotManifestLast(t *testing.T) {
	store := &orderedObjectStorage{}
	s := newPluginHerder(t, store)
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	rec := sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: testPauseImage, SnapshotFiles: []string{"a", "b", "c"}}
	for _, f := range rec.SnapshotFiles {
		if err := os.WriteFile(dir+"/"+f, []byte(f), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.uploadSnapshot(context.Background(), uri, dir, &rec, "ate-demo", "counter"); err != nil {
		t.Fatalf("uploadSnapshot: %v", err)
	}
	if len(store.puts) != 4 {
		t.Fatalf("puts = %v, want 3 files then the manifest", store.puts)
	}
	for i, p := range store.puts {
		isManifest := strings.HasSuffix(p, "/"+sandboxManifestName)
		if isManifest != (i == len(store.puts)-1) {
			t.Fatalf("puts = %v, want the manifest last and only last", store.puts)
		}
	}

	// The manifest round-trips, and scratch space is cleaned up.
	got, err := s.fetchManifest(context.Background(), uri.String())
	if err != nil {
		t.Fatalf("fetchManifest: %v", err)
	}
	back, err := unmarshalSandboxRecord(got)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(back.SnapshotFiles, rec.SnapshotFiles) {
		t.Errorf("manifest files = %v, want %v", back.SnapshotFiles, rec.SnapshotFiles)
	}
	if entries, _ := os.ReadDir(s.snapshotScratchDir); len(entries) != 0 {
		t.Errorf("scratch dir not cleaned up: %v", entries)
	}
}

func TestSnapshotTransferSkipsEmptyFileLists(t *testing.T) {
	store := &orderedObjectStorage{}
	s := newPluginHerder(t, store)
	if err := s.downloadExternalCheckpoint(context.Background(), pausedSnapshotURI, t.TempDir(), nil); err != nil {
		t.Errorf("downloadExternalCheckpoint(no files) = %v, want nil", err)
	}
	if err := s.uploadSnapshotFiles(context.Background(), pausedSnapshotURI, t.TempDir(), nil); err != nil {
		t.Errorf("uploadSnapshotFiles(no files) = %v, want nil", err)
	}
	if len(store.puts) != 0 {
		t.Errorf("objects stored = %v, want none", store.puts)
	}
}

// failingNodePlugin fails every call with err.
type failingNodePlugin struct {
	err error
}

func (p failingNodePlugin) FetchSnapshot(context.Context, *objectstorev1.FetchSnapshotRequest, ...grpc.CallOption) (*objectstorev1.FetchSnapshotResponse, error) {
	return nil, p.err
}

func (p failingNodePlugin) UploadSnapshot(context.Context, *objectstorev1.UploadSnapshotRequest, ...grpc.CallOption) (*objectstorev1.UploadSnapshotResponse, error) {
	return nil, p.err
}

// A snapshot plugin that cannot be reached reaches ate-api-server as
// Unavailable, which it retries, rather than Internal, which crashes the
// actor. The upload after CheckpointWorkload is the exception: a retried
// Checkpoint cannot redo it, so it stays Internal. Other plugin errors reach
// ate-api-server as Internal.
func TestSnapshotPluginErrorCodeThroughAtelet(t *testing.T) {
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	rec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    testPauseImage,
		SnapshotFiles: []string{"a"},
		Fidelity:      ateattr.SnapshotFidelityMemory,
	}
	transfers := []struct {
		name      string
		retryable bool
		run       func(ctx context.Context, s *AteomHerder) error
	}{
		{"download", true, func(ctx context.Context, s *AteomHerder) error {
			return s.downloadExternalCheckpoint(ctx, pausedSnapshotURI, t.TempDir(), []string{"a"})
		}},
		{"paused upload", true, func(ctx context.Context, s *AteomHerder) error {
			dir := t.TempDir()
			writeLocalSnapshot(t, dir, rec, map[string]string{"a": "a"})
			_, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), dir, uri)
			return err
		}},
		{"checkpoint upload", false, func(ctx context.Context, s *AteomHerder) error {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "a"), []byte("a"), 0o600); err != nil {
				return err
			}
			return s.uploadExternalCheckpoint(ctx, validCheckpointRequest(), dir, &rec)
		}},
	}
	for _, pluginCode := range []codes.Code{codes.Unavailable, codes.Internal, codes.NotFound} {
		for _, tr := range transfers {
			t.Run(pluginCode.String()+"/"+tr.name, func(t *testing.T) {
				want := codes.Internal
				if pluginCode == codes.Unavailable && tr.retryable {
					want = codes.Unavailable
				}
				s := &AteomHerder{
					snapshotPlugin:     failingNodePlugin{err: status.Error(pluginCode, "plugin failed")},
					snapshotScratchDir: t.TempDir(),
				}
				// atelet's server interceptor decides the code ate-api-server sees.
				_, err := ateinterceptors.InternalServerUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"},
					func(ctx context.Context, _ any) (any, error) { return nil, tr.run(ctx, s) })
				if got := status.Code(err); got != want {
					t.Errorf("code = %s (%v), want %s", got, err, want)
				}
			})
		}
	}
}

// A snapshot plugin's status text can quote a signed URL or other credential.
// Neither it nor the snapshot URI's query reaches atelet's RPC log or the
// error ate-api-server receives: only the plugin's code and the snapshot URI
// as scheme://host/path do, and callers still read the code.
func TestSnapshotPluginErrorKeepsCredentialsOut(t *testing.T) {
	const (
		secret    = "SECRET-SIG"
		signedURI = "s3://bucket/snapshots/a?sig=" + secret
		shownURI  = "s3://bucket/snapshots/a"
	)
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}
	rec := sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: testPauseImage, SnapshotFiles: []string{"a"}, Fidelity: ateattr.SnapshotFidelityMemory}
	transfers := []struct {
		name string
		uri  string // the URI the transfer names in its error, when it takes one
		run  func(ctx context.Context, s *AteomHerder) error
	}{
		{"download", signedURI, func(ctx context.Context, s *AteomHerder) error {
			return s.downloadExternalCheckpoint(ctx, signedURI, t.TempDir(), []string{"a"})
		}},
		{"upload", signedURI, func(ctx context.Context, s *AteomHerder) error {
			return s.uploadSnapshotFiles(ctx, signedURI, t.TempDir(), []string{"a"})
		}},
		{"manifest fetch", signedURI, func(ctx context.Context, s *AteomHerder) error {
			_, err := s.fetchManifest(ctx, signedURI)
			return err
		}},
		{"paused upload", "", func(ctx context.Context, s *AteomHerder) error {
			dir := t.TempDir()
			writeLocalSnapshot(t, dir, rec, map[string]string{"a": "a"})
			_, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), dir, uri)
			return err
		}},
	}
	for _, pluginCode := range []codes.Code{codes.Unavailable, codes.Internal, codes.NotFound} {
		for _, tr := range transfers {
			t.Run(pluginCode.String()+"/"+tr.name, func(t *testing.T) {
				logs := captureLogs(t)
				s := &AteomHerder{
					snapshotPlugin:     failingNodePlugin{err: status.Error(pluginCode, "get "+signedURI+": AccountKey="+secret)},
					snapshotScratchDir: t.TempDir(),
				}
				_, err := ateinterceptors.InternalServerUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"},
					func(ctx context.Context, _ any) (any, error) { return nil, tr.run(ctx, s) })
				if err == nil {
					t.Fatal("transfer succeeded")
				}
				for name, text := range map[string]string{"error": err.Error(), "logs": logs.String()} {
					for _, leak := range []string{secret, "sig=", "AccountKey"} {
						if strings.Contains(text, leak) {
							t.Errorf("%s carries %q:\n%s", name, leak, text)
						}
					}
				}
				if tr.uri != "" && !strings.Contains(err.Error(), shownURI) {
					t.Errorf("error %q does not name the snapshot as %s", err, shownURI)
				}
			})
		}
	}
}

// The plugin's code survives the message being dropped.
func TestPluginCallErrorKeepsCode(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.NotFound, codes.Internal, codes.Canceled} {
		err := pluginCallError("fetch", "s3://b/o?sig=x", status.Error(code, "secret"))
		if got := status.Code(err); got != code {
			t.Errorf("code = %s, want %s", got, code)
		}
	}
	if err := pluginCallError("fetch", "s3://b/o", nil); err != nil {
		t.Errorf("pluginCallError(nil) = %v", err)
	}
	if got := status.Code(pluginCallError("fetch", "s3://b/o", context.DeadlineExceeded)); got != codes.DeadlineExceeded {
		t.Errorf("code for a context error = %s, want %s", got, codes.DeadlineExceeded)
	}
}

// pluginSidecar serves a NodePlugin on a Unix socket and can be stopped and
// started again on the same path, as kubelet restarts a sidecar container.
type pluginSidecar struct {
	t      *testing.T
	path   string
	plugin *objectstoreplugin.NodePlugin

	mu  sync.Mutex
	srv *grpc.Server
}

func newPluginSidecar(t *testing.T, store objectstorage.ObjectStorage) *pluginSidecar {
	t.Helper()
	plugin, err := objectstoreplugin.NewNodePlugin(store, "/")
	if err != nil {
		t.Fatal(err)
	}
	// Unix socket paths are length-limited; t.TempDir can exceed that on macOS.
	dir, err := os.MkdirTemp("", "snapplug")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := &pluginSidecar{t: t, path: filepath.Join(dir, "plugin.sock"), plugin: plugin}
	t.Cleanup(s.stop)
	return s
}

// start may run on any goroutine, so it reports failures with Error.
func (s *pluginSidecar) start() {
	lis, err := objectstoreplugin.Listen(s.path)
	if err != nil {
		s.t.Errorf("Listen: %v", err)
		return
	}
	srv := grpc.NewServer()
	objectstorev1.RegisterNodeProviderServer(srv, s.plugin)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()
}

// stop ends the server and every connection to it at once, as a crashed
// sidecar does.
func (s *pluginSidecar) stop() {
	s.mu.Lock()
	srv := s.srv
	s.srv = nil
	s.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
}

// The upload after CheckpointWorkload waits out a plugin outage longer than
// the ready wait, since a retried Checkpoint cannot redo it. The paused
// upload, which a retry can redo, still fails within the ready wait.
func TestCheckpointUploadWaitsOutPluginOutage(t *testing.T) {
	const wait = 200 * time.Millisecond
	store := &orderedObjectStorage{}
	sidecar := newPluginSidecar(t, store)
	sidecar.start()
	conn, err := objectstoreplugin.Dial(sidecar.path, wait)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := objectstoreplugin.WaitReady(ctx, conn); err != nil {
		t.Fatalf("WaitReady = %v", err)
	}
	s := &AteomHerder{
		snapshotPlugin:     objectstorev1.NewNodeProviderClient(conn),
		snapshotScratchDir: t.TempDir(),
	}
	rec := sandboxAssetsRecord{
		SandboxClass:  "gvisor",
		PauseImage:    testPauseImage,
		SnapshotFiles: []string{"a"},
		Fidelity:      ateattr.SnapshotFidelityMemory,
	}
	pausedDir := t.TempDir()
	writeLocalSnapshot(t, pausedDir, rec, map[string]string{"a": "a"})
	checkpointDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(checkpointDir, "a"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	uri, err := resources.ParseSnapshotURI(pausedSnapshotURI)
	if err != nil {
		t.Fatal(err)
	}

	// Lose the plugin, and wait until the connection has seen it go so the
	// uploads start on a lost connection rather than racing the loss.
	sidecar.stop()
	for conn.GetState() == connectivity.Ready {
		if !conn.WaitForStateChange(ctx, connectivity.Ready) {
			t.Fatal("connection still ready after the plugin stopped")
		}
	}

	if _, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), pausedDir, uri); apierror.Code(err) != codes.Unavailable {
		t.Fatalf("paused upload during the outage = %v, want %s", err, codes.Unavailable)
	}

	const outage = 10 * wait
	restarted := make(chan struct{})
	go func() {
		defer close(restarted)
		time.Sleep(outage)
		sidecar.start()
	}()
	defer func() { <-restarted }()

	start := time.Now()
	if err := s.uploadExternalCheckpoint(ctx, validCheckpointRequest(), checkpointDir, &rec); err != nil {
		t.Fatalf("checkpoint upload across a plugin outage = %v, want success", err)
	}
	if elapsed := time.Since(start); elapsed < outage/2 {
		t.Errorf("checkpoint upload succeeded after %s, want it to have waited out the %s outage", elapsed, outage)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	object := strings.TrimPrefix(testSnapshotPath, "bucket/")
	if want := []string{object + "/a.zstd", object + "/" + sandboxManifestName}; !slices.Equal(store.puts, want) {
		t.Errorf("objects stored = %v, want %v", store.puts, want)
	}
}

// fetchManifest keeps the plugin's NotFound readable, which the paused
// snapshot upload uses to tell a missing snapshot from a failed probe.
func TestFetchManifestKeepsNotFound(t *testing.T) {
	s := &AteomHerder{
		snapshotPlugin:     failingNodePlugin{err: status.Error(codes.NotFound, "no manifest")},
		snapshotScratchDir: t.TempDir(),
	}
	_, err := s.fetchManifest(context.Background(), pausedSnapshotURI)
	if status.Code(err) != codes.NotFound {
		t.Errorf("fetchManifest = %v, want %s", err, codes.NotFound)
	}
}

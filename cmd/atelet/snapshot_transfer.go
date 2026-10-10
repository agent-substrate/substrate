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
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/objectstoreplugin"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// External snapshots move between the node and storage only through the
// snapshot plugin. These helpers adapt the plugin's file-oriented API to the
// manifest bytes atelet works with.

// pluginCallError replaces a snapshot plugin's error with one that keeps its
// gRPC code and drops its message. The message may quote a signed URL or other
// credential, and atelet's errors reach ate-api-server, its RPC log and the
// actor's crash message, so only the code and the redacted snapshot URI are
// reported. Callers read the code: NotFound means no such snapshot, and
// Unavailable is retried.
func pluginCallError(op, snapshotURI string, err error) error {
	if err == nil {
		return nil
	}
	code := status.Code(err)
	if code == codes.Unknown {
		code = status.FromContextError(err).Code() // a context error that is not a status
	}
	return status.Errorf(code, "snapshot plugin %s of %s failed (%s)", op, redactURLString(snapshotURI), code)
}

// fetchSnapshotFiles downloads the named snapshot files into dstDir.
func (s *AteomHerder) fetchSnapshotFiles(ctx context.Context, snapshotURI, dstDir string, files []string) error {
	if len(files) == 0 {
		return nil
	}
	_, err := s.snapshotPlugin.FetchSnapshot(ctx, &objectstorev1.FetchSnapshotRequest{
		SnapshotUri: snapshotURI,
		WritePath:   dstDir,
		Files:       files,
	})
	return objectstoreplugin.CallError(pluginCallError("fetch", snapshotURI, err))
}

// uploadSnapshotFiles uploads the named files in srcDir to the snapshot,
// passing opts to the plugin call. It returns the plugin's code without its
// message, and leaves the Unavailable mapping to the caller: an upload that can
// be retried reports it through objectstoreplugin.CallError, one that cannot
// does not.
func (s *AteomHerder) uploadSnapshotFiles(ctx context.Context, snapshotURI, srcDir string, files []string, opts ...grpc.CallOption) error {
	if len(files) == 0 {
		return nil
	}
	_, err := s.snapshotPlugin.UploadSnapshot(ctx, &objectstorev1.UploadSnapshotRequest{
		SnapshotUri: snapshotURI,
		LocalPath:   srcDir,
		Files:       files,
	}, opts...)
	return pluginCallError("upload", snapshotURI, err)
}

// fetchManifest returns a snapshot's manifest. The returned error is the
// plugin's gRPC status, so a missing manifest reports codes.NotFound.
func (s *AteomHerder) fetchManifest(ctx context.Context, snapshotURI string) ([]byte, error) {
	dir, err := s.snapshotScratch()
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := s.fetchSnapshotFiles(ctx, snapshotURI, dir, []string{sandboxManifestName}); err != nil {
		return nil, err
	}
	return os.ReadFile(filepath.Join(dir, sandboxManifestName))
}

// uploadManifest uploads manifest as the snapshot's manifest, as
// uploadSnapshotFiles does.
func (s *AteomHerder) uploadManifest(ctx context.Context, snapshotURI string, manifest []byte, opts ...grpc.CallOption) error {
	dir, err := s.snapshotScratch()
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if err := os.WriteFile(filepath.Join(dir, sandboxManifestName), manifest, 0o600); err != nil {
		return fmt.Errorf("while staging snapshot manifest: %w", err)
	}
	return s.uploadSnapshotFiles(ctx, snapshotURI, dir, []string{sandboxManifestName}, opts...)
}

// snapshotScratch creates a fresh directory for one manifest transfer. The
// caller removes it.
func (s *AteomHerder) snapshotScratch() (string, error) {
	if err := os.MkdirAll(s.snapshotScratchDir, 0o700); err != nil {
		return "", fmt.Errorf("while creating snapshot scratch dir: %w", err)
	}
	dir, err := os.MkdirTemp(s.snapshotScratchDir, "manifest-")
	if err != nil {
		return "", fmt.Errorf("while creating snapshot scratch dir: %w", err)
	}
	return dir, nil
}

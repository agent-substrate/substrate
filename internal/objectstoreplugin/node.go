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

// Package objectstoreplugin implements the snapshot plugin API on GCS or S3
// object storage, using the plugin's own credentials.
//
// Snapshot files are stored as zstd-compressed objects named <file>.zstd
// under the snapshot URI, except the manifest, which is stored as is. This is
// the layout atelet has always written, so snapshots taken before the plugin
// existed stay readable.
package objectstoreplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstoresnapshotv1 "github.com/agent-substrate/substrate/pkg/proto/objectstoresnapshotpb/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This plugin stores each file as <name>.zstd, except manifestFile, which it
// stores as-is so that snapshots written before atelet used this plugin stay
// readable.
const manifestFile = "manifest.json"

// NodePlugin serves NodeProvider on an object storage client.
type NodePlugin struct {
	objectstoresnapshotv1.UnimplementedNodeProviderServer

	gcsClient objectstorage.ObjectStorage
	// root confines every local path a caller names.
	root string
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root.
func NewNodePlugin(gcsClient objectstorage.ObjectStorage, root string) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	return &NodePlugin{gcsClient: gcsClient, root: filepath.Clean(root)}, nil
}

// FetchSnapshot downloads the requested snapshot files into write_path.
func (p *NodePlugin) FetchSnapshot(ctx context.Context, req *objectstoresnapshotv1.FetchSnapshotRequest) (*objectstoresnapshotv1.FetchSnapshotResponse, error) {
	uri, dstDir, err := p.validate(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(dstDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening restore directory: %w", err))
	}
	defer root.Close()

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		fileName := fileName
		g.Go(func() error {
			if fileName == manifestFile {
				return p.fetchManifest(gCtx, uri, root)
			}
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			local, err := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				return fmt.Errorf("while opening %s in restore directory: %w", fileName, err)
			}
			fetchErr := objectstorage.FetchFileFromGCSWithZstd(gCtx, p.gcsClient, objectURI, local)
			closeErr := local.Close()
			if err := errors.Join(fetchErr, closeErr); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	return &objectstoresnapshotv1.FetchSnapshotResponse{}, nil
}

// UploadSnapshot uploads the requested files from local_path into the
// snapshot.
func (p *NodePlugin) UploadSnapshot(ctx context.Context, req *objectstoresnapshotv1.UploadSnapshotRequest) (*objectstoresnapshotv1.UploadSnapshotResponse, error) {
	uri, srcDir, err := p.validate(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(srcDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening snapshot directory: %w", err))
	}
	defer root.Close()

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		g.Go(func() error {
			local, err := root.Open(fileName)
			if err != nil {
				return fmt.Errorf("while opening %s in snapshot directory: %w", fileName, err)
			}
			defer local.Close()
			info, err := local.Stat()
			if err != nil {
				return fmt.Errorf("while inspecting %s in snapshot directory: %w", fileName, err)
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("snapshot file %s is not a regular file", fileName)
			}
			if fileName == manifestFile {
				return p.uploadManifest(gCtx, uri, local)
			}

			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if err := objectstorage.SendFileToGCSWithZstd(gCtx, p.gcsClient, objectURI, local); err != nil {
				return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	return &objectstoresnapshotv1.UploadSnapshotResponse{}, nil
}

// fetchManifest downloads the uncompressed manifest into root.
func (p *NodePlugin) fetchManifest(ctx context.Context, uri resources.SnapshotURI, root *os.Root) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return err
	}
	manifest, err := objectstorage.FetchFromGCS(ctx, p.gcsClient, manifestURI)
	if err != nil {
		return fmt.Errorf("while fetching snapshot manifest: %w", err)
	}
	if err := root.WriteFile(manifestFile, manifest, 0o600); err != nil {
		return fmt.Errorf("while writing %s in restore directory: %w", manifestFile, err)
	}
	return nil
}

// uploadManifest uploads the manifest from local uncompressed.
func (p *NodePlugin) uploadManifest(ctx context.Context, uri resources.SnapshotURI, local *os.File) error {
	manifest, err := io.ReadAll(local)
	if err != nil {
		return fmt.Errorf("while reading %s in snapshot directory: %w", manifestFile, err)
	}
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	if err := objectstorage.SendBytesToGCS(ctx, p.gcsClient, manifestURI, manifest); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// validate checks a request's snapshot URI, local directory and file names,
// returning the parsed URI and the local directory relative to p.root.
func (p *NodePlugin) validate(snapshotURI, dir string, files []string) (resources.SnapshotURI, string, error) {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	if !filepath.IsAbs(dir) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is not absolute", dir)
	}
	rel, err := filepath.Rel(p.root, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return resources.SnapshotURI{}, "", status.Errorf(codes.InvalidArgument, "local path %q is outside %q", dir, p.root)
	}
	if len(files) == 0 {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, "no files requested")
	}
	if err := resources.ValidateSnapshotFileNames(files); err != nil {
		return resources.SnapshotURI{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	return uri, rel, nil
}

// openDir opens rel, a directory below p.root, through an os.Root at p.root,
// so a symlink anywhere on the path cannot lead outside p.root.
func (p *NodePlugin) openDir(rel string) (*os.Root, error) {
	base, err := os.OpenRoot(p.root)
	if err != nil {
		return nil, err
	}
	defer base.Close()
	return base.OpenRoot(rel)
}

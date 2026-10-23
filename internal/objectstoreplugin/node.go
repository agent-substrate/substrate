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

// Package objectstoreplugin implements the object-store plugin API on GCS or S3
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
	"slices"

	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/objectstorage"
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
	objectstore.UnimplementedNodeProviderServer

	// creds supplies the client each transfer runs with.
	creds    Credentials
	uploader *objectstorage.Uploader
	// root confines every local path a caller names.
	root string
}

// NewNodePlugin returns a NodePlugin that reads and writes local files only
// below root. All upload requests share uploadMemory bytes of reservations for
// compression and backend buffers. client is the pod's own storage client,
// GCS or S3 as the environment selected. It sizes that budget and serves
// every transfer unless WithCredentials supplies a per-actor one.
func NewNodePlugin(client objectstorage.ObjectStorage, root string, uploadMemory int64, opts ...NodeOption) (*NodePlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	uploader, err := objectstorage.NewUploader(client, uploadMemory)
	if err != nil {
		return nil, err
	}
	p := &NodePlugin{creds: Ambient(client), uploader: uploader, root: filepath.Clean(root)}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// NodeOption configures a NodePlugin.
type NodeOption func(*NodePlugin)

// WithCredentials makes transfers run with the client creds supplies for the
// actor JWT each request carries, instead of the ambient client.
func WithCredentials(creds Credentials) NodeOption {
	return func(p *NodePlugin) { p.creds = creds }
}

// FetchSnapshot downloads the requested snapshot files into write_path.
func (p *NodePlugin) FetchSnapshot(ctx context.Context, req *objectstore.FetchSnapshotRequest) (*objectstore.FetchSnapshotResponse, error) {
	uri, dstDir, err := p.validate(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(dstDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening restore directory: %w", err))
	}
	defer root.Close()
	tr := p.begin(req.GetActorJwt())

	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		fileName := fileName
		g.Go(func() error {
			if fileName == manifestFile {
				return p.fetchManifest(gCtx, tr, uri, root)
			}
			objectURI, err := uri.ObjectURI(fileName + ".zstd")
			if err != nil {
				return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
			}
			if dir := filepath.Dir(fileName); dir != "." {
				if err := root.MkdirAll(dir, 0o700); err != nil {
					return fmt.Errorf("while creating %s in restore directory: %w", dir, err)
				}
			}
			local, err := root.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
			if err != nil {
				return fmt.Errorf("while opening %s in restore directory: %w", fileName, err)
			}
			fetchErr := tr.do(gCtx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) error {
				if err := truncate(local); err != nil {
					return err
				}
				return objectstorage.FetchFileFromGCSWithZstd(gCtx, client, objectURI, local)
			})
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
	return &objectstore.FetchSnapshotResponse{}, nil
}

// UploadSnapshot uploads the requested files from local_path into the
// snapshot.
func (p *NodePlugin) UploadSnapshot(ctx context.Context, req *objectstore.UploadSnapshotRequest) (*objectstore.UploadSnapshotResponse, error) {
	uri, srcDir, err := p.validate(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := p.openDir(srcDir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening snapshot directory: %w", err))
	}
	defer root.Close()
	tr := p.begin(req.GetActorJwt())

	upload := func(ctx context.Context, fileName string) error {
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
			return p.uploadManifest(ctx, tr, uri, local)
		}
		objectURI, err := uri.ObjectURI(fileName + ".zstd")
		if err != nil {
			return fmt.Errorf("while addressing %s in GCS: %w", fileName, err)
		}
		err = tr.do(ctx, func(_ objectstorage.ObjectStorage, uploader *objectstorage.Uploader) error {
			if err := rewind(local); err != nil {
				return err
			}
			return uploader.SendFile(ctx, objectURI, local)
		})
		if err != nil {
			return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
		}
		return nil
	}
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range req.GetFiles() {
		if fileName != manifestFile {
			g.Go(func() error { return upload(gCtx, fileName) })
		}
	}
	if err := g.Wait(); err != nil {
		return nil, toStatus(err)
	}
	// A manifest commits the snapshot only after every requested data file has
	// succeeded. Use ctx: errgroup cancels gCtx when Wait returns.
	if slices.Contains(req.GetFiles(), manifestFile) {
		if err := upload(ctx, manifestFile); err != nil {
			return nil, toStatus(err)
		}
	}
	return &objectstore.UploadSnapshotResponse{}, nil
}

// fetchManifest downloads the uncompressed manifest into root.
func (p *NodePlugin) fetchManifest(ctx context.Context, tr *transfer, uri resources.SnapshotURI, root *os.Root) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return err
	}
	var manifest []byte
	err = tr.do(ctx, func(client objectstorage.ObjectStorage, _ *objectstorage.Uploader) (err error) {
		manifest, err = objectstorage.FetchFromGCS(ctx, client, manifestURI)
		return err
	})
	if err != nil {
		return fmt.Errorf("while fetching snapshot manifest: %w", err)
	}
	if err := root.WriteFile(manifestFile, manifest, 0o600); err != nil {
		return fmt.Errorf("while writing %s in restore directory: %w", manifestFile, err)
	}
	return nil
}

// uploadManifest uploads the manifest from local uncompressed.
func (p *NodePlugin) uploadManifest(ctx context.Context, tr *transfer, uri resources.SnapshotURI, local *os.File) error {
	manifestURI, err := uri.ObjectURI(manifestFile)
	if err != nil {
		return fmt.Errorf("while addressing snapshot manifest in GCS: %w", err)
	}
	err = tr.do(ctx, func(_ objectstorage.ObjectStorage, uploader *objectstorage.Uploader) error {
		if err := rewind(local); err != nil {
			return err
		}
		return uploader.Send(ctx, manifestURI, local)
	})
	if err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

// rewind puts an upload's source back at its start. A request storage
// refuses may already have read the file, and the retry must send all of it
// rather than what is left.
func rewind(f *os.File) error {
	_, err := f.Seek(0, io.SeekStart)
	return err
}

// truncate empties a download's destination, so a retry does not append to
// whatever a refused attempt wrote.
func truncate(f *os.File) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	return rewind(f)
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

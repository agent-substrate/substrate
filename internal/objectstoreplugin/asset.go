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
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/objectstorage"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// sha256Pattern matches the SandboxConfig's sha256 format.
var sha256Pattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// AssetPlugin serves AssetProvider on an object storage client. It reads
// gs:// and s3:// URIs as <bucket>/<object> on the configured backend, the way
// it reads snapshot URIs.
//
// It downloads each asset into stagingDir, which must be private to the
// plugin, and copies it to the caller's file only once its size and sha256
// check out. The caller can therefore never read bytes it did not pin, such as
// a snapshot's.
type AssetPlugin struct {
	objectstorev1.UnimplementedAssetProviderServer

	client objectstorage.ObjectStorage
	// root confines every local path a caller names.
	root string
	// stagingDir holds downloads until they are verified.
	stagingDir string
}

// NewAssetPlugin returns an AssetPlugin that writes caller files only below
// root and stages downloads in stagingDir, which must not be below root.
func NewAssetPlugin(client objectstorage.ObjectStorage, root, stagingDir string) (*AssetPlugin, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	if !filepath.IsAbs(stagingDir) {
		return nil, fmt.Errorf("asset staging directory %q is not an absolute path", stagingDir)
	}
	root, stagingDir = filepath.Clean(root), filepath.Clean(stagingDir)
	if rel, err := filepath.Rel(root, stagingDir); err == nil && (rel == "." || filepath.IsLocal(rel)) {
		return nil, fmt.Errorf("asset staging directory %q is inside root %q, where the caller could read unverified downloads", stagingDir, root)
	}
	return &AssetPlugin{client: client, root: root, stagingDir: stagingDir}, nil
}

// FetchAsset downloads the object at asset_uri into the staging directory and,
// if it is at most max_bytes long and hashes to sha256, copies it into the
// existing file at write_path.
func (p *AssetPlugin) FetchAsset(ctx context.Context, req *objectstorev1.FetchAssetRequest) (*objectstorev1.FetchAssetResponse, error) {
	bucket, object, err := parseAssetURI(req.GetAssetUri())
	if err != nil {
		return nil, err
	}
	// The URI carries no user info or query from here on, so messages may
	// name it.
	uri := req.GetAssetUri()
	if !sha256Pattern.MatchString(req.GetSha256()) {
		return nil, status.Errorf(codes.InvalidArgument, "sha256 %q is not 64 lower-case hex characters", req.GetSha256())
	}
	wantSum, _ := hex.DecodeString(req.GetSha256())
	if req.GetMaxBytes() <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "max_bytes %d is not positive", req.GetMaxBytes())
	}
	rel, err := p.writePath(req.GetWritePath())
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(p.root)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening %s: %w", p.root, err))
	}
	defer root.Close()
	// Checked before downloading so a bad request costs no transfer. Lstat:
	// a symlink is never the file the caller created, a directory cannot be
	// opened for writing, and opening a FIFO would block.
	if err := checkWriteFile(root, rel, req.GetWritePath()); err != nil {
		return nil, err
	}

	staged, err := p.stage(ctx, bucket, object, uri, req.GetMaxBytes(), wantSum)
	if err != nil {
		return nil, err
	}
	defer func() {
		staged.Close()
		os.Remove(staged.Name())
	}()

	// No O_CREATE: the caller owns the file's lifetime, so a request that
	// outlives its caller cannot recreate a file the caller already removed.
	dst, err := root.OpenFile(rel, os.O_WRONLY|os.O_TRUNC, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, status.Errorf(codes.FailedPrecondition, "write path %q does not exist", req.GetWritePath())
	}
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening write path %q: %w", req.GetWritePath(), err))
	}
	defer dst.Close()
	if _, err := io.Copy(dst, staged); err != nil {
		return nil, toStatus(fmt.Errorf("while writing asset %s to %q: %w", uri, req.GetWritePath(), err))
	}
	if err := dst.Close(); err != nil {
		return nil, toStatus(fmt.Errorf("while closing write path %q: %w", req.GetWritePath(), err))
	}
	return &objectstorev1.FetchAssetResponse{}, nil
}

// stage downloads bucket/object into a new file in the staging directory and
// returns it, positioned at its start, once its size and sha256 check out.
// The file is removed on error; on success the caller removes it.
func (p *AssetPlugin) stage(ctx context.Context, bucket, object, uri string, maxBytes int64, wantSum []byte) (_ *os.File, retErr error) {
	f, err := os.CreateTemp(p.stagingDir, "asset-")
	if err != nil {
		return nil, toStatus(fmt.Errorf("while creating staging file: %w", err))
	}
	defer func() {
		if retErr != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	rc, err := p.client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, toStatus(fmt.Errorf("while opening asset %s: %w", uri, err))
	}
	defer rc.Close()
	hasher := sha256.New()
	// +1 lets an over-cap object trip n > maxBytes without staging all of it.
	n, err := io.Copy(io.MultiWriter(f, hasher), io.LimitReader(rc, maxBytes+1))
	if err != nil {
		return nil, toStatus(fmt.Errorf("while downloading asset %s: %w", uri, err))
	}
	if n > maxBytes {
		return nil, status.Errorf(codes.FailedPrecondition, "asset %s is larger than max_bytes %d", uri, maxBytes)
	}
	if got := hasher.Sum(nil); !bytes.Equal(got, wantSum) {
		return nil, status.Errorf(codes.FailedPrecondition, "asset %s has sha256 %x, want %x", uri, got, wantSum)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, toStatus(fmt.Errorf("while rewinding staging file: %w", err))
	}
	return f, nil
}

// writePath checks that path is absolute and below p.root, and returns it
// relative to p.root.
func (p *AssetPlugin) writePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", status.Errorf(codes.InvalidArgument, "write path %q is not absolute", path)
	}
	rel, err := filepath.Rel(p.root, path)
	if err != nil || !filepath.IsLocal(rel) {
		return "", status.Errorf(codes.InvalidArgument, "write path %q is not below %q", path, p.root)
	}
	return rel, nil
}

// checkWriteFile requires rel, below root, to be an existing regular file.
func checkWriteFile(root *os.Root, rel, shown string) error {
	info, err := root.Lstat(rel)
	if errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.FailedPrecondition, "write path %q does not exist", shown)
	}
	if err != nil {
		return toStatus(fmt.Errorf("while inspecting write path %q: %w", shown, err))
	}
	if !info.Mode().IsRegular() {
		return status.Errorf(codes.FailedPrecondition, "write path %q is not a regular file (mode %v)", shown, info.Mode().Type())
	}
	return nil
}

// parseAssetURI accepts gs://<bucket>/<object> and s3://<bucket>/<object>
// whose object name is in canonical form and is not stored under a snapshot
// location, and returns the bucket and object name.
func parseAssetURI(uri string) (bucket, object string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", status.Error(codes.InvalidArgument, "asset URI is not a valid URI")
	}
	// Checked first so later messages can name the URI: user info or a query
	// may carry credentials.
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return "", "", status.Error(codes.InvalidArgument, "asset URI must not carry user info, a query or a fragment")
	}
	if u.Scheme != "gs" && u.Scheme != "s3" {
		return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s: scheme %q is not served by this plugin (want gs or s3)", uri, u.Scheme)
	}
	object = strings.TrimPrefix(u.Path, "/")
	if u.Host == "" || object == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s must name a bucket and an object", uri)
	}
	// Snapshot URIs are built with url.JoinPath, so a snapshot object's name
	// is always canonical. A non-canonical name could still resolve to one on
	// a backend that cleans or trims names, past the check below.
	if strings.Contains(object, `\`) {
		return "", "", status.Errorf(codes.InvalidArgument, `asset URI %s must not contain '\'`, uri)
	}
	for seg := range strings.SplitSeq(object, "/") {
		if seg == "" || strings.TrimSpace(seg) != seg || strings.HasSuffix(seg, ".") {
			return "", "", status.Errorf(codes.InvalidArgument, "asset URI %s has an empty, '.'-terminated or space-padded path segment", uri)
		}
	}
	if err := refuseSnapshotLocation(u); err != nil {
		return "", "", err
	}
	return u.Host, object, nil
}

// refuseSnapshotLocation returns PermissionDenied if u, or any prefix of it,
// is a snapshot or tag URI: snapshot files, manifests and tags are never
// assets, however their sha256 came to be known.
func refuseSnapshotLocation(u *url.URL) error {
	segments := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	for i := len(segments); i > 0; i-- {
		prefix := url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/" + strings.Join(segments[:i], "/")}
		if _, err := resources.ParseSnapshotURI(prefix.String()); err == nil {
			return status.Errorf(codes.PermissionDenied, "asset URI %s is stored under snapshot %s; assets must not live under a snapshot location", u.String(), prefix.String())
		}
	}
	return nil
}

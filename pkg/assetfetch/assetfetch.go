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

// Package assetfetch holds the checks every object-store plugin applies when
// it serves AssetProvider.FetchAsset, whatever its storage backend:
//
//   - CheckObjectName refuses an object stored under a snapshot or tag
//     location, before anything is read.
//   - Stager.Fetch downloads into a directory private to the plugin, enforces
//     max_bytes while copying, verifies the sha256, and only then writes the
//     caller's file.
//
// A plugin parses its own URI scheme into an object name and an opener, and
// leaves the rest to this package. Errors are gRPC statuses with the codes
// objectstore.v1 documents for FetchAsset.
package assetfetch

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

	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

// MaxURIBytes bounds asset_uri, as the SandboxConfig CRD bounds
// AssetFile.url, so the checks below stay cheap whatever a SandboxConfig names.
const MaxURIBytes = 2048

// sha256Pattern matches the SandboxConfig's sha256 format.
var sha256Pattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// ParseURI parses an asset URI of at most MaxURIBytes that carries no user
// info, query or fragment and is not opaque. Its errors do not quote the URI,
// since user info or a query may carry credentials; once it succeeds, the URI
// may be named in messages.
func ParseURI(uri string) (*url.URL, error) {
	if len(uri) > MaxURIBytes {
		return nil, status.Errorf(codes.InvalidArgument, "asset URI is %d bytes long, more than the limit of %d", len(uri), MaxURIBytes)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "asset URI is not a valid URI")
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, status.Error(codes.InvalidArgument, "asset URI must not carry user info, a query or a fragment")
	}
	return u, nil
}

// CheckObjectName requires object, the name of the object uri points at
// within its bucket or container, to be in canonical form and not stored
// under a snapshot or tag location. base is uri up to the object name (e.g.
// gs://<bucket>); the error names the snapshot location as base/<prefix>.
//
// Snapshot files, manifests and tags are never assets, however their sha256
// came to be known, so this returns PermissionDenied for them. Every <root>
// a snapshot location may have is matched, so the check holds whatever key
// prefix the plugin stores snapshots under.
func CheckObjectName(uri, base, object string) error {
	if object == "" {
		return status.Errorf(codes.InvalidArgument, "asset URI %s must name an object", uri)
	}
	// Snapshot URIs are built with url.JoinPath, so a snapshot object's name
	// is always canonical. A non-canonical name could still resolve to one on
	// a backend that cleans or trims names, past the check below.
	if strings.Contains(object, `\`) {
		return status.Errorf(codes.InvalidArgument, `asset URI %s must not contain '\'`, uri)
	}
	segments := strings.Split(object, "/")
	for _, seg := range segments {
		if seg == "" || strings.TrimSpace(seg) != seg || strings.HasSuffix(seg, ".") {
			return status.Errorf(codes.InvalidArgument, "asset URI %s has an empty, '.'-terminated or space-padded path segment", uri)
		}
	}
	if n := snapshotPrefixLen(segments); n > 0 {
		return status.Errorf(codes.PermissionDenied, "asset URI %s is stored under snapshot %s/%s; assets must not live under a snapshot location",
			uri, strings.TrimSuffix(base, "/"), strings.Join(segments[:n], "/"))
	}
	return nil
}

// snapshotPrefixLen returns the length of a prefix of segments that names a
// snapshot or a tag, the way resources.ParseSnapshotURI reads one, or 0 if no
// prefix does:
//
//	<root>/atespaces/<atespace>/actors/<uid>/snapshots/<name>
//	<root>/atespaces/<atespace>/tags/<uid>
//
// segments must be canonical (see CheckObjectName), which makes every <root>
// a valid snapshot location. Each segment is read at most a fixed number of
// times, so the cost is linear in the URI's length.
func snapshotPrefixLen(segments []string) int {
	for i, seg := range segments {
		if seg != "atespaces" {
			continue
		}
		owner := segments[i+1:]
		switch {
		case len(owner) >= 3 && owner[1] == "tags" &&
			isResourceName(owner[0]) && isResourceName(owner[2]):
			return i + 4
		case len(owner) >= 5 && owner[1] == "actors" && owner[3] == "snapshots" &&
			isResourceName(owner[0]) && isResourceName(owner[2]) && isResourceName(owner[4]):
			return i + 6
		}
	}
	return 0
}

// isResourceName matches resources.IsValidResourceName, which names every
// atespace, actor UID, snapshot and tag in a snapshot location.
func isResourceName(name string) bool {
	return len(content.IsDNS1123Label(name)) == 0
}

// Stager downloads assets into a staging directory private to the plugin and
// copies each to the caller's file only once its size and sha256 check out.
// The caller can therefore never read bytes it did not pin, such as a
// snapshot's.
type Stager struct {
	// root confines every local path a caller names.
	root string
	// stagingDir holds downloads until they are verified.
	stagingDir string
}

// NewStager returns a Stager that writes caller files only below root and
// stages downloads in stagingDir, which must not be below root.
func NewStager(root, stagingDir string) (*Stager, error) {
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
	return &Stager{root: root, stagingDir: stagingDir}, nil
}

// OpenFunc opens the asset for reading.
type OpenFunc func(ctx context.Context) (io.ReadCloser, error)

// Fetch checks req's sha256, max_bytes and write_path, downloads the asset
// open returns into the staging directory and, if it is at most max_bytes
// long and hashes to sha256, copies it into the existing file at write_path.
// uri is req's asset URI once it is safe to quote (see ParseURI). toStatus
// maps every other error, from open, the download or a local file, to a gRPC
// status; it must keep a context error's code.
//
// The caller checks the URI (ParseURI, CheckObjectName) first, so a refused
// request reads nothing.
func (s *Stager) Fetch(ctx context.Context, req *objectstorev1.FetchAssetRequest, uri string, open OpenFunc, toStatus func(error) error) error {
	if !sha256Pattern.MatchString(req.GetSha256()) {
		return status.Errorf(codes.InvalidArgument, "sha256 %q is not 64 lower-case hex characters", req.GetSha256())
	}
	wantSum, _ := hex.DecodeString(req.GetSha256())
	if req.GetMaxBytes() <= 0 {
		return status.Errorf(codes.InvalidArgument, "max_bytes %d is not positive", req.GetMaxBytes())
	}
	rel, err := s.writePath(req.GetWritePath())
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(s.root)
	if err != nil {
		return toStatus(fmt.Errorf("while opening %s: %w", s.root, err))
	}
	defer root.Close()
	// Checked before downloading so a bad request costs no transfer. Lstat:
	// a symlink is never the file the caller created, a directory cannot be
	// opened for writing, and opening a FIFO would block.
	if err := checkWriteFile(root, rel, req.GetWritePath(), toStatus); err != nil {
		return err
	}

	staged, err := s.stage(ctx, open, uri, req.GetMaxBytes(), wantSum, toStatus)
	if err != nil {
		return err
	}
	defer func() {
		staged.Close()
		os.Remove(staged.Name())
	}()

	// No O_CREATE: the caller owns the file's lifetime, so a request that
	// outlives its caller cannot recreate a file the caller already removed.
	dst, err := root.OpenFile(rel, os.O_WRONLY|os.O_TRUNC, 0)
	if errors.Is(err, os.ErrNotExist) {
		return status.Errorf(codes.FailedPrecondition, "write path %q does not exist", req.GetWritePath())
	}
	if err != nil {
		return toStatus(fmt.Errorf("while opening write path %q: %w", req.GetWritePath(), err))
	}
	defer dst.Close()
	if _, err := io.Copy(dst, staged); err != nil {
		return toStatus(fmt.Errorf("while writing asset %s to %q: %w", uri, req.GetWritePath(), err))
	}
	if err := dst.Close(); err != nil {
		return toStatus(fmt.Errorf("while closing write path %q: %w", req.GetWritePath(), err))
	}
	return nil
}

// stage downloads the asset into a new file in the staging directory and
// returns it, positioned at its start, once its size and sha256 check out.
// The file is removed on error; on success the caller removes it.
func (s *Stager) stage(ctx context.Context, open OpenFunc, uri string, maxBytes int64, wantSum []byte, toStatus func(error) error) (_ *os.File, retErr error) {
	f, err := os.CreateTemp(s.stagingDir, "asset-")
	if err != nil {
		return nil, toStatus(fmt.Errorf("while creating staging file: %w", err))
	}
	defer func() {
		if retErr != nil {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	rc, err := open(ctx)
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

// writePath checks that path is absolute and below s.root, and returns it
// relative to s.root.
func (s *Stager) writePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", status.Errorf(codes.InvalidArgument, "write path %q is not absolute", path)
	}
	rel, err := filepath.Rel(s.root, path)
	if err != nil || !filepath.IsLocal(rel) {
		return "", status.Errorf(codes.InvalidArgument, "write path %q is not below %q", path, s.root)
	}
	return rel, nil
}

// checkWriteFile requires rel, below root, to be an existing regular file.
func checkWriteFile(root *os.Root, rel, shown string, toStatus func(error) error) error {
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

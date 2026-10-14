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

// Package provider implements objectstore.v1 on Azure Blob Storage.
package provider

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"go.ate.dev/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "go.ate.dev/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	blockSize         = 4 * 1024 * 1024
	stageConcurrency  = 8
	integrityMetadata = "substrate_sha256"
)

var errIntegrity = errors.New("blob integrity failure")

// Server implements both node and control snapshot services.
type Server struct {
	objectstorev1.UnimplementedNodeProviderServer
	objectstorev1.UnimplementedControlProviderServer

	stores map[string]blobstore.Store
	prefix string
	root   string
}

// New returns a snapshot provider confined to root, prefix, and the
// containers in stores, which maps each container name to its store.
func New(stores map[string]blobstore.Store, prefix, root string) (*Server, error) {
	if len(stores) == 0 {
		return nil, errors.New("at least one container is required")
	}
	for container, store := range stores {
		if container == "" || strings.Contains(container, "/") {
			return nil, fmt.Errorf("container %q is invalid", container)
		}
		if store == nil {
			return nil, fmt.Errorf("blob store for container %q is required", container)
		}
	}
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("root %q is not an absolute path", root)
	}
	prefix = strings.Trim(prefix, "/")
	if prefix != "" && !validKey(prefix) {
		return nil, fmt.Errorf("prefix %q is invalid", prefix)
	}
	return &Server{stores: maps.Clone(stores), prefix: prefix, root: filepath.Clean(root)}, nil
}

// FetchSnapshot downloads and verifies every requested file before atomically
// replacing its destination.
func (s *Server) FetchSnapshot(ctx context.Context, req *objectstorev1.FetchSnapshotRequest) (*objectstorev1.FetchSnapshotResponse, error) {
	loc, dir, err := s.validateNodeRequest(req.GetSnapshotUri(), req.GetWritePath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := s.openDir(dir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("opening restore directory: %w", err))
	}
	defer root.Close()

	for _, name := range req.GetFiles() {
		if err := fetchFile(ctx, loc.store, root, path.Join(loc.prefix, name), name); err != nil {
			return nil, toStatus(err)
		}
	}
	return &objectstorev1.FetchSnapshotResponse{}, nil
}

// UploadSnapshot stages deterministic blocks and commits each blob only after
// all of its bytes are durably staged.
func (s *Server) UploadSnapshot(ctx context.Context, req *objectstorev1.UploadSnapshotRequest) (*objectstorev1.UploadSnapshotResponse, error) {
	loc, dir, err := s.validateNodeRequest(req.GetSnapshotUri(), req.GetLocalPath(), req.GetFiles())
	if err != nil {
		return nil, err
	}
	root, err := s.openDir(dir)
	if err != nil {
		return nil, toStatus(fmt.Errorf("opening snapshot directory: %w", err))
	}
	defer root.Close()

	for _, name := range req.GetFiles() {
		local, err := root.Open(name)
		if err != nil {
			return nil, toStatus(fmt.Errorf("opening snapshot file %q: %w", name, err))
		}
		info, statErr := local.Stat()
		if statErr != nil {
			local.Close()
			return nil, toStatus(fmt.Errorf("inspecting snapshot file %q: %w", name, statErr))
		}
		if !info.Mode().IsRegular() {
			local.Close()
			return nil, status.Errorf(codes.InvalidArgument, "snapshot file %q is not a regular file", name)
		}
		uploadErr := upload(ctx, loc.store, path.Join(loc.prefix, name), local)
		closeErr := local.Close()
		if err := errors.Join(uploadErr, closeErr); err != nil {
			return nil, toStatus(fmt.Errorf("uploading snapshot file %q: %w", name, err))
		}
	}
	return &objectstorev1.UploadSnapshotResponse{}, nil
}

// CleanupSnapshot removes every committed blob below the requested prefix. A
// missing container holds no blobs, so cleanup against it succeeds.
func (s *Server) CleanupSnapshot(ctx context.Context, req *objectstorev1.CleanupSnapshotRequest) (*objectstorev1.CleanupSnapshotResponse, error) {
	loc, err := s.parseURI(req.GetSnapshotUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	names, err := loc.store.List(ctx, loc.prefix+"/")
	if isAbsent(err) {
		return &objectstorev1.CleanupSnapshotResponse{}, nil
	}
	if err != nil {
		return nil, toStatus(err)
	}
	for _, name := range names {
		err := loc.store.Delete(ctx, name)
		if errors.Is(err, blobstore.ErrContainerNotFound) {
			return &objectstorev1.CleanupSnapshotResponse{}, nil
		}
		if err != nil {
			return nil, toStatus(err)
		}
	}
	return &objectstorev1.CleanupSnapshotResponse{}, nil
}

// isAbsent reports whether err means the blob or its container is gone.
func isAbsent(err error) bool {
	return errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, blobstore.ErrContainerNotFound)
}

// CopySnapshot copies and re-verifies every committed source blob. Source and
// destination may be in different configured containers.
func (s *Server) CopySnapshot(ctx context.Context, req *objectstorev1.CopySnapshotRequest) (*objectstorev1.CopySnapshotResponse, error) {
	src, err := s.parseURI(req.GetSrcUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	dst, err := s.parseURI(req.GetDstUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	names, err := src.store.List(ctx, src.prefix+"/")
	if err != nil && !errors.Is(err, blobstore.ErrNotFound) {
		return nil, toStatus(err)
	}
	if len(names) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "source snapshot holds no files")
	}
	for _, name := range names {
		download, err := src.store.Download(ctx, name)
		if err != nil {
			return nil, toStatus(err)
		}
		// Keep the listed suffix byte for byte. Blob names are flat, so
		// cleaning a suffix such as "../x" would move it out of dst.prefix.
		target := dst.prefix + "/" + strings.TrimPrefix(name, src.prefix+"/")
		uploadErr := uploadVerified(ctx, dst.store, target, download)
		if err := errors.Join(uploadErr, download.Body.Close()); err != nil {
			return nil, toStatus(fmt.Errorf("copying blob %q: %w", name, err))
		}
	}
	return &objectstorev1.CopySnapshotResponse{}, nil
}

func fetchFile(ctx context.Context, store blobstore.Store, root *os.Root, blobName, fileName string) error {
	download, err := store.Download(ctx, blobName)
	if err != nil {
		return err
	}
	defer download.Body.Close()

	tempName, temp, err := createTemp(root)
	if err != nil {
		return fmt.Errorf("opening temporary restore file: %w", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(temp, hash), download.Body)
	syncErr := temp.Sync()
	closeErr := temp.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		_ = root.Remove(tempName)
		return fmt.Errorf("writing restore file: %w", err)
	}
	if err := verifyHash(download.Metadata, hash.Sum(nil)); err != nil {
		_ = root.Remove(tempName)
		return err
	}
	parent := path.Dir(fileName)
	if err := root.MkdirAll(parent, 0o700); err != nil {
		_ = root.Remove(tempName)
		return fmt.Errorf("creating restore subdirectory: %w", err)
	}
	if err := root.Rename(tempName, fileName); err != nil {
		_ = root.Remove(tempName)
		return fmt.Errorf("replacing restore file: %w", err)
	}
	return syncDirs(root, parent)
}

// syncDirs syncs dir and each of its ancestors up to the root, so a renamed
// file and any subdirectory created for it survive a crash.
func syncDirs(root *os.Root, dir string) error {
	for {
		d, err := root.Open(dir)
		if err != nil {
			return fmt.Errorf("opening restore directory for sync: %w", err)
		}
		if err := errors.Join(d.Sync(), d.Close()); err != nil {
			return fmt.Errorf("syncing restore directory: %w", err)
		}
		if dir == "." {
			return nil
		}
		dir = path.Dir(dir)
	}
}

// createTemp creates a restore file whose name does not depend on the
// requested file name, so a name close to the file system's limit still
// leaves room for the temporary one.
func createTemp(root *os.Root) (string, *os.File, error) {
	for range 10 {
		var random [8]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, err
		}
		name := fmt.Sprintf(".substrate-part-%x", random)
		file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, errors.New("could not allocate a temporary restore file")
}

func upload(ctx context.Context, store blobstore.Store, blobName string, source io.Reader) error {
	return uploadExpected(ctx, store, blobName, source, "")
}

func uploadExpected(ctx context.Context, store blobstore.Store, blobName string, source io.Reader, expectedHash string) error {
	stageCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	hash := sha256.New()
	blockIDs := []string{}
	semaphore := make(chan struct{}, stageConcurrency)
	var stages sync.WaitGroup
	var firstStageErr error
	var stageErrOnce sync.Once

	waitForStages := func() error {
		stages.Wait()
		if firstStageErr != nil {
			return firstStageErr
		}
		return ctx.Err()
	}

	for index := 0; ; index++ {
		buf := make([]byte, blockSize)
		n, err := io.ReadFull(source, buf)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			cancel()
			stages.Wait()
			return err
		}
		if n > 0 {
			block := buf[:n]
			_, _ = hash.Write(block)
			sum := md5.Sum(block)
			blockID := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%08d-%x", index, sum))
			select {
			case semaphore <- struct{}{}:
			case <-stageCtx.Done():
				return waitForStages()
			}
			stages.Add(1)
			go func() {
				defer stages.Done()
				defer func() { <-semaphore }()
				if err := store.StageBlock(stageCtx, blobName, blockID, block, sum[:]); err != nil {
					stageErrOnce.Do(func() {
						firstStageErr = err
						cancel()
					})
				}
			}()
			blockIDs = append(blockIDs, blockID)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
	}
	if err := waitForStages(); err != nil {
		return err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if expectedHash != "" && digest != expectedHash {
		return fmt.Errorf("%w: source blob got sha256 %s, want %s", errIntegrity, digest, expectedHash)
	}
	return store.CommitBlockList(ctx, blobName, blockIDs, map[string]*string{integrityMetadata: &digest})
}

func uploadVerified(ctx context.Context, store blobstore.Store, blobName string, download blobstore.Download) error {
	want, err := metadataHash(download.Metadata)
	if err != nil {
		return err
	}
	return uploadExpected(ctx, store, blobName, download.Body, want)
}

func verifyHash(metadata map[string]*string, sum []byte) error {
	want, err := metadataHash(metadata)
	if err != nil {
		return err
	}
	got := hex.EncodeToString(sum)
	if got != want {
		return fmt.Errorf("%w: got sha256 %s, want %s", errIntegrity, got, want)
	}
	return nil
}

func metadataHash(metadata map[string]*string) (string, error) {
	for key, value := range metadata {
		if strings.EqualFold(key, integrityMetadata) && value != nil {
			if len(*value) != sha256.Size*2 {
				return "", fmt.Errorf("%w: blob has invalid SHA-256 metadata", errIntegrity)
			}
			return strings.ToLower(*value), nil
		}
	}
	return "", fmt.Errorf("%w: blob has no SHA-256 metadata", errIntegrity)
}

func (s *Server) validateNodeRequest(snapshotURI, dir string, files []string) (location, string, error) {
	loc, err := s.parseURI(snapshotURI)
	if err != nil {
		return location{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	if !filepath.IsAbs(dir) {
		return location{}, "", status.Errorf(codes.InvalidArgument, "local path %q is not absolute", dir)
	}
	rel, err := filepath.Rel(s.root, dir)
	if err != nil || !filepath.IsLocal(rel) {
		return location{}, "", status.Errorf(codes.InvalidArgument, "local path %q is outside %q", dir, s.root)
	}
	if err := validateFileNames(files); err != nil {
		return location{}, "", status.Error(codes.InvalidArgument, err.Error())
	}
	return loc, rel, nil
}

func validateFileNames(files []string) error {
	if len(files) == 0 {
		return errors.New("no files requested")
	}
	seen := make(map[string]struct{}, len(files))
	for _, name := range files {
		// atelet names files relative to the snapshot directory, some in
		// subdirectories (gVisor's fs/ checkpoint). Each name is also the
		// blob name's suffix, so it follows the same rules as a prefix.
		if !validKey(name) {
			return fmt.Errorf("snapshot file %q is not a clean relative path", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("snapshot file %q is duplicated", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// location is a snapshot URI resolved to its container's store and the blob
// name prefix within that container.
type location struct {
	store  blobstore.Store
	prefix string
}

// parseURI resolves an azblob://<container>/<prefix> snapshot URI. The blob
// name prefix is the URI's decoded path, as for the in-tree object stores:
// ate-api-server derives tag and owner prefixes from a snapshot URI by
// parsing and re-encoding it, so only the decoded path names the same blobs
// in every URI it sends for one snapshot.
func (s *Server) parseURI(raw string) (location, error) {
	const scheme = "azblob://"
	if !strings.HasPrefix(raw, scheme) {
		return location{}, fmt.Errorf("snapshot URI %q must use azblob://", raw)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return location{}, fmt.Errorf("snapshot URI %q is invalid", raw)
	}
	container, objectPrefix := parsed.Host, strings.TrimPrefix(parsed.Path, "/")
	if container == "" || objectPrefix == "" {
		return location{}, fmt.Errorf("snapshot URI %q must name a container and non-empty prefix", raw)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return location{}, fmt.Errorf("snapshot URI %q must not have userinfo, a query or a fragment", raw)
	}
	store, ok := s.stores[container]
	if !ok {
		return location{}, fmt.Errorf("snapshot URI container %q is not one of the plugin's configured containers %v", container, slices.Sorted(maps.Keys(s.stores)))
	}
	if !validKey(objectPrefix) {
		return location{}, fmt.Errorf("snapshot URI %q has an invalid prefix", raw)
	}
	if s.prefix != "" {
		objectPrefix = path.Join(s.prefix, objectPrefix)
	}
	return location{store: store, prefix: objectPrefix}, nil
}

// validKey reports whether key is a clean, relative blob name prefix. Like
// ate-api-server's check on snapshot locations, it also rejects '\' and
// segments made only of dots and spaces, which some HTTP clients and storage
// services normalize or trim and so could address blobs outside the prefix.
func validKey(key string) bool {
	if key == "." || !fs.ValidPath(key) || strings.Contains(key, `\`) {
		return false
	}
	for segment := range strings.SplitSeq(key, "/") {
		if strings.Trim(segment, ". ") == "" {
			return false
		}
	}
	return true
}

func (s *Server) openDir(rel string) (*os.Root, error) {
	base, err := os.OpenRoot(s.root)
	if err != nil {
		return nil, err
	}
	defer base.Close()
	return base.OpenRoot(rel)
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, blobstore.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, blobstore.ErrContainerNotFound):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, blobstore.ErrPermission):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, blobstore.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, os.ErrNotExist):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, errIntegrity):
		return status.Error(codes.DataLoss, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

var _ objectstorev1.NodeProviderServer = (*Server)(nil)
var _ objectstorev1.ControlProviderServer = (*Server)(nil)

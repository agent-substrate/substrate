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
	"bytes"
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
	// minBlockSize is the block size for files of up to
	// blobstore.MaxBlocks*minBlockSize bytes (about 195 GiB). Larger files
	// use larger blocks; see blockLimits.blockSize.
	minBlockSize = 4 << 20
	// blockAlignment is the multiple larger block sizes are rounded up to.
	blockAlignment = 1 << 20
	// maxStages is how many blocks one upload stages at once.
	maxStages = 8
	// bufferBudget bounds the block data an upload that buffers its blocks
	// holds at once; see blockSource.buffer.
	bufferBudget = maxStages * minBlockSize
	// hashBufferSize is the buffer an upload that does not buffer its
	// blocks reads its source through to hash it.
	hashBufferSize    = 1 << 20
	integrityMetadata = "substrate_sha256"
)

var (
	errIntegrity = errors.New("blob integrity failure")
	errTooLarge  = errors.New("too large for one Azure block blob")
)

// blockLimits are the block blob limits an upload stays within.
type blockLimits struct {
	maxBlocks    int64
	maxBlockSize int64
}

var azureLimits = blockLimits{maxBlocks: blobstore.MaxBlocks, maxBlockSize: blobstore.MaxBlockSize}

// blockSize returns the size of the blocks an upload of size bytes stages:
// minBlockSize, or size/maxBlocks rounded up to blockAlignment when that is
// larger, so the blob never needs more than maxBlocks blocks. It fails if
// that block is larger than maxBlockSize. A negative size is unknown and gets
// minBlockSize; upload still refuses such a source once it needs more than
// maxBlocks blocks.
func (l blockLimits) blockSize(size int64) (int64, error) {
	if size < 0 {
		return minBlockSize, nil
	}
	need := ceilDiv(size, l.maxBlocks)
	if need > l.maxBlockSize {
		return 0, fmt.Errorf("%w: %d bytes need more than %d blocks of at most %d bytes", errTooLarge, size, l.maxBlocks, l.maxBlockSize)
	}
	aligned := min(ceilDiv(need, blockAlignment)*blockAlignment, l.maxBlockSize)
	return max(minBlockSize, aligned), nil
}

// ceilDiv returns a/b rounded up, for a >= 0 and b > 0, without overflow.
func ceilDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 {
		q++
	}
	return q
}

// bufferSlots returns how many blocks of blockSize bytes, at most
// bufferBudget, an upload that buffers its blocks holds at once.
func bufferSlots(blockSize int64) int {
	return int(bufferBudget / blockSize)
}

// Server implements both node and control snapshot services.
type Server struct {
	objectstorev1.UnimplementedNodeProviderServer
	objectstorev1.UnimplementedControlProviderServer

	stores map[string]blobstore.Store
	prefix string
	root   string
	limits blockLimits
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
	return &Server{stores: maps.Clone(stores), prefix: prefix, root: filepath.Clean(root), limits: azureLimits}, nil
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

	// Open and size every file before staging any block, so a request
	// holding a file too large for one blob fails before it transfers data.
	files := make([]*os.File, len(req.GetFiles()))
	sizes := make([]int64, len(files))
	defer func() {
		for _, local := range files {
			if local != nil {
				local.Close()
			}
		}
	}()
	for i, name := range req.GetFiles() {
		local, err := root.Open(name)
		if err != nil {
			return nil, toStatus(fmt.Errorf("opening snapshot file %q: %w", name, err))
		}
		files[i] = local
		info, err := local.Stat()
		if err != nil {
			return nil, toStatus(fmt.Errorf("inspecting snapshot file %q: %w", name, err))
		}
		if !info.Mode().IsRegular() {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot file %q is not a regular file", name)
		}
		if _, err := s.limits.blockSize(info.Size()); err != nil {
			return nil, toStatus(fmt.Errorf("snapshot file %q: %w", name, err))
		}
		sizes[i] = info.Size()
	}
	for i, name := range req.GetFiles() {
		uploadErr := s.limits.upload(ctx, loc.store, path.Join(loc.prefix, name), fileSource(files[i], sizes[i]), "")
		closeErr := files[i].Close()
		files[i] = nil
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
		uploadErr := s.limits.uploadVerified(ctx, dst.store, target, src.store, name, download)
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

// A blockSource is the data one upload commits as a blob.
type blockSource struct {
	// r yields the data, read once and in order.
	r io.Reader
	// size is how many bytes r yields, or negative if unknown.
	size int64
	// reread returns a body that reads bytes [off, off+n) of the data again,
	// for StageBlock to send and, to retry, send again.
	reread func(ctx context.Context, off, n int64) io.ReadSeekCloser
	// buffer is set for a source whose second read costs a second download.
	// Blocks of up to bufferBudget bytes are then held in memory from the
	// read that hashes them until they are staged, instead of read again.
	buffer bool
}

// fileSource returns the first size bytes of file as a blockSource that reads
// each block from the file twice, once to hash it and once to stage it, so an
// upload holds no file data in memory.
func fileSource(file *os.File, size int64) blockSource {
	return blockSource{
		r:    io.NewSectionReader(file, 0, size),
		size: size,
		reread: func(_ context.Context, off, n int64) io.ReadSeekCloser {
			return nopCloser{io.NewSectionReader(file, off, n)}
		},
	}
}

type nopCloser struct{ io.ReadSeeker }

func (nopCloser) Close() error { return nil }

// upload commits src as blobName in blocks of blockSize(src.size) bytes. It
// reads src.r once, hashing each block and then staging it, up to maxStages
// at once, from src.reread, so it holds no block data beyond a
// hashBufferSize buffer. If src.buffer is set and a block fits in
// bufferBudget, it instead stages each block from the bytes it hashed,
// holding at most bufferBudget bytes of blocks at once. A non-empty
// expectedHash must match the SHA-256 of the bytes read from src.r.
func (l blockLimits) upload(ctx context.Context, store blobstore.Store, blobName string, src blockSource, expectedHash string) error {
	blockSize, err := l.blockSize(src.size)
	if err != nil {
		return err
	}
	buffered := src.buffer && blockSize <= bufferBudget
	var hashBuf []byte
	slotCount := maxStages
	if buffered {
		slotCount = bufferSlots(blockSize)
	} else {
		hashBuf = make([]byte, hashBufferSize)
	}
	stageCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	hash := sha256.New()
	blockIDs := []string{}
	// A slot is taken before a block is read and released after it is
	// staged, so it bounds the blocks in flight and, when buffered, the
	// buffers alive at once.
	slots := make(chan struct{}, slotCount)
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
	abort := func(err error) error {
		<-slots
		cancel()
		stages.Wait()
		return err
	}

	var off int64
	for index := 0; ; index++ {
		select {
		case slots <- struct{}{}:
		case <-stageCtx.Done():
			return waitForStages()
		}
		var (
			n    int64
			sum  [md5.Size]byte
			body io.ReadSeekCloser
		)
		if buffered {
			buf := make([]byte, blockSize)
			read, err := io.ReadFull(src.r, buf)
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return abort(err)
			}
			n = int64(read)
			block := buf[:read]
			_, _ = hash.Write(block)
			sum = md5.Sum(block)
			body = nopCloser{bytes.NewReader(block)}
		} else {
			blockHash := md5.New()
			n, err = io.CopyBuffer(io.MultiWriter(hash, blockHash), io.LimitReader(src.r, blockSize), hashBuf)
			if err != nil {
				return abort(err)
			}
			blockHash.Sum(sum[:0])
		}
		if n == 0 {
			<-slots
			break
		}
		if int64(index) == l.maxBlocks {
			return abort(fmt.Errorf("%w: source holds more than %d blocks of %d bytes", errTooLarge, l.maxBlocks, blockSize))
		}
		if !buffered {
			body = src.reread(stageCtx, off, n)
		}
		blockID := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, "%08d-%x", index, sum))
		stages.Add(1)
		go func() {
			defer stages.Done()
			defer func() { <-slots }()
			defer body.Close()
			if err := store.StageBlock(stageCtx, blobName, blockID, body, sum[:]); err != nil {
				stageErrOnce.Do(func() {
					firstStageErr = err
					cancel()
				})
			}
		}()
		blockIDs = append(blockIDs, blockID)
		off += n
		if n < blockSize {
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

// uploadVerified commits download, the body of blob srcName in srcStore, as
// blobName in store, and fails unless its bytes match the SHA-256 in the
// download's metadata. Blocks larger than bufferBudget are staged from
// ranged downloads of the source, so a copy of any size holds at most
// bufferBudget bytes of blocks.
func (l blockLimits) uploadVerified(ctx context.Context, store blobstore.Store, blobName string, srcStore blobstore.Store, srcName string, download blobstore.Download) error {
	want, err := metadataHash(download.Metadata)
	if err != nil {
		return err
	}
	return l.upload(ctx, store, blobName, blockSource{
		r:      download.Body,
		size:   download.Size,
		buffer: true,
		reread: func(ctx context.Context, off, n int64) io.ReadSeekCloser {
			return &rangeBody{ctx: ctx, store: srcStore, name: srcName, off: off, n: n}
		},
	}, want)
}

// rangeBody reads bytes [off, off+n) of a blob. It opens a ranged download at
// its position on the first read after it is created or seeks, so StageBlock
// can seek it back to the start to retry. The bytes it reads are checked
// against the MD5 that StageBlock sends, so a blob that changed since the
// read that hashed them fails the stage.
type rangeBody struct {
	ctx    context.Context
	store  blobstore.Store
	name   string
	off, n int64

	pos  int64
	body io.ReadCloser
}

func (r *rangeBody) Read(p []byte) (int, error) {
	if r.pos >= r.n {
		return 0, io.EOF
	}
	if r.body == nil {
		body, err := r.store.DownloadRange(r.ctx, r.name, r.off+r.pos, r.n-r.pos)
		if err != nil {
			return 0, err
		}
		r.body = body
	}
	if int64(len(p)) > r.n-r.pos {
		p = p[:r.n-r.pos]
	}
	read, err := r.body.Read(p)
	r.pos += int64(read)
	if errors.Is(err, io.EOF) && r.pos < r.n {
		return read, fmt.Errorf("blob %q ended %d bytes into a %d byte range: %w", r.name, r.pos, r.n, io.ErrUnexpectedEOF)
	}
	return read, err
}

func (r *rangeBody) Seek(offset int64, whence int) (int64, error) {
	pos := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		pos += r.pos
	case io.SeekEnd:
		pos += r.n
	default:
		return 0, fmt.Errorf("seek whence %d is invalid", whence)
	}
	if pos < 0 {
		return 0, fmt.Errorf("seek to %d is before the start", pos)
	}
	if pos != r.pos {
		if err := r.Close(); err != nil {
			return 0, err
		}
		r.pos = pos
	}
	return pos, nil
}

// Close closes the open download, if any.
func (r *rangeBody) Close() error {
	if r.body == nil {
		return nil
	}
	err := r.body.Close()
	r.body = nil
	return err
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
	case errors.Is(err, errTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

var _ objectstorev1.NodeProviderServer = (*Server)(nil)
var _ objectstorev1.ControlProviderServer = (*Server)(nil)

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

package glutton

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// diskKeyRE rejects anything that could escape the data dir or hit a
// hidden file: only alphanumerics, underscore, and dash are permitted.
var diskKeyRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type Service struct {
	gluttonpb.UnimplementedGluttonServer

	dataDir string

	// TODO: split this into per-resource locks (ram, fds, peers). A single
	// global mutex serializes unrelated operations across all three.
	mu  sync.Mutex
	ram map[string][]byte
	// ramCursor is each array's next WRITE_MODE_OVERWRITE_ROTATE offset.
	// Absent means 0; invalidated whenever the array is reallocated.
	ramCursor map[string]int
	fds       []*os.File
	peers     map[string]*peerGossip

	ramWriteBytes  metric.Int64Counter
	ramReadBytes   metric.Int64Counter
	diskWriteBytes metric.Int64Counter
	diskReadBytes  metric.Int64Counter
	pingsReceived  metric.Int64Counter
	gossipSent     metric.Int64Counter
	gossipLatency  metric.Float64Histogram
}

// New constructs a Service storing WriteDisk files under dir and registers its
// otel instruments. The caller is responsible for creating dir and for calling
// Close to stop any running gossip goroutines.
func New(dir string) (*Service, error) {
	s := &Service{
		dataDir:   dir,
		ram:       make(map[string][]byte),
		ramCursor: make(map[string]int),
		peers:     make(map[string]*peerGossip),
	}
	if err := s.initMetrics(); err != nil {
		return nil, err
	}
	return s, nil
}

// Close cancels every running gossip goroutine and waits for them to exit.
func (s *Service) Close() {
	s.mu.Lock()
	peers := s.peers
	s.peers = make(map[string]*peerGossip)
	s.mu.Unlock()
	for _, p := range peers {
		p.cancel()
		<-p.done
	}
}

// Write to RAM, either overwriting previously-used RAM or allocating additional RAM
// per request instructions. Data written will be random bytes.
func (s *Service) WriteRAM(ctx context.Context, req *gluttonpb.WriteRAMRequest) (*gluttonpb.WriteRAMResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	sizeBytes, err := parseBytes(req.GetSize())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "size: %v", err)
	}
	if sizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "size must be non-negative")
	}
	size := int(sizeBytes)

	switch req.GetWriteMode() {
	case gluttonpb.WriteMode_WRITE_MODE_TRUNCATE:
		buf, err := randomBytes(size)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate random bytes: %v", err)
		}
		s.mu.Lock()
		s.ram[req.GetKey()] = buf
		delete(s.ramCursor, req.GetKey())
		s.mu.Unlock()
	case gluttonpb.WriteMode_WRITE_MODE_OVERWRITE:
		s.mu.Lock()
		existing := s.ram[req.GetKey()]
		if size > len(existing) {
			existing = make([]byte, size)
			s.ram[req.GetKey()] = existing
			delete(s.ramCursor, req.GetKey())
		}
		if _, err := rand.Read(existing[:size]); err != nil {
			s.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "generate random bytes: %v", err)
		}
		s.mu.Unlock()
	case gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE:
		if err := s.rotateRAM(req.GetKey(), size); err != nil {
			return nil, err
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown write_mode %v", req.GetWriteMode())
	}

	s.ramWriteBytes.Add(ctx, int64(size))
	return &gluttonpb.WriteRAMResponse{}, nil
}

// rotateRAM re-randomizes size bytes starting at the key's cursor, wrapping
// at the end of the array, then advances the cursor past the write. Repeated
// rotates therefore walk the whole array instead of re-dirtying the same
// prefix. The cursor lives in process memory, so it rides along in snapshots
// and the walk keeps advancing across suspend/resume cycles.
func (s *Service) rotateRAM(key string, size int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.ram[key]
	if len(existing) == 0 {
		return status.Errorf(codes.NotFound, "rotate needs an existing array %q; fill with TRUNCATE first", key)
	}
	if size > len(existing) {
		size = len(existing)
	}
	start := s.ramCursor[key]
	head := existing[start:min(start+size, len(existing))]
	if _, err := rand.Read(head); err != nil {
		return status.Errorf(codes.Internal, "generate random bytes: %v", err)
	}
	if wrapped := size - len(head); wrapped > 0 {
		if _, err := rand.Read(existing[:wrapped]); err != nil {
			return status.Errorf(codes.Internal, "generate random bytes: %v", err)
		}
	}
	s.ramCursor[key] = (start + size) % len(existing)
	return nil
}

// pageSize is the stride of the ReadRAM walk: one byte per 4KiB page is
// enough to force every page resident without the cost of reading them all.
const pageSize = 4096

// Walk RAM previously written by WriteRAM, reading one byte per page so
// every touched page must be resident before the response returns. After a
// demand-paged restore this converts restore-time laziness into measurable
// read latency.
func (s *Service) ReadRAM(ctx context.Context, req *gluttonpb.ReadRAMRequest) (*gluttonpb.ReadRAMResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arr, ok := s.ram[req.GetKey()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no RAM array %q", req.GetKey())
	}
	walk := int64(len(arr))
	if req.GetSize() != "" {
		n, err := parseBytes(req.GetSize())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "size: %v", err)
		}
		if n < 0 {
			return nil, status.Error(codes.InvalidArgument, "size must be non-negative")
		}
		walk = min(n, walk)
	}
	var sum uint32
	for i := int64(0); i < walk; i += pageSize {
		sum ^= uint32(arr[i])
	}
	s.ramReadBytes.Add(ctx, walk)
	return &gluttonpb.ReadRAMResponse{Size: walk, Checksum: sum}, nil
}

// Write to disk using the specified mode. Data written will be random bytes.
func (s *Service) WriteDisk(ctx context.Context, req *gluttonpb.WriteDiskRequest) (*gluttonpb.WriteDiskResponse, error) {
	if !diskKeyRE.MatchString(req.GetKey()) {
		return nil, status.Errorf(codes.InvalidArgument, "key %q must match %s", req.GetKey(), diskKeyRE)
	}
	if req.GetSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "size must be non-negative")
	}

	path := filepath.Join(s.dataDir, req.GetKey())

	var flag int
	switch req.GetWriteMode() {
	case gluttonpb.WriteMode_WRITE_MODE_TRUNCATE:
		flag = os.O_RDWR | os.O_CREATE | os.O_TRUNC
	case gluttonpb.WriteMode_WRITE_MODE_OVERWRITE:
		// No O_TRUNC: writes go from offset 0 but any bytes beyond size remain.
		flag = os.O_RDWR | os.O_CREATE
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown write_mode %v", req.GetWriteMode())
	}

	f, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open %s: %v", path, err)
	}
	defer f.Close()

	h := sha256.New()
	size := int64(req.GetSize())
	if err := streamRandomBytes(io.MultiWriter(f, h), size); err != nil {
		return nil, status.Errorf(codes.Internal, "write %s: %v", path, err)
	}

	// OVERWRITE has no O_TRUNC, bytes from a larger, earlier write will persist.
	// The cursor is already at size, so folding the remainder into the
	// same digest completes it without re-reading the prefix.
	if req.GetWriteMode() == gluttonpb.WriteMode_WRITE_MODE_OVERWRITE {
		tail, err := io.Copy(h, f)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "hash tail %s: %v", path, err)
		}
		size += tail
	}

	s.diskWriteBytes.Add(ctx, int64(req.GetSize()))
	return &gluttonpb.WriteDiskResponse{Size: size, Sha256: h.Sum(nil)}, nil
}

func (s *Service) ReadDisk(ctx context.Context, req *gluttonpb.ReadDiskRequest) (*gluttonpb.ReadDiskResponse, error) {
	if !diskKeyRE.MatchString(req.GetKey()) {
		return nil, status.Errorf(codes.InvalidArgument, "key %q must match %s", req.GetKey(), diskKeyRE)
	}

	path := filepath.Join(s.dataDir, req.GetKey())

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "file %q not found", req.GetKey())
		}
		return nil, status.Errorf(codes.Internal, "open %s: %v", path, err)
	}
	defer f.Close()

	h := sha256.New()

	if req.GetReadMode() == gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY {
		n, err := io.Copy(h, f)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read %s: %v", path, err)
		}
		s.diskReadBytes.Add(ctx, n)
		return &gluttonpb.ReadDiskResponse{
			Size:   n,
			Sha256: h.Sum(nil),
		}, nil
	}

	data, err := io.ReadAll(io.TeeReader(f, h))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read %s: %v", path, err)
	}

	s.diskReadBytes.Add(ctx, int64(len(data)))
	return &gluttonpb.ReadDiskResponse{
		Size:   int64(len(data)),
		Sha256: h.Sum(nil),
		Data:   data,
	}, nil
}

// Make sure it has the specified number of file descriptors open. It will open or
// close file descriptors to hit the desired count (note this count is in addition to the other
// FDs needed to run the process).
func (s *Service) OpenFD(_ context.Context, req *gluttonpb.OpenFDRequest) (*gluttonpb.OpenFDResponse, error) {
	if req.GetCount() < 0 {
		return nil, status.Error(codes.InvalidArgument, "count must be non-negative")
	}
	target := int(req.GetCount())

	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.fds) > target {
		last := len(s.fds) - 1
		if err := s.fds[last].Close(); err != nil {
			slog.Warn("Failed to close glutton fd", slog.Any("err", err))
		}
		s.fds[last] = nil
		s.fds = s.fds[:last]
	}
	for len(s.fds) < target {
		f, err := os.Open(os.DevNull)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "open %s: %v", os.DevNull, err)
		}
		s.fds = append(s.fds, f)
	}
	return &gluttonpb.OpenFDResponse{}, nil
}

// Receive a ping request, echoing the same response back.
func (s *Service) Ping(ctx context.Context, req *gluttonpb.PingRequest) (*gluttonpb.PingResponse, error) {
	s.pingsReceived.Add(ctx, 1)
	return &gluttonpb.PingResponse{Message: req.GetMessage()}, nil
}

func randomBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// streamRandomBytesChunk caps per-syscall random fill and write size so a
// multi-gigabyte WriteDisk doesn't have to materialize in RAM.
const streamRandomBytesChunk = 1 << 20 // 1 MiB

// streamRandomBytes writes total random bytes to w sequentially, in
// streamRandomBytesChunk-sized chunks. The caller is responsible for the
// file's open mode and starting offset; this writes from the current
// position forward.
func streamRandomBytes(w io.Writer, total int64) error {
	if total <= 0 {
		return nil
	}
	buf := make([]byte, streamRandomBytesChunk)
	var written int64
	for written < total {
		chunk := buf
		if remaining := total - written; remaining < int64(len(chunk)) {
			chunk = buf[:remaining]
		}
		if _, err := rand.Read(chunk); err != nil {
			return fmt.Errorf("generate random bytes: %w", err)
		}
		n, err := w.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	return nil
}

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
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

func TestWriteDiskReadDiskRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	tests := []struct {
		name string
		key  string
		size int32
	}{
		{name: "zero size", key: "zero", size: 0},
		{name: "small size", key: "small", size: 1024},
		{name: "chunk unaligned size", key: "unaligned", size: (1 << 20) + 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeResp, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
				Key:       tt.key,
				Size:      tt.size,
				WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
			})
			if err != nil {
				t.Fatalf("WriteDisk failed: %v", err)
			}
			if writeResp.GetSize() != int64(tt.size) {
				t.Errorf("WriteDisk size mismatch: got %d, want %d", writeResp.GetSize(), tt.size)
			}

			// 1. Full data read
			readResp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{
				Key:      tt.key,
				ReadMode: gluttonpb.ReadMode_READ_MODE_DATA,
			})
			if err != nil {
				t.Fatalf("ReadDisk (DATA) failed: %v", err)
			}

			if readResp.GetSize() != int64(tt.size) {
				t.Errorf("ReadDisk size mismatch: got %d, want %d", readResp.GetSize(), tt.size)
			}
			if !bytes.Equal(readResp.GetSha256(), writeResp.GetSha256()) {
				t.Errorf("sha256 mismatch between WriteDisk and ReadDisk")
			}
			if len(readResp.GetData()) != int(tt.size) {
				t.Errorf("ReadDisk data length mismatch: got %d, want %d", len(readResp.GetData()), tt.size)
			}

			computedDigest := sha256.Sum256(readResp.GetData())
			if !bytes.Equal(readResp.GetSha256(), computedDigest[:]) {
				t.Errorf("ReadDisk returned sha256 does not match computed digest of returned data")
			}

			// 2. Digest-only read
			digestResp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{
				Key:      tt.key,
				ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY,
			})
			if err != nil {
				t.Fatalf("ReadDisk (DIGEST_ONLY) failed: %v", err)
			}
			if digestResp.GetSize() != int64(tt.size) {
				t.Errorf("ReadDisk (DIGEST_ONLY) size mismatch: got %d, want %d", digestResp.GetSize(), tt.size)
			}
			if !bytes.Equal(digestResp.GetSha256(), writeResp.GetSha256()) {
				t.Errorf("sha256 mismatch between WriteDisk and ReadDisk (DIGEST_ONLY)")
			}
			if len(digestResp.GetData()) != 0 {
				t.Errorf("ReadDisk (DIGEST_ONLY) should not return data payload, got %d bytes", len(digestResp.GetData()))
			}
		})
	}
}

func TestWriteDiskTruncateProducesExactSize(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "testfile"
	size := int32(2048)

	_, err = svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
		Key:       key,
		Size:      size,
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
	})
	if err != nil {
		t.Fatalf("WriteDisk failed: %v", err)
	}

	filePath := filepath.Join(tempDir, key)
	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if fi.Size() != int64(size) {
		t.Errorf("file size on disk mismatch: got %d, want %d", fi.Size(), size)
	}
}

func TestWriteDiskOverwriteDigestMatchesReadDisk(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "overwrittenfile"

	// 1. Initial write of large file (4096 bytes)
	_, err = svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
		Key:       key,
		Size:      4096,
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
	})
	if err != nil {
		t.Fatalf("WriteDisk (large) failed: %v", err)
	}

	// 2. Overwrite prefix with smaller size (1024 bytes) without truncation
	overwriteResp, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
		Key:       key,
		Size:      1024,
		WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE,
	})
	if err != nil {
		t.Fatalf("WriteDisk (overwrite) failed: %v", err)
	}

	if overwriteResp.GetSize() != 4096 {
		t.Errorf("expected WriteDisk under OVERWRITE to report total file size 4096, got %d", overwriteResp.GetSize())
	}

	// 3. ReadDisk reads the entire file (4096 bytes)
	readResp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{
		Key:      key,
		ReadMode: gluttonpb.ReadMode_READ_MODE_DATA,
	})
	if err != nil {
		t.Fatalf("ReadDisk failed: %v", err)
	}

	if readResp.GetSize() != 4096 {
		t.Errorf("expected ReadDisk size 4096, got %d", readResp.GetSize())
	}
	if !bytes.Equal(readResp.GetSha256(), overwriteResp.GetSha256()) {
		t.Errorf("expected WriteDisk(OVERWRITE) whole-file digest to match ReadDisk digest")
	}
}

func TestReadDiskRejectsInvalidKey(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	_, err = svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{Key: "../escape"})
	if err == nil {
		t.Error("expected error for invalid key with path traversal, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument code, got %v", err)
	}
}

// TestWriteDiskFileCountSpreadsBytesOverFiles checks that a multi-file write
// lays total bytes out as file_count files under key, that its digest is the
// concatenation ReadDisk reads back, and that a single-file write reports the
// same shape a caller would see with file_count unset.
func TestWriteDiskFileCountSpreadsBytesOverFiles(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	tests := []struct {
		name      string
		size      int32
		fileCount int32
		wantSizes []int64
	}{
		{name: "even split", size: 4096, fileCount: 4, wantSizes: []int64{1024, 1024, 1024, 1024}},
		{name: "remainder goes to the first files", size: 10, fileCount: 3, wantSizes: []int64{4, 3, 3}},
		{name: "more files than bytes", size: 2, fileCount: 3, wantSizes: []int64{1, 1, 0}},
		{name: "chunk unaligned files", size: (1 << 20) + 2, fileCount: 2, wantSizes: []int64{(1 << 19) + 1, (1 << 19) + 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "multi"
			writeResp, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
				Key:       key,
				Size:      tt.size,
				WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
				FileCount: tt.fileCount,
			})
			if err != nil {
				t.Fatalf("WriteDisk failed: %v", err)
			}
			if writeResp.GetSize() != int64(tt.size) {
				t.Errorf("WriteDisk size: got %d, want total %d", writeResp.GetSize(), tt.size)
			}

			entries, err := os.ReadDir(filepath.Join(tempDir, key))
			if err != nil {
				t.Fatalf("key is not a directory: %v", err)
			}
			if len(entries) != int(tt.fileCount) {
				t.Fatalf("files under key: got %d, want %d", len(entries), tt.fileCount)
			}
			var concatenated []byte
			for i, entry := range entries {
				if entry.Name() != diskFileName(i) {
					t.Errorf("file %d named %q, want %q", i, entry.Name(), diskFileName(i))
				}
				data, err := os.ReadFile(filepath.Join(tempDir, key, entry.Name()))
				if err != nil {
					t.Fatalf("reading file %d: %v", i, err)
				}
				if int64(len(data)) != tt.wantSizes[i] {
					t.Errorf("file %d size: got %d, want %d", i, len(data), tt.wantSizes[i])
				}
				concatenated = append(concatenated, data...)
			}
			wantDigest := sha256.Sum256(concatenated)
			if !bytes.Equal(writeResp.GetSha256(), wantDigest[:]) {
				t.Errorf("WriteDisk sha256 is not the digest of the files concatenated in name order")
			}

			for _, mode := range []gluttonpb.ReadMode{gluttonpb.ReadMode_READ_MODE_DATA, gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY} {
				readResp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{Key: key, ReadMode: mode})
				if err != nil {
					t.Fatalf("ReadDisk(%v) failed: %v", mode, err)
				}
				if readResp.GetSize() != int64(tt.size) {
					t.Errorf("ReadDisk(%v) size: got %d, want %d", mode, readResp.GetSize(), tt.size)
				}
				if !bytes.Equal(readResp.GetSha256(), writeResp.GetSha256()) {
					t.Errorf("ReadDisk(%v) sha256 differs from WriteDisk's", mode)
				}
				wantData := concatenated
				if mode == gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY {
					wantData = nil
				}
				if !bytes.Equal(readResp.GetData(), wantData) {
					t.Errorf("ReadDisk(%v) data: got %d bytes, want %d", mode, len(readResp.GetData()), len(wantData))
				}
			}
		})
	}
}

// TestWriteDiskTruncateReplacesLayout checks that TRUNCATE swaps key between
// its single-file and multi-file layouts without leaving the other behind,
// and that shrinking file_count drops the files a larger count wrote.
func TestWriteDiskTruncateReplacesLayout(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "layout"
	write := func(size, fileCount int32) *gluttonpb.WriteDiskResponse {
		t.Helper()
		resp, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
			Key:       key,
			Size:      size,
			WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
			FileCount: fileCount,
		})
		if err != nil {
			t.Fatalf("WriteDisk(size=%d, file_count=%d) failed: %v", size, fileCount, err)
		}
		return resp
	}
	readBack := func(want *gluttonpb.WriteDiskResponse) {
		t.Helper()
		resp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{Key: key, ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY})
		if err != nil {
			t.Fatalf("ReadDisk failed: %v", err)
		}
		if resp.GetSize() != want.GetSize() || !bytes.Equal(resp.GetSha256(), want.GetSha256()) {
			t.Errorf("ReadDisk size %d / sha256 %x, want the last write's %d / %x", resp.GetSize(), resp.GetSha256(), want.GetSize(), want.GetSha256())
		}
	}
	path := filepath.Join(tempDir, key)

	// file -> directory
	write(64, 0)
	readBack(write(64, 8))
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("key should be a directory after a multi-file write: %v", err)
	}
	if len(entries) != 8 {
		t.Errorf("files after file_count=8: got %d, want 8", len(entries))
	}

	// shrinking the count removes the surplus files
	readBack(write(64, 2))
	entries, err = os.ReadDir(path)
	if err != nil {
		t.Fatalf("key should still be a directory: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("files after file_count=2: got %d, want 2", len(entries))
	}

	// directory -> file
	readBack(write(64, 1))
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
	if fi.IsDir() {
		t.Errorf("key should be a plain file after a single-file write")
	}
}

func TestWriteDiskOverwriteWithFileCountKeepsTails(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "overwrite-multi"

	if _, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
		Key: key, Size: 4096, WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE, FileCount: 4,
	}); err != nil {
		t.Fatalf("WriteDisk (large) failed: %v", err)
	}
	// Each of the four files keeps its 1024-byte length: 256 bytes are
	// rewritten and the 768-byte tail persists.
	overwriteResp, err := svc.WriteDisk(ctx, &gluttonpb.WriteDiskRequest{
		Key: key, Size: 1024, WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE, FileCount: 4,
	})
	if err != nil {
		t.Fatalf("WriteDisk (overwrite) failed: %v", err)
	}
	if overwriteResp.GetSize() != 4096 {
		t.Errorf("WriteDisk(OVERWRITE) size: got %d, want the files' total 4096", overwriteResp.GetSize())
	}

	readResp, err := svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{Key: key, ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY})
	if err != nil {
		t.Fatalf("ReadDisk failed: %v", err)
	}
	if readResp.GetSize() != 4096 {
		t.Errorf("ReadDisk size: got %d, want 4096", readResp.GetSize())
	}
	if !bytes.Equal(readResp.GetSha256(), overwriteResp.GetSha256()) {
		t.Errorf("WriteDisk(OVERWRITE) digest over all files should match ReadDisk's")
	}
}

func TestWriteDiskRejectsNegativeFileCount(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	_, err = svc.WriteDisk(context.Background(), &gluttonpb.WriteDiskRequest{Key: "neg", Size: 1, FileCount: -1})
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for negative file_count, got %v", err)
	}
}

func TestReadDiskNotFound(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := New(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	_, err = svc.ReadDisk(ctx, &gluttonpb.ReadDiskRequest{Key: "nonexistent"})
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", err)
	}
}

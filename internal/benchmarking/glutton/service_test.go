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

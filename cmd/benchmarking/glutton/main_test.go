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
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/proto/glutton"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TestSplitGRPCServesReadyzAndGRPCOnOneListener starts the grpc-mode handler
// on a real listener and exercises both protocols against it: the readyz
// probe is a plain HTTP GET, and it must not stop gRPC from being served.
func TestSplitGRPCServesReadyzAndGRPCOnOneListener(t *testing.T) {
	svc, err := newGluttonService(t.TempDir())
	if err != nil {
		t.Fatalf("newGluttonService: %v", err)
	}
	defer svc.Close()

	grpcSrv := grpc.NewServer()
	glutton.RegisterGluttonServer(grpcSrv, svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := newServer(splitGRPC(grpcSrv, mux))
	go srv.Serve(lis)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := http.Get("http://" + lis.Addr().String() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	pong, err := glutton.NewGluttonClient(conn).Ping(ctx, &glutton.PingRequest{Message: "hi"})
	if err != nil {
		t.Fatalf("Ping over gRPC: %v", err)
	}
	if pong.GetMessage() != "hi" {
		t.Errorf("Ping = %q, want %q", pong.GetMessage(), "hi")
	}
}

// TestSplitGRPCRoutesOnContentType pins the routing rule itself: an HTTP/2
// request is not enough to reach the gRPC server, the content type is what
// decides.
func TestSplitGRPCRoutesOnContentType(t *testing.T) {
	grpcHit := false
	handler := splitGRPC(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { grpcHit = true }),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }),
	)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := newServer(handler)
	go srv.Serve(lis)
	defer srv.Close()

	resp, err := http.Get("http://" + lis.Addr().String() + "/anything")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("HTTP/1.1 GET = %d, want %d (the non-gRPC handler)", resp.StatusCode, http.StatusTeapot)
	}
	if grpcHit {
		t.Error("HTTP/1.1 GET reached the gRPC handler")
	}
}

func TestWriteDiskReadDiskRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := newGluttonService(tempDir)
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
			writeResp, err := svc.WriteDisk(ctx, &glutton.WriteDiskRequest{
				Key:       tt.key,
				Size:      tt.size,
				WriteMode: glutton.WriteMode_WRITE_MODE_TRUNCATE,
			})
			if err != nil {
				t.Fatalf("WriteDisk failed: %v", err)
			}
			if writeResp.GetSize() != int64(tt.size) {
				t.Errorf("WriteDisk size mismatch: got %d, want %d", writeResp.GetSize(), tt.size)
			}

			// 1. Full data read
			readResp, err := svc.ReadDisk(ctx, &glutton.ReadDiskRequest{
				Key:      tt.key,
				ReadMode: glutton.ReadMode_READ_MODE_DATA,
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
			digestResp, err := svc.ReadDisk(ctx, &glutton.ReadDiskRequest{
				Key:      tt.key,
				ReadMode: glutton.ReadMode_READ_MODE_DIGEST_ONLY,
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
	svc, err := newGluttonService(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "testfile"
	size := int32(2048)

	_, err = svc.WriteDisk(ctx, &glutton.WriteDiskRequest{
		Key:       key,
		Size:      size,
		WriteMode: glutton.WriteMode_WRITE_MODE_TRUNCATE,
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
	svc, err := newGluttonService(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	key := "overwrittenfile"

	// 1. Initial write of large file (4096 bytes)
	_, err = svc.WriteDisk(ctx, &glutton.WriteDiskRequest{
		Key:       key,
		Size:      4096,
		WriteMode: glutton.WriteMode_WRITE_MODE_TRUNCATE,
	})
	if err != nil {
		t.Fatalf("WriteDisk (large) failed: %v", err)
	}

	// 2. Overwrite prefix with smaller size (1024 bytes) without truncation
	overwriteResp, err := svc.WriteDisk(ctx, &glutton.WriteDiskRequest{
		Key:       key,
		Size:      1024,
		WriteMode: glutton.WriteMode_WRITE_MODE_OVERWRITE,
	})
	if err != nil {
		t.Fatalf("WriteDisk (overwrite) failed: %v", err)
	}

	if overwriteResp.GetSize() != 4096 {
		t.Errorf("expected WriteDisk under OVERWRITE to report total file size 4096, got %d", overwriteResp.GetSize())
	}

	// 3. ReadDisk reads the entire file (4096 bytes)
	readResp, err := svc.ReadDisk(ctx, &glutton.ReadDiskRequest{
		Key:      key,
		ReadMode: glutton.ReadMode_READ_MODE_DATA,
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
	svc, err := newGluttonService(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	_, err = svc.ReadDisk(ctx, &glutton.ReadDiskRequest{Key: "../escape"})
	if err == nil {
		t.Error("expected error for invalid key with path traversal, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument code, got %v", err)
	}
}

func TestReadDiskNotFound(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := newGluttonService(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	_, err = svc.ReadDisk(ctx, &glutton.ReadDiskRequest{Key: "nonexistent"})
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.NotFound {
		t.Errorf("expected NotFound code, got %v", err)
	}
}

func TestHTTPRoutes(t *testing.T) {
	tempDir := t.TempDir()
	svc, err := newGluttonService(tempDir)
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	defer svc.Close()

	ts := httptest.NewServer(newMux(svc))
	defer ts.Close()

	// 1. /readyz GET -> 200 OK
	res, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz status: got %d, want 200", res.StatusCode)
	}
	res.Body.Close()

	// 2. GET on /ping -> 405 Method Not Allowed
	res, err = http.Get(ts.URL + "/ping")
	if err != nil {
		t.Fatalf("GET /ping failed: %v", err)
	}
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET /ping status: got %d, want 405", res.StatusCode)
	}
	res.Body.Close()

	// 3. POST bad body -> 400 Bad Request
	res, err = http.Post(ts.URL+"/ping", "application/x-protobuf", bytes.NewReader([]byte("garbage")))
	if err != nil {
		t.Fatalf("POST /ping garbage failed: %v", err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /ping garbage status: got %d, want 400", res.StatusCode)
	}
	res.Body.Close()

	// 4. POST /ping -> 200 OK & protobuf Content-Type & ServerElapsedTrailer & echo message
	pingReqBytes, _ := proto.Marshal(&glutton.PingRequest{Message: "hello"})
	res, err = http.Post(ts.URL+"/ping", "application/x-protobuf", bytes.NewReader(pingReqBytes))
	if err != nil {
		t.Fatalf("POST /ping failed: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST /ping status: got %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("POST /ping Content-Type: got %q, want application/x-protobuf", ct)
	}
	if elapsed := res.Header.Get(ateinterceptors.ServerElapsedTrailer); elapsed == "" {
		t.Errorf("POST /ping missing header %q", ateinterceptors.ServerElapsedTrailer)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	var pingResp glutton.PingResponse
	if err := proto.Unmarshal(body, &pingResp); err != nil {
		t.Fatalf("unmarshal PingResponse failed: %v", err)
	}
	if pingResp.GetMessage() != "hello" {
		t.Errorf("PingResponse message: got %q, want 'hello'", pingResp.GetMessage())
	}

	// 5. POST /writedisk -> 200 OK & protobuf Content-Type
	writeReqBytes, _ := proto.Marshal(&glutton.WriteDiskRequest{
		Key:       "httpfile",
		Size:      512,
		WriteMode: glutton.WriteMode_WRITE_MODE_TRUNCATE,
	})
	res, err = http.Post(ts.URL+"/writedisk", "application/x-protobuf", bytes.NewReader(writeReqBytes))
	if err != nil {
		t.Fatalf("POST /writedisk failed: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST /writedisk status: got %d, want 200", res.StatusCode)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	var writeResp glutton.WriteDiskResponse
	if err := proto.Unmarshal(body, &writeResp); err != nil {
		t.Fatalf("unmarshal WriteDiskResponse failed: %v", err)
	}
	if writeResp.GetSize() != 512 {
		t.Errorf("WriteDiskResponse size: got %d, want 512", writeResp.GetSize())
	}

	// 6. POST /readdisk -> 200 OK & matching size & digest
	readReqBytes, _ := proto.Marshal(&glutton.ReadDiskRequest{Key: "httpfile"})
	res, err = http.Post(ts.URL+"/readdisk", "application/x-protobuf", bytes.NewReader(readReqBytes))
	if err != nil {
		t.Fatalf("POST /readdisk failed: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("POST /readdisk status: got %d, want 200", res.StatusCode)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	var readResp glutton.ReadDiskResponse
	if err := proto.Unmarshal(body, &readResp); err != nil {
		t.Fatalf("unmarshal ReadDiskResponse failed: %v", err)
	}
	if readResp.GetSize() != 512 {
		t.Errorf("ReadDiskResponse size: got %d, want 512", readResp.GetSize())
	}
	if !bytes.Equal(readResp.GetSha256(), writeResp.GetSha256()) {
		t.Errorf("sha256 mismatch over HTTP between writedisk and readdisk")
	}

	// 7. unknown key -> 404 (NotFound mapping)
	missBytes, _ := proto.Marshal(&glutton.ReadDiskRequest{Key: "nosuchfile"})
	res, err = http.Post(ts.URL+"/readdisk", "application/x-protobuf", bytes.NewReader(missBytes))
	if err != nil {
		t.Fatalf("POST /readdisk miss failed: %v", err)
	}
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("POST /readdisk miss status: got %d, want 404", res.StatusCode)
	}
	res.Body.Close()

	// 8. traversal key -> 400 (InvalidArgument mapping)
	badBytes, _ := proto.Marshal(&glutton.ReadDiskRequest{Key: "../etc/passwd"})
	res, err = http.Post(ts.URL+"/readdisk", "application/x-protobuf", bytes.NewReader(badBytes))
	if err != nil {
		t.Fatalf("POST /readdisk bad key failed: %v", err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("POST /readdisk bad key status: got %d, want 400", res.StatusCode)
	}
	res.Body.Close()
}

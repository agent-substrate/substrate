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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// newTestService builds a Service rooted in a per-test temp dir.
func newTestService(t *testing.T) *Service {
	t.Helper()
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(svc.Close)
	return svc
}

func TestBurnCPU(t *testing.T) {
	svc := newTestService(t)

	start := time.Now()
	resp, err := svc.BurnCPU(context.Background(), &gluttonpb.BurnCPURequest{
		DurationMs:  50,
		Parallelism: 2,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("BurnCPU: %v", err)
	}
	if resp.GetIterations() <= 0 {
		t.Errorf("BurnCPU iterations = %d, want > 0", resp.GetIterations())
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("BurnCPU returned after %v, want >= 50ms", elapsed)
	}
}

func TestBurnCPU_ZeroDurationReturnsImmediately(t *testing.T) {
	svc := newTestService(t)
	resp, err := svc.BurnCPU(context.Background(), &gluttonpb.BurnCPURequest{})
	if err != nil {
		t.Fatalf("BurnCPU: %v", err)
	}
	if resp.GetIterations() != 0 {
		t.Errorf("BurnCPU iterations = %d, want 0 for zero duration", resp.GetIterations())
	}
}

func TestBurnCPU_NegativeDurationRejected(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.BurnCPU(context.Background(), &gluttonpb.BurnCPURequest{DurationMs: -1})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("BurnCPU(-1ms) code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestIngest(t *testing.T) {
	svc := newTestService(t)
	payload := []byte("pretend this is a repository tarball")

	resp, err := svc.Ingest(context.Background(), &gluttonpb.IngestRequest{
		Key:     "clone",
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if resp.GetSize() != int64(len(payload)) {
		t.Errorf("Ingest size = %d, want %d", resp.GetSize(), len(payload))
	}
	want := sha256.Sum256(payload)
	if !bytes.Equal(resp.GetSha256(), want[:]) {
		t.Errorf("Ingest sha256 mismatch")
	}
	got, err := os.ReadFile(filepath.Join(svc.dataDir, "clone"))
	if err != nil {
		t.Fatalf("read ingested file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("ingested file contents differ from payload")
	}
}

func TestIngest_AppendGrowsFile(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()

	if _, err := svc.Ingest(ctx, &gluttonpb.IngestRequest{Key: "deps", Payload: []byte("aaaa")}); err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	resp, err := svc.Ingest(ctx, &gluttonpb.IngestRequest{Key: "deps", Payload: []byte("bb"), Append: true})
	if err != nil {
		t.Fatalf("append Ingest: %v", err)
	}
	if resp.GetSize() != 6 {
		t.Errorf("appended file size = %d, want 6", resp.GetSize())
	}
}

func TestIngest_RejectsPathEscape(t *testing.T) {
	svc := newTestService(t)
	_, err := svc.Ingest(context.Background(), &gluttonpb.IngestRequest{
		Key:     "../escape",
		Payload: []byte("x"),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("Ingest(../escape) code = %v, want InvalidArgument", status.Code(err))
	}
}

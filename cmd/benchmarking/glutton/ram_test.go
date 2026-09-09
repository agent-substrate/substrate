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
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/glutton"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newRAMTestService(t *testing.T) *gluttonService {
	t.Helper()
	svc, err := newGluttonService(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create glutton service: %v", err)
	}
	t.Cleanup(svc.Close)
	return svc
}

func fillRAM(t *testing.T, svc *gluttonService, key, size string) {
	t.Helper()
	_, err := svc.WriteRAM(context.Background(), &glutton.WriteRAMRequest{
		Key: key, Size: size, WriteMode: glutton.WriteMode_WRITE_MODE_TRUNCATE,
	})
	if err != nil {
		t.Fatalf("WriteRAM truncate %s (%s): %v", key, size, err)
	}
}

func rotateRAM(t *testing.T, svc *gluttonService, key, size string) {
	t.Helper()
	_, err := svc.WriteRAM(context.Background(), &glutton.WriteRAMRequest{
		Key: key, Size: size, WriteMode: glutton.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
	})
	if err != nil {
		t.Fatalf("WriteRAM rotate %s (%s): %v", key, size, err)
	}
}

// ramCopy snapshots the current bytes of a RAM array for change comparison.
func ramCopy(svc *gluttonService, key string) []byte {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return append([]byte(nil), svc.ram[key]...)
}

func TestReadRAMWalksArray(t *testing.T) {
	svc := newRAMTestService(t)
	ctx := context.Background()
	fillRAM(t, svc, "m", "64Ki")

	whole, err := svc.ReadRAM(ctx, &glutton.ReadRAMRequest{Key: "m"})
	if err != nil {
		t.Fatalf("ReadRAM whole: %v", err)
	}
	if whole.GetSize() != 64<<10 {
		t.Errorf("whole walk size = %d, want %d", whole.GetSize(), 64<<10)
	}

	again, err := svc.ReadRAM(ctx, &glutton.ReadRAMRequest{Key: "m"})
	if err != nil {
		t.Fatalf("ReadRAM repeat: %v", err)
	}
	if again.GetChecksum() != whole.GetChecksum() {
		t.Errorf("repeat checksum = %d, want %d (walk must be deterministic)", again.GetChecksum(), whole.GetChecksum())
	}

	partial, err := svc.ReadRAM(ctx, &glutton.ReadRAMRequest{Key: "m", Size: "4Ki"})
	if err != nil {
		t.Fatalf("ReadRAM partial: %v", err)
	}
	if partial.GetSize() != 4<<10 {
		t.Errorf("partial walk size = %d, want %d", partial.GetSize(), 4<<10)
	}

	clamped, err := svc.ReadRAM(ctx, &glutton.ReadRAMRequest{Key: "m", Size: "1Gi"})
	if err != nil {
		t.Fatalf("ReadRAM oversized: %v", err)
	}
	if clamped.GetSize() != 64<<10 {
		t.Errorf("oversized walk size = %d, want %d (clamped to the array)", clamped.GetSize(), 64<<10)
	}
}

func TestReadRAMErrors(t *testing.T) {
	svc := newRAMTestService(t)
	ctx := context.Background()
	fillRAM(t, svc, "m", "4Ki")

	tests := []struct {
		name string
		req  *glutton.ReadRAMRequest
		code codes.Code
	}{
		{name: "empty key", req: &glutton.ReadRAMRequest{}, code: codes.InvalidArgument},
		{name: "missing key", req: &glutton.ReadRAMRequest{Key: "nope"}, code: codes.NotFound},
		{name: "bad size", req: &glutton.ReadRAMRequest{Key: "m", Size: "lots"}, code: codes.InvalidArgument},
		{name: "negative size", req: &glutton.ReadRAMRequest{Key: "m", Size: "-1"}, code: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.ReadRAM(ctx, tt.req)
			if status.Code(err) != tt.code {
				t.Errorf("ReadRAM error = %v, want code %v", err, tt.code)
			}
		})
	}
}

// TestWriteRAMRotateAdvances verifies consecutive rotates dirty a moving
// window: first [0,4Ki), then [4Ki,8Ki), never the untouched remainder. A
// 4Ki random block matching its previous contents is astronomically
// unlikely, so byte comparison is a reliable change detector.
func TestWriteRAMRotateAdvances(t *testing.T) {
	svc := newRAMTestService(t)
	fillRAM(t, svc, "m", "8Ki")
	before := ramCopy(svc, "m")

	rotateRAM(t, svc, "m", "4Ki")
	afterFirst := ramCopy(svc, "m")
	if bytes.Equal(before[:4<<10], afterFirst[:4<<10]) {
		t.Error("first rotate left [0,4Ki) unchanged")
	}
	if !bytes.Equal(before[4<<10:], afterFirst[4<<10:]) {
		t.Error("first rotate touched [4Ki,8Ki)")
	}

	rotateRAM(t, svc, "m", "4Ki")
	afterSecond := ramCopy(svc, "m")
	if !bytes.Equal(afterFirst[:4<<10], afterSecond[:4<<10]) {
		t.Error("second rotate touched [0,4Ki); want the window to advance")
	}
	if bytes.Equal(afterFirst[4<<10:], afterSecond[4<<10:]) {
		t.Error("second rotate left [4Ki,8Ki) unchanged")
	}
}

// TestWriteRAMRotateWraps drives the cursor past the end of the array: an
// 8Ki array rotated by 6Ki twice writes [6Ki,8Ki) plus the wrapped [0,4Ki)
// on the second call, leaving [4Ki,6Ki) untouched.
func TestWriteRAMRotateWraps(t *testing.T) {
	svc := newRAMTestService(t)
	fillRAM(t, svc, "m", "8Ki")

	rotateRAM(t, svc, "m", "6Ki")
	afterFirst := ramCopy(svc, "m")

	rotateRAM(t, svc, "m", "6Ki")
	afterSecond := ramCopy(svc, "m")
	if bytes.Equal(afterFirst[6<<10:], afterSecond[6<<10:]) {
		t.Error("wrapping rotate left [6Ki,8Ki) unchanged")
	}
	if bytes.Equal(afterFirst[:4<<10], afterSecond[:4<<10]) {
		t.Error("wrapping rotate left the wrapped [0,4Ki) unchanged")
	}
	if !bytes.Equal(afterFirst[4<<10:6<<10], afterSecond[4<<10:6<<10]) {
		t.Error("wrapping rotate touched [4Ki,6Ki)")
	}
}

func TestWriteRAMRotateClampsAndKeepsRotating(t *testing.T) {
	svc := newRAMTestService(t)
	fillRAM(t, svc, "m", "4Ki")
	before := ramCopy(svc, "m")

	// Oversized rotate rewrites the whole array and must not corrupt the
	// cursor: the follow-up rotate still succeeds.
	rotateRAM(t, svc, "m", "1Mi")
	if bytes.Equal(before, ramCopy(svc, "m")) {
		t.Error("oversized rotate left the array unchanged")
	}
	rotateRAM(t, svc, "m", "1Ki")
}

func TestWriteRAMRotateNeedsExistingArray(t *testing.T) {
	svc := newRAMTestService(t)
	_, err := svc.WriteRAM(context.Background(), &glutton.WriteRAMRequest{
		Key: "nope", Size: "4Ki", WriteMode: glutton.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("rotate on missing array = %v, want NotFound", err)
	}
}

// TestWriteRAMTruncateResetsRotateCursor re-fills after rotating and checks
// the next rotate starts back at the head of the new array.
func TestWriteRAMTruncateResetsRotateCursor(t *testing.T) {
	svc := newRAMTestService(t)
	fillRAM(t, svc, "m", "8Ki")
	rotateRAM(t, svc, "m", "4Ki") // cursor now 4Ki

	fillRAM(t, svc, "m", "8Ki") // reallocates; cursor must reset
	before := ramCopy(svc, "m")

	rotateRAM(t, svc, "m", "4Ki")
	after := ramCopy(svc, "m")
	if bytes.Equal(before[:4<<10], after[:4<<10]) {
		t.Error("rotate after truncate left [0,4Ki) unchanged; cursor was not reset")
	}
	if !bytes.Equal(before[4<<10:], after[4<<10:]) {
		t.Error("rotate after truncate touched [4Ki,8Ki)")
	}
}

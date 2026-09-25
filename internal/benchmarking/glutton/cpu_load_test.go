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
	"runtime"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// TestUseCPULifecycle covers start, replace, and stop through a series of
// UseCPU calls. Each call replaces the running pool, so the reported and
// observed worker counts must track the latest request.
func TestUseCPULifecycle(t *testing.T) {
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()

	// Start 2 workers.
	resp, err := svc.UseCPU(ctx, &gluttonpb.UseCPURequest{
		NumCores:      2,
		DutyCycle:     0.1,
		CycleLengthMs: 20,
	})
	if err != nil {
		t.Fatalf("UseCPU start: %v", err)
	}
	if resp.GetNumCores() != 2 {
		t.Errorf("UseCPU start num_cores = %d, want 2", resp.GetNumCores())
	}
	if got := svc.cpu.N(); got != 2 {
		t.Errorf("cpu.N() after start = %d, want 2", got)
	}

	// Replacing with a smaller pool should stop the old goroutines and
	// leave the new count behind.
	resp, err = svc.UseCPU(ctx, &gluttonpb.UseCPURequest{
		NumCores:      1,
		DutyCycle:     0.1,
		CycleLengthMs: 20,
	})
	if err != nil {
		t.Fatalf("UseCPU replace: %v", err)
	}
	if resp.GetNumCores() != 1 {
		t.Errorf("UseCPU replace num_cores = %d, want 1", resp.GetNumCores())
	}
	if got := svc.cpu.N(); got != 1 {
		t.Errorf("cpu.N() after replace = %d, want 1", got)
	}

	// num_cores=0 stops all workers.
	resp, err = svc.UseCPU(ctx, &gluttonpb.UseCPURequest{})
	if err != nil {
		t.Fatalf("UseCPU stop: %v", err)
	}
	if resp.GetNumCores() != 0 {
		t.Errorf("UseCPU stop num_cores = %d, want 0", resp.GetNumCores())
	}
	if got := svc.cpu.N(); got != 0 {
		t.Errorf("cpu.N() after stop = %d, want 0", got)
	}
}

// TestUseCPUCapAtGomaxprocs covers the three cap_at_gomaxprocs states:
// unset (default true), explicitly true, and explicitly false.
func TestUseCPUCapAtGomaxprocs(t *testing.T) {
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	prev := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	ctx := context.Background()
	requested := int32(8)
	trueVal := true
	falseVal := false

	tests := []struct {
		name    string
		capFlag *bool
		want    int32
	}{
		{name: "unset defaults to capped", capFlag: nil, want: 2},
		{name: "explicit true is capped", capFlag: &trueVal, want: 2},
		{name: "explicit false is uncapped", capFlag: &falseVal, want: requested},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := svc.UseCPU(ctx, &gluttonpb.UseCPURequest{
				NumCores:        requested,
				DutyCycle:       0.01,
				CycleLengthMs:   50,
				CapAtGomaxprocs: tt.capFlag,
			})
			if err != nil {
				t.Fatalf("UseCPU: %v", err)
			}
			if resp.GetNumCores() != tt.want {
				t.Errorf("num_cores = %d, want %d (GOMAXPROCS=2)", resp.GetNumCores(), tt.want)
			}
			// Reset between subtests so the pool doesn't leak.
			if _, err := svc.UseCPU(ctx, &gluttonpb.UseCPURequest{}); err != nil {
				t.Fatalf("UseCPU stop: %v", err)
			}
		})
	}
}

// TestUseCPURejectsInvalidArgs pins the request validation.
func TestUseCPURejectsInvalidArgs(t *testing.T) {
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	tests := []struct {
		name string
		req  *gluttonpb.UseCPURequest
	}{
		{name: "negative num_cores", req: &gluttonpb.UseCPURequest{NumCores: -1}},
		{name: "duty_cycle below range", req: &gluttonpb.UseCPURequest{NumCores: 1, DutyCycle: -0.1}},
		{name: "duty_cycle above range", req: &gluttonpb.UseCPURequest{NumCores: 1, DutyCycle: 1.5}},
		{name: "negative cycle_length_ms", req: &gluttonpb.UseCPURequest{NumCores: 1, CycleLengthMs: -5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.UseCPU(ctx, tt.req)
			if err == nil {
				t.Fatalf("UseCPU: expected error, got nil")
			}
			if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
				t.Errorf("UseCPU: expected InvalidArgument, got %v", err)
			}
		})
	}
}

// TestUseCPUBurnsCPUTime verifies that the worker actually consumes CPU
// (not just wall time). At 100% duty for a short window, the process's
// user CPU time should advance by roughly the wall time on an unloaded
// system.
func TestUseCPUBurnsCPUTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CPU burn timing test in short mode")
	}
	svc, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()
	startUser := processUserCPUTime(t)
	if _, err := svc.UseCPU(ctx, &gluttonpb.UseCPURequest{
		NumCores:      1,
		DutyCycle:     1.0,
		CycleLengthMs: 10,
	}); err != nil {
		t.Fatalf("UseCPU: %v", err)
	}

	const window = 300 * time.Millisecond
	time.Sleep(window)

	if _, err := svc.UseCPU(ctx, &gluttonpb.UseCPURequest{}); err != nil {
		t.Fatalf("UseCPU stop: %v", err)
	}
	delta := processUserCPUTime(t) - startUser

	// Comfortable lower bound (~50% of the window) to keep this from
	// flaking under a loaded CI runner; the goal is to prove the worker
	// is burning CPU, not to certify accuracy.
	minWant := window / 2
	if delta < minWant {
		t.Errorf("user CPU time delta = %v, want >= %v over %v of 100%%-duty load", delta, minWant, window)
	}
}

func processUserCPUTime(t *testing.T) time.Duration {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatalf("Getrusage: %v", err)
	}
	return time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
}

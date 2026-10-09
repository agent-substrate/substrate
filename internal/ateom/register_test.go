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

package ateom

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// testActors stands in for the ateom's own ceiling, which is a flag in
// production rather than a constant here.
const testActors = 7

func TestFromFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cpu    string
		memory string
		want   *ateletpb.WorkerResources
	}{
		{name: "limits set", cpu: "2000", memory: "4294967296", want: &ateletpb.WorkerResources{Actors: testActors, Resources: &ateletpb.Resources{
			Limits: []*ateletpb.Limits{{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "4Gi"}},
		}}},
		{name: "unparseable is none", cpu: "2Gi", memory: "", want: &ateletpb.WorkerResources{Actors: testActors}},
		{name: "negative is none", cpu: "-1", memory: "-1", want: &ateletpb.WorkerResources{Actors: testActors}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, CPULimitFile), []byte(tc.cpu), 0o600); err != nil {
				t.Fatalf("writing CPU limit: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, MemoryLimitFile), []byte(tc.memory), 0o600); err != nil {
				t.Fatalf("writing memory limit: %v", err)
			}

			got := fromDir(dir, testActors)
			if diff := cmp.Diff(tc.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("reported capacity mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromFilesMissing(t *testing.T) {
	got := fromDir(t.TempDir(), testActors)
	want := &ateletpb.WorkerResources{Actors: testActors}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("unset environment capacity mismatch (-want +got):\n%s", diff)
	}
}

// TestBuildRequest checks what buildRequest itself contributes: the actor
// ceiling and the caller's hardware identity. Capacity limits come from the
// production mount path, which may or may not exist where the test runs, so
// their parsing is covered by TestFromFiles instead.
func TestBuildRequest(t *testing.T) {
	hw := &ateletpb.HardwareIdentity{
		Attributes: map[string]string{
			"architecture": "amd64",
			"cpu_features": "e2f3bc8848800ca6",
		},
	}
	got := buildRequest(ReportConfig{
		Actors:   testActors,
		Hardware: hw,
	})
	if got.GetCapacity().GetActors() != testActors {
		t.Errorf("buildRequest() actors = %d, want %d", got.GetCapacity().GetActors(), testActors)
	}
	if diff := cmp.Diff(hw, got.GetHardware(), protocmp.Transform()); diff != "" {
		t.Errorf("buildRequest() hardware mismatch (-want +got):\n%s", diff)
	}
}

// reportSeam swaps the one-shot call out so the retry loop can be exercised
// without a socket or certificates.
func TestReportRetriesUntilAccepted(t *testing.T) {
	attempts := 0
	send := func() error {
		attempts++
		if attempts < 3 {
			return errors.New("worker record does not exist yet")
		}
		return nil
	}
	if err := retryReport(context.Background(), send, time.Millisecond); err != nil {
		t.Fatalf("retryReport() failed: %v", err)
	}
	if attempts != 3 {
		t.Errorf("gave up after %d attempts, want 3", attempts)
	}
}

func TestReportStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	send := func() error {
		attempts++
		if attempts == 2 {
			cancel()
		}
		return errors.New("still failing")
	}
	if err := retryReport(ctx, send, time.Millisecond); !errors.Is(err, context.Canceled) {
		t.Errorf("retryReport() = %v, want context.Canceled", err)
	}
}

// A misconfiguration must surface rather than spin in the retry loop: the
// caller exits on it, and retrying forever would leave the worker idle and
// silent instead.
func TestReportFailsFastOnBadCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Report(ctx, ReportConfig{
		SocketPath:           filepath.Join(t.TempDir(), "atelet.sock"),
		CredentialBundlePath: filepath.Join(t.TempDir(), "does-not-exist.pem"),
		TrustBundlePath:      filepath.Join(t.TempDir(), "also-missing.pem"),
		Hardware:             &ateletpb.HardwareIdentity{Attributes: map[string]string{"architecture": "amd64"}},
	})
	if err == nil {
		t.Fatal("Report() with unreadable credentials succeeded, want an error the caller can exit on")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
	}
}

func TestReportFailsFastOnNilHardware(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := Report(ctx, ReportConfig{}); err == nil {
		t.Fatal("Report() with nil Hardware succeeded, want an error the caller can exit on")
	}
}

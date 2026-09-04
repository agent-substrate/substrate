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

package ateomcapacity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestFromFiles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cpu    string
		memory string
		want   *ateapipb.Resources
	}{
		{name: "limits set", cpu: "2000", memory: "4294967296", want: resources.CPUMemory(2000, 4294967296)},
		{name: "unparseable is none", cpu: "2Gi", memory: "", want: nil},
		{name: "negative is none", cpu: "-1", memory: "-1", want: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, CPULimitFile), []byte(tc.cpu), 0o600); err != nil {
				t.Fatalf("writing CPU limit: %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, MemoryLimitFile), []byte(tc.memory), 0o600); err != nil {
				t.Fatalf("writing memory limit: %v", err)
			}

			got := fromDir(dir).GetCapacity()
			if got.GetActors() != actorsPerAteom {
				t.Errorf("actors = %d, want %d", got.GetActors(), actorsPerAteom)
			}
			if diff := cmp.Diff(tc.want, got.GetResources(), protocmp.Transform()); diff != "" {
				t.Errorf("reported resources mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromFilesMissing(t *testing.T) {
	got := fromDir(t.TempDir()).GetCapacity()
	if got.GetResources() != nil {
		t.Errorf("unset environment reported %v, want no compute", got.GetResources())
	}
	if got.GetActors() != actorsPerAteom {
		t.Errorf("actors = %d, want %d", got.GetActors(), actorsPerAteom)
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
	})
	if err == nil {
		t.Fatal("Report() with unreadable credentials succeeded, want an error the caller can exit on")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Report() retried a permanent failure until the deadline: %v", err)
	}
}

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actoroperation

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestTryAcquireExcludesAndReleases(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")

	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() failed: %v", err)
	}
	if _, err := TryAcquire(actorPath); !errors.Is(err, ErrLocked) {
		t.Fatalf("overlapping TryAcquire() error = %v, want ErrLocked", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Release() failed: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("second Release() failed: %v", err)
	}

	reacquired, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() after release failed: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatalf("reacquired Release() failed: %v", err)
	}
}

func TestAcquireHonorsContext(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")
	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() failed: %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Release(); err != nil {
			t.Errorf("Release() failed: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := Acquire(ctx, actorPath); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Acquire() error = %v, want DeadlineExceeded", err)
	}
}

func TestAcquireDoesNotTakeFreeLockAfterCancellation(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := Acquire(ctx, actorPath); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire() error = %v, want Canceled", err)
	}

	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() after canceled Acquire failed: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Release() failed: %v", err)
	}
}

func TestOperationIDRejectsStaleOperation(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")
	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() failed: %v", err)
	}
	defer lock.Release()

	if err := lock.SetOperationID("operation-1"); err != nil {
		t.Fatalf("SetOperationID(operation-1) failed: %v", err)
	}
	if exists, err := lock.HasOperationID(); err != nil || !exists {
		t.Fatalf("HasOperationID() = %v, %v, want true, nil", exists, err)
	}
	if err := lock.CheckOperationID("operation-1"); err != nil {
		t.Fatalf("CheckOperationID(operation-1) failed: %v", err)
	}
	if err := lock.SetOperationID("operation-2"); err != nil {
		t.Fatalf("SetOperationID(operation-2) failed: %v", err)
	}
	if err := lock.CheckOperationID("operation-1"); !errors.Is(err, ErrOperationChanged) {
		t.Fatalf("stale CheckOperationID() error = %v, want ErrOperationChanged", err)
	}
}

func TestMissingOperationIDIsStale(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")
	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() failed: %v", err)
	}
	defer lock.Release()

	if err := lock.CheckOperationID("operation-1"); !errors.Is(err, ErrOperationChanged) {
		t.Fatalf("CheckOperationID() error = %v, want ErrOperationChanged", err)
	}
}

func TestHasOperationIDIsFalseBeforeFirstGeneration(t *testing.T) {
	actorPath := filepath.Join(t.TempDir(), "actor")
	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() failed: %v", err)
	}
	defer lock.Release()

	if exists, err := lock.HasOperationID(); err != nil || exists {
		t.Fatalf("HasOperationID() = %v, %v, want false, nil", exists, err)
	}
}

func TestTryAcquireExcludesAnotherProcess(t *testing.T) {
	if os.Getenv("ACTOR_OPERATION_LOCK_HELPER") == "1" {
		actorPath := os.Getenv("ACTOR_OPERATION_LOCK_PATH")
		readyPath := os.Getenv("ACTOR_OPERATION_READY_PATH")
		releasePath := os.Getenv("ACTOR_OPERATION_RELEASE_PATH")

		lock, err := TryAcquire(actorPath)
		if err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(readyPath, nil, 0o600); err != nil {
			os.Exit(3)
		}
		if os.Getenv("ACTOR_OPERATION_EXIT_AFTER_READY") == "1" {
			os.Exit(0)
		}
		for {
			if _, err := os.Stat(releasePath); err == nil {
				break
			} else if !errors.Is(err, os.ErrNotExist) {
				os.Exit(4)
			}
			time.Sleep(time.Millisecond)
		}
		if err := lock.Release(); err != nil {
			os.Exit(5)
		}
		os.Exit(0)
	}

	tmpDir := t.TempDir()
	actorPath := filepath.Join(tmpDir, "actor")
	readyPath := filepath.Join(tmpDir, "ready")
	releasePath := filepath.Join(tmpDir, "release")
	cmd := exec.Command(os.Args[0], "-test.run=TestTryAcquireExcludesAnotherProcess")
	cmd.Env = append(os.Environ(),
		"ACTOR_OPERATION_LOCK_HELPER=1",
		"ACTOR_OPERATION_LOCK_PATH="+actorPath,
		"ACTOR_OPERATION_READY_PATH="+readyPath,
		"ACTOR_OPERATION_RELEASE_PATH="+releasePath,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting helper: %v", err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(releasePath, nil, 0o600)
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("checking helper readiness: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for helper to acquire lock")
		}
		time.Sleep(time.Millisecond)
	}

	if _, err := TryAcquire(actorPath); !errors.Is(err, ErrLocked) {
		t.Fatalf("TryAcquire() while helper holds lock error = %v, want ErrLocked", err)
	}
	if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
		t.Fatalf("releasing helper: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("helper failed: %v", err)
	}

	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() after helper exited failed: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Release() failed: %v", err)
	}
}

func TestLockReleasedWhenProcessExits(t *testing.T) {
	tmpDir := t.TempDir()
	actorPath := filepath.Join(tmpDir, "actor")
	readyPath := filepath.Join(tmpDir, "ready")
	cmd := exec.Command(os.Args[0], "-test.run=TestTryAcquireExcludesAnotherProcess")
	cmd.Env = append(os.Environ(),
		"ACTOR_OPERATION_LOCK_HELPER=1",
		"ACTOR_OPERATION_EXIT_AFTER_READY=1",
		"ACTOR_OPERATION_LOCK_PATH="+actorPath,
		"ACTOR_OPERATION_READY_PATH="+readyPath,
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("helper failed: %v", err)
	}
	if _, err := os.Stat(readyPath); err != nil {
		t.Fatalf("helper did not report lock acquisition: %v", err)
	}

	lock, err := TryAcquire(actorPath)
	if err != nil {
		t.Fatalf("TryAcquire() after helper exit failed: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatalf("Release() failed: %v", err)
	}
}

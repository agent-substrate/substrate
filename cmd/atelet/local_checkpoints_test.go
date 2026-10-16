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
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/resources"
)

func writeSnapshotDir(t *testing.T, dir, prefix string) {
	t.Helper()
	p := filepath.Join(dir, prefix)
	if err := os.MkdirAll(p, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p, "memory.img"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPruneRemovesEverySnapshot(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotDir(t, dir, "pause-1")
	writeSnapshotDir(t, dir, "pause-2")
	writeSnapshotDir(t, dir, "pause-3")

	if err := pruneLocalCheckpointDir(context.Background(), dir); err != nil {
		t.Fatalf("pruneLocalCheckpointDir() = %v, want nil", err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dir still exists (err=%v), want removed entirely", err)
	}
}

func TestPruneMissingDirIsNoop(t *testing.T) {
	if err := pruneLocalCheckpointDir(context.Background(), filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatalf("pruneLocalCheckpointDir() = %v, want nil", err)
	}
}

// An undeletable snapshot must be reported — Terminate turns that error into a
// failed RPC so the delete workflow retries — without stranding the snapshots
// that could have been removed.
func TestPruneReportsFailureAndStillRemovesTheRest(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions, so no snapshot can be made undeletable")
	}
	dir := t.TempDir()
	writeSnapshotDir(t, dir, "pause-1")
	writeSnapshotDir(t, dir, "pause-2")

	// A snapshot dir with no write bit: its files cannot be unlinked.
	stuck := filepath.Join(dir, "pause-stuck")
	writeSnapshotDir(t, dir, "pause-stuck")
	if err := os.Chmod(stuck, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stuck, 0o700) })

	err := pruneLocalCheckpointDir(context.Background(), dir)
	if err == nil {
		t.Fatal("pruneLocalCheckpointDir() = nil, want an error naming the undeletable snapshot")
	}
	if !strings.Contains(err.Error(), "pause-stuck") {
		t.Errorf("pruneLocalCheckpointDir() = %v, want it to name pause-stuck", err)
	}
	for _, name := range []string{"pause-1", "pause-2"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s still exists (err=%v), want removed", name, err)
		}
	}
}

func TestPruneKeepsTheNamedSnapshot(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"pause-1", "pause-2"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name, "checkpoint.img"), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	plan, err := planLocalCheckpointPruneDir(dir, "pause-2")
	if err != nil {
		t.Fatalf("planLocalCheckpointPruneDir() = %v, want nil", err)
	}
	if want := []string{filepath.Join(dir, "pause-1")}; !slices.Equal(plan.paths, want) {
		t.Fatalf("planned paths = %v, want %v", plan.paths, want)
	}
	// A snapshot written after the plan is not the plan's to remove.
	if err := os.MkdirAll(filepath.Join(dir, "pause-3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := plan.run(context.Background()); err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "pause-1")); !os.IsNotExist(err) {
		t.Errorf("pause-1 survived the prune (stat err = %v)", err)
	}
	for _, name := range []string{"pause-2", "pause-3"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s did not survive the prune: %v", name, err)
		}
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("the local checkpoints dir was removed although a snapshot was kept: %v", err)
	}
}

func TestPruneKeepOfMissingDirPlansNothing(t *testing.T) {
	plan, err := planLocalCheckpointPruneDir(filepath.Join(t.TempDir(), "absent"), "pause-1")
	if err != nil {
		t.Fatalf("planLocalCheckpointPruneDir() = %v, want nil", err)
	}
	if len(plan.paths) != 0 {
		t.Fatalf("planned paths = %v, want none", plan.paths)
	}
	if err := plan.run(context.Background()); err != nil {
		t.Fatalf("run() = %v, want nil", err)
	}
}

// blockRemovals swaps removeLocalCheckpoint for one that blocks until release
// is closed, as a disk that is far behind would; started receives once per
// blocked call.
func blockRemovals(t *testing.T) (started <-chan struct{}, release chan struct{}) {
	t.Helper()
	rel := make(chan struct{})
	st := make(chan struct{}, 8)
	orig := removeLocalCheckpoint
	removeLocalCheckpoint = func(path string) error {
		select {
		case st <- struct{}{}:
		default:
		}
		<-rel
		return orig(path)
	}
	t.Cleanup(func() { removeLocalCheckpoint = orig })
	return st, rel
}

// mkSnapshotDir is writeSnapshotDir without t.Fatal, for use off the test
// goroutine.
func mkSnapshotDir(dir, name string) error {
	if err := os.MkdirAll(filepath.Join(dir, name), 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name, "memory.img"), []byte("x"), 0o600)
}

func TestPruneAfterResponseWaitsForThePreviousRemoval(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "actor-uid-backpressure"
	dir := ateletpath.LocalCheckpointsDir(actorUID)
	for _, name := range []string{"pause-1", "pause-2"} {
		writeSnapshotDir(t, dir, name)
	}
	started, release := blockRemovals(t)

	s := &AteomHerder{}
	ref := resources.ActorRef{Atespace: "ate-demo", Name: "counter"}
	s.pruneLocalCheckpointsAfterResponse(context.Background(), ref, actorUID, "pause-2") // removes pause-1, slowly
	<-started

	// The next pause must wait for that removal before it writes anything:
	// at most one superseded snapshot per actor is ever pending.
	done := make(chan error, 1)
	go func() {
		if err := s.waitForLocalCheckpointRemoval(context.Background(), ref, actorUID); err != nil {
			done <- err
			return
		}
		if err := mkSnapshotDir(dir, "pause-3"); err != nil {
			done <- err
			return
		}
		s.pruneLocalCheckpointsAfterResponse(context.Background(), ref, actorUID, "pause-3")
		done <- nil
	}()
	select {
	case err := <-done:
		t.Fatalf("second pause returned (%v) while the previous removal was still running", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second pause: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second pause did not return after the previous removal finished")
	}
	s.localCheckpointPrunes.Wait()
	for name, want := range map[string]bool{"pause-1": false, "pause-2": false, "pause-3": true} {
		_, err := os.Stat(filepath.Join(dir, name))
		if got := err == nil; got != want {
			t.Errorf("%s exists = %v, want %v (err %v)", name, got, want, err)
		}
	}
	s.pendingRemovalsMu.Lock()
	left := len(s.pendingRemovals)
	s.pendingRemovalsMu.Unlock()
	if left != 0 {
		t.Errorf("%d pending-removal entries left after every removal finished, want 0", left)
	}
}

func TestWaitForLocalCheckpointRemovalHonorsContext(t *testing.T) {
	useTempNodeDirs(t)
	const actorUID = "actor-uid-ctx"
	dir := ateletpath.LocalCheckpointsDir(actorUID)
	writeSnapshotDir(t, dir, "pause-1")
	writeSnapshotDir(t, dir, "pause-2")
	started, release := blockRemovals(t)

	s := &AteomHerder{}
	ref := resources.ActorRef{Atespace: "ate-demo", Name: "counter"}
	s.pruneLocalCheckpointsAfterResponse(context.Background(), ref, actorUID, "pause-2")
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := s.waitForLocalCheckpointRemoval(ctx, ref, actorUID)
	// Let the removal finish before the test's temp dirs go away.
	close(release)
	s.localCheckpointPrunes.Wait()
	// The deadline comes back with its own gRPC code, so the control plane
	// retries instead of treating it as an internal failure.
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("waitForLocalCheckpointRemoval() = %v, want gRPC code DeadlineExceeded", err)
	}
	// Once the removal has finished, the same wait returns at once.
	if err := s.waitForLocalCheckpointRemoval(context.Background(), ref, actorUID); err != nil {
		t.Fatalf("waitForLocalCheckpointRemoval() after the removal finished = %v, want nil", err)
	}
}

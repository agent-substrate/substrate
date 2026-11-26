// Copyright 2026 The Agent Substrate Authors
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

package trash

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func createFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRetireAsyncAndWaitForActor(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src := filepath.Join(root, "actor-1", "durable-dir")
	createFile(t, filepath.Join(src, "a.txt"), "hello")

	started := make(chan string, 1)
	release := make(chan struct{})

	r := New(t.Context())
	r.workers = 1
	r.removeFunc = func(p string) error {
		started <- p
		<-release
		return removeAll(p)
	}
	r.Start()
	t.Cleanup(func() {
		_ = r.Wait(context.Background())
	})

	if err := r.Retire(t.Context(), trashDir, src, RetireOptions{
		ActorUID: "actor-1",
		Kind:     KindActorDir,
	}); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	// Original path is gone immediately before the worker finishes unlinking.
	if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("src still exists after Retire: %v", err)
	}

	trashedPath := <-started

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- r.WaitForActor(t.Context(), "actor-1")
	}()

	// Context cancellation unblocks WaitForActor while removal is blocked.
	cancelCtx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := r.WaitForActor(cancelCtx, "actor-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForActor(canceled) = %v, want context.Canceled", err)
	}

	close(release)
	if err := <-waitDone; err != nil {
		t.Fatalf("WaitForActor: %v", err)
	}
	if _, err := os.Stat(trashedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trashedPath still exists after WaitForActor: %v", err)
	}
}

func TestWaitForActorReturnsOnReaperCtxCancel(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src := filepath.Join(root, "actor-ctx", "durable-dir")
	createFile(t, filepath.Join(src, "a.txt"), "hello")

	reaperCtx, cancelReaper := context.WithCancel(t.Context())
	r := New(reaperCtx)
	// Workers not started: actorPending stays > 0 until reaperCtx cancels.
	if err := r.Retire(t.Context(), trashDir, src, RetireOptions{
		ActorUID: "actor-ctx",
		Kind:     KindActorDir,
	}); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- r.WaitForActor(context.Background(), "actor-ctx")
	}()
	cancelReaper()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForActor = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForActor hung after reaper context cancellation")
	}
}

func TestWaitForActorReturnsCanceledOnWait(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src := filepath.Join(root, "actor-wait", "durable-dir")
	createFile(t, filepath.Join(src, "a.txt"), "hello")

	r := New(t.Context())
	if err := r.Retire(t.Context(), trashDir, src, RetireOptions{
		ActorUID: "actor-wait",
		Kind:     KindActorDir,
	}); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	if err := r.Wait(t.Context()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if err := r.WaitForActor(t.Context(), "actor-wait"); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForActor after Wait = %v, want context.Canceled", err)
	}
}

func TestRetireEmptyDirRemovesInPlace(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	emptyDir := filepath.Join(root, "actor-empty", "durable-dir")
	if err := os.MkdirAll(emptyDir, 0o755); err != nil {
		t.Fatal(err)
	}

	r := New(t.Context())
	if err := r.Retire(t.Context(), trashDir, emptyDir, RetireOptions{
		ActorUID: "actor-empty",
		Kind:     KindActorDir,
	}); err != nil {
		t.Fatalf("Retire(emptyDir): %v", err)
	}
	if _, err := os.Stat(emptyDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("emptyDir still exists: %v", err)
	}
	if _, err := os.Stat(trashDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trashDir should not be created for empty dir: %v", err)
	}
}

func TestRemoveAllReadOnlyFallback(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src := filepath.Join(root, "actor-ro", "bundles")
	sub := filepath.Join(src, "readonly-dir")
	createFile(t, filepath.Join(sub, "file.txt"), "locked")
	if err := os.Chmod(sub, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(sub, 0o700)
	})

	r := New(t.Context())
	r.workers = 1
	r.Start()
	t.Cleanup(func() {
		_ = r.Wait(context.Background())
	})

	if err := r.Retire(t.Context(), trashDir, src, RetireOptions{
		ActorUID: "actor-ro",
		Kind:     KindActorDir,
	}); err != nil {
		t.Fatalf("Retire: %v", err)
	}
	if err := r.WaitForActor(t.Context(), "actor-ro"); err != nil {
		t.Fatalf("WaitForActor: %v", err)
	}
	entries, err := os.ReadDir(trashDir)
	if err != nil {
		t.Fatalf("ReadDir(trashDir): %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("trashDir still has %d entries, want 0", len(entries))
	}
}

func TestWaitForActorDetachesAndExhaustsRetriesOnPoisonPill(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src := filepath.Join(root, "actor-fail", "durable-dir")
	createFile(t, filepath.Join(src, "file.txt"), "stuck")

	var attempts atomic.Int32
	exhausted := make(chan struct{})
	r := New(t.Context())
	r.workers = 1
	r.maxRetries = 2
	r.baseRetryDelay = 5 * time.Millisecond
	r.removeFunc = func(string) error {
		if attempts.Add(1) == 2 {
			close(exhausted)
		}
		return errors.New("immutable")
	}
	r.Start()
	t.Cleanup(func() {
		_ = r.Wait(context.Background())
	})

	if err := r.Retire(t.Context(), trashDir, src, RetireOptions{
		ActorUID:       "actor-fail",
		Kind:           KindActorDir,
		EstimatedBytes: 1234,
	}); err != nil {
		t.Fatalf("Retire: %v", err)
	}

	// WaitForActor returns once the first attempt fails rather than wedging.
	if err := r.WaitForActor(t.Context(), "actor-fail"); err != nil {
		t.Fatalf("WaitForActor: %v", err)
	}

	select {
	case <-exhausted:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for retry exhaustion; attempts=%d", attempts.Load())
	}

	// Wait for finishItem to release dev accounting after retries exhaust.
	dev, err := pathDev(trashDir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		acc := r.byDev[dev]
		done := acc != nil && acc.pendingItems == 0 && acc.pendingBytes == 0
		r.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dev accounting not cleared after retry exhaustion: %+v", acc)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBackpressureBlocksUnblocksAndNonBlockingBypasses(t *testing.T) {
	root := t.TempDir()
	trashDir := filepath.Join(root, ".trash")
	src1 := filepath.Join(root, "a1", "snap")
	src2 := filepath.Join(root, "a2", "snap")
	src3 := filepath.Join(root, "a3", "snap")
	createFile(t, filepath.Join(src1, "img"), "1")
	createFile(t, filepath.Join(src2, "img"), "2")
	createFile(t, filepath.Join(src3, "img"), "3")

	started := make(chan struct{}, 3)
	release := make(chan struct{})

	r := New(t.Context())
	r.workers = 1
	r.maxPendingBytes = 100
	r.removeFunc = func(p string) error {
		started <- struct{}{}
		<-release
		return removeAll(p)
	}
	r.Start()
	t.Cleanup(func() {
		_ = r.Wait(context.Background())
	})

	// Single oversized item on an empty queue must not deadlock.
	if err := r.Retire(t.Context(), trashDir, src1, RetireOptions{
		ActorUID:       "a1",
		Kind:           KindLocalSnapshot,
		EstimatedBytes: 150,
	}); err != nil {
		t.Fatalf("Retire(src1): %v", err)
	}
	<-started

	// Live blocking Retire with EstimatedBytes == 0 must also wait when
	// pendingBytes already meets or exceeds maxPendingBytes.
	retire2Done := make(chan error, 1)
	go func() {
		retire2Done <- r.Retire(t.Context(), trashDir, src2, RetireOptions{
			ActorUID: "a2",
			Kind:     KindActorDir,
		})
	}()

	// Wait deterministically until src2 is blocked in waitForHeadroom.
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		waiting := r.headroomWaiters
		r.mu.Unlock()
		if waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for src2 to block in waitForHeadroom")
		}
		time.Sleep(time.Millisecond)
	}

	// NonBlocking bypasses backpressure immediately while src2 stays blocked.
	if err := r.Retire(t.Context(), trashDir, src3, RetireOptions{
		ActorUID:       "a3",
		Kind:           KindLocalSnapshot,
		EstimatedBytes: 50,
		NonBlocking:    true,
	}); err != nil {
		t.Fatalf("Retire(src3, NonBlocking): %v", err)
	}
	select {
	case err := <-retire2Done:
		t.Fatalf("Retire(src2) returned early before release: %v", err)
	default:
	}

	close(release)
	if err := <-retire2Done; err != nil {
		t.Fatalf("Retire(src2): %v", err)
	}
	for _, uid := range []string{"a1", "a2", "a3"} {
		if err := r.WaitForActor(t.Context(), uid); err != nil {
			t.Fatalf("WaitForActor(%s): %v", uid, err)
		}
	}
}

func TestRetireRenameFailureFallback(t *testing.T) {
	root := t.TempDir()
	// Make trashDir a regular file so MkdirAll/Rename fails.
	badTrash := filepath.Join(root, "not-a-dir")
	createFile(t, badTrash, "x")

	srcSync := filepath.Join(root, "sync-dir")
	createFile(t, filepath.Join(srcSync, "f"), "data")

	srcNB := filepath.Join(root, "nb-dir")
	createFile(t, filepath.Join(srcNB, "f"), "data")

	r := New(t.Context())

	// Blocking Retire falls back to synchronous removal.
	if err := r.Retire(t.Context(), badTrash, srcSync, RetireOptions{ActorUID: "a"}); err != nil {
		t.Fatalf("Retire(sync fallback): %v", err)
	}
	if _, err := os.Stat(srcSync); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("srcSync still exists after sync fallback: %v", err)
	}

	// NonBlocking Retire returns the error without removing srcNB.
	if err := r.Retire(t.Context(), badTrash, srcNB, RetireOptions{ActorUID: "a", NonBlocking: true}); err == nil {
		t.Fatal("Retire(NonBlocking) = nil, want error")
	}
	if _, err := os.Stat(srcNB); err != nil {
		t.Fatalf("srcNB should remain after NonBlocking failure: %v", err)
	}
}

func TestStartupSweepSkipsLiveInstanceEntries(t *testing.T) {
	root := t.TempDir()
	trash1 := filepath.Join(root, "actors", ".trash")
	trash2 := filepath.Join(root, "snapshots", ".trash")
	orphan1 := filepath.Join(trash1, "old-actor-1")
	orphan2 := filepath.Join(trash2, "old-snap-2")
	createFile(t, filepath.Join(orphan1, "f"), "a")
	createFile(t, filepath.Join(orphan2, "f"), "b")

	// Also place an entry retired by this same Reaper before Start so we verify
	// sweepStartup does not double-enqueue it.
	liveSrc := filepath.Join(root, "actors", "live-actor", "durable-dir")
	createFile(t, filepath.Join(liveSrc, "f"), "live")

	var liveRemoves atomic.Int32
	removed := make(chan string, 4)
	releaseWorker := make(chan struct{})
	r := New(t.Context())
	r.workers = 1
	r.removeFunc = func(p string) error {
		<-releaseWorker
		if filepath.Dir(p) == trash1 && filepath.Base(p) != "old-actor-1" {
			liveRemoves.Add(1)
		}
		err := removeAll(p)
		removed <- p
		return err
	}
	if err := r.Retire(t.Context(), trash1, liveSrc, RetireOptions{ActorUID: "live-actor", Kind: KindActorDir}); err != nil {
		t.Fatalf("Retire(liveSrc): %v", err)
	}
	r.Start(trash1, trash2)
	// Second Start must be a no-op.
	r.Start(trash1, trash2)
	t.Cleanup(func() {
		_ = r.Wait(context.Background())
	})

	// Hold the worker until sweepStartup has scanned both trash dirs while
	// liveSrc's retired dir is still on disk in trash1, proving sweepStartup
	// skips it by instancePrefix rather than racing past a deleted entry.
	dev, err := pathDev(trash1)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.mu.Lock()
		acc := r.byDev[dev]
		pending := 0
		if acc != nil {
			pending = acc.pendingItems
		}
		r.mu.Unlock()
		if pending == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for sweepStartup; pendingItems=%d, want 3", pending)
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseWorker)

	seen := map[string]bool{}
	for range 3 {
		select {
		case p := <-removed:
			seen[p] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for startup sweep; saw %v", seen)
		}
	}
	if !seen[orphan1] || !seen[orphan2] {
		t.Fatalf("startup sweep removed %v, want %q and %q", seen, orphan1, orphan2)
	}
	if got := liveRemoves.Load(); got != 1 {
		t.Fatalf("live item removed %d times, want 1 (startup sweep must not double-enqueue)", got)
	}
}

func TestNilReaperRemovesSynchronously(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "dir")
	createFile(t, filepath.Join(src, "f"), "data")

	var r *Reaper
	if err := r.Retire(t.Context(), filepath.Join(root, ".trash"), src, RetireOptions{ActorUID: "a"}); err != nil {
		t.Fatalf("nil Retire: %v", err)
	}
	if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("src still exists after nil Retire: %v", err)
	}
	if err := r.WaitForActor(t.Context(), "a"); err != nil {
		t.Fatalf("nil WaitForActor: %v", err)
	}
}

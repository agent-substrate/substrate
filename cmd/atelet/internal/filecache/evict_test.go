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

package filecache

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const evictAll = int64(1) << 40

// cacheEntry materializes an entry through the real retrieval path, backdates
// its last-use clock by age, and returns its key. keepLink controls whether a
// consumer hard link survives (dst removed = the actor dir was wiped).
func cacheEntry(t *testing.T, s *Store, name, content string, age time.Duration, keepLink bool) Key {
	t.Helper()
	key := URIKey("test://" + name)
	fetch, _ := countingFetcher(content)
	dst := dstPath(t, s, "evict-"+name)
	if err := s.GetFileTo(context.Background(), key, dst, fetch); err != nil {
		t.Fatalf("GetFileTo(%s): %v", name, err)
	}
	if !keepLink {
		if err := os.Remove(dst); err != nil {
			t.Fatal(err)
		}
	}
	stale := time.Now().Add(-age)
	if err := os.Chtimes(s.entryDir(key), stale, stale); err != nil {
		t.Fatal(err)
	}
	return key
}

func entryExists(t *testing.T, s *Store, key Key) bool {
	t.Helper()
	_, err := os.Stat(s.entryDir(key))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return err == nil
}

// noRetiredLeftovers asserts the two-phase retire completed: no .rm-* dirs
// remain at the store root.
func noRetiredLeftovers(t *testing.T, s *Store) {
	t.Helper()
	children, err := os.ReadDir(s.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		if strings.HasPrefix(child.Name(), rmPrefix) {
			t.Errorf("retired dir %q left behind after pass", child.Name())
		}
	}
}

func TestEvictUnusedNonPositiveTargetIsNoop(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	key := cacheEntry(t, s, "kept", "x", time.Hour, false)

	stats, err := s.EvictUnused(context.Background(), 0, false)
	if err != nil {
		t.Fatalf("EvictUnused(0): %v", err)
	}
	if stats != (EvictStats{}) {
		t.Errorf("stats = %+v, want zero", stats)
	}
	if !entryExists(t, s, key) {
		t.Error("entry evicted by zero-target pass")
	}
}

func TestEvictUnusedTakesLeastRecentlyUsedAndStopsAtTarget(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	oldest := cacheEntry(t, s, "oldest", "aaaaa", 3*time.Hour, false)
	middle := cacheEntry(t, s, "middle", "bbbbb", 2*time.Hour, false)
	newest := cacheEntry(t, s, "newest", "ccccc", time.Hour, false)

	// A 1-byte target forces exactly one eviction, which must be the LRU.
	stats, err := s.EvictUnused(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 1 || stats.FreedBytes < 5 {
		t.Errorf("stats = %+v, want Retired=1 and FreedBytes >= 5", stats)
	}
	if entryExists(t, s, oldest) {
		t.Error("LRU entry survived")
	}
	if !entryExists(t, s, middle) || !entryExists(t, s, newest) {
		t.Error("pass evicted beyond its target")
	}
	noRetiredLeftovers(t, s)
}

func TestEvictUnusedRespectsMinAge(t *testing.T) {
	s := newTestStore(t, WithMinAge(time.Hour))
	young := cacheEntry(t, s, "young", "x", 30*time.Minute, false)

	stats, err := s.EvictUnused(context.Background(), evictAll, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 0 || stats.SkippedYoung != 1 {
		t.Errorf("stats = %+v, want Retired=0 SkippedYoung=1", stats)
	}
	if !entryExists(t, s, young) {
		t.Error("entry younger than minAge evicted")
	}
}

func TestEvictUnusedPrefersUnlinkedOverOlderLinked(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	// The linked entry is older; pure LRU would take it first. The pass must
	// prefer the unlinked one, whose bytes actually come back.
	linked := cacheEntry(t, s, "linked", "xx", 3*time.Hour, true)
	unlinked := cacheEntry(t, s, "unlinked", "yy", time.Hour, false)

	stats, err := s.EvictUnused(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 1 || stats.FreedBytes == 0 || stats.PendingBytes != 0 {
		t.Errorf("stats = %+v, want one eviction with freed bytes only", stats)
	}
	if entryExists(t, s, unlinked) {
		t.Error("unlinked entry survived")
	}
	if !entryExists(t, s, linked) {
		t.Error("linked entry evicted while an unlinked one satisfied the target")
	}
}

func TestEvictUnusedLinkedEntryIsSafeForConsumer(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	key := URIKey("test://held")
	fetch, calls := countingFetcher("held bytes")
	dst := dstPath(t, s, "held")
	if err := s.GetFileTo(context.Background(), key, dst, fetch); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-time.Hour)
	if err := os.Chtimes(s.entryDir(key), stale, stale); err != nil {
		t.Fatal(err)
	}

	stats, err := s.EvictUnused(context.Background(), evictAll, false)
	if err != nil {
		t.Fatalf("EvictUnused: %v", err)
	}
	if stats.Retired != 1 || stats.FreedBytes != 0 || stats.PendingBytes == 0 {
		t.Errorf("stats = %+v, want one eviction counted as pending bytes", stats)
	}
	if entryExists(t, s, key) {
		t.Error("entry still published after eviction")
	}
	// The consumer's link is untouched: eviction cost is a re-download,
	// never a broken consumer.
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "held bytes" {
		t.Errorf("consumer link after eviction: %q, %v", got, err)
	}
	if err := s.GetFileTo(context.Background(), key, dstPath(t, s, "held2"), fetch); err != nil {
		t.Fatalf("GetFileTo after eviction: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("fetch ran %d times, want 2 (original + post-eviction)", calls.Load())
	}
}

func TestEvictUnusedDryRunTouchesNothing(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	a := cacheEntry(t, s, "a", "aa", 2*time.Hour, false)
	b := cacheEntry(t, s, "b", "bb", time.Hour, true)

	stats, err := s.EvictUnused(context.Background(), evictAll, true)
	if err != nil {
		t.Fatalf("EvictUnused(dryRun): %v", err)
	}
	if stats.Retired != 2 || stats.FreedBytes == 0 || stats.PendingBytes == 0 {
		t.Errorf("stats = %+v, want both entries reported (one freed, one pending)", stats)
	}
	if !entryExists(t, s, a) || !entryExists(t, s, b) {
		t.Error("dry run removed entries")
	}
	noRetiredLeftovers(t, s)
}

func TestEvictUnusedRemovalFailureIsNotCountedFreed(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	s := newTestStore(t, WithMinAge(0))
	// A tree entry with a write-protected subdirectory: listing and the
	// retire rename work, but RemoveAll cannot unlink the file inside.
	key := publishTestEntry(t, s, "stuck", map[string]string{
		filepath.Join(dataName, "locked", "f"): "12345",
	})
	locked := filepath.Join(s.entryDir(key), dataName, "locked")
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { // let TempDir cleanup succeed wherever the dir ended up
		matches, _ := filepath.Glob(filepath.Join(s.root, rmPrefix+"*", dataName, "locked"))
		for _, m := range append(matches, locked) {
			_ = os.Chmod(m, 0o700)
		}
	})
	stale := time.Now().Add(-time.Hour)
	if err := os.Chtimes(s.entryDir(key), stale, stale); err != nil {
		t.Fatal(err)
	}

	stats, err := s.EvictUnused(context.Background(), evictAll, false)
	if err == nil {
		t.Fatal("EvictUnused succeeded despite unremovable entry, want error")
	}
	if stats.FreedBytes != 0 {
		t.Errorf("FreedBytes = %d for a failed removal, want 0", stats.FreedBytes)
	}
	if stats.Retired != 1 {
		t.Errorf("Retired = %d, want 1 (the retire itself succeeded)", stats.Retired)
	}
	if entryExists(t, s, key) {
		t.Error("entry still published after retire")
	}
}

func TestRetireEntryVetoesWhenLastUseMoved(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	key := cacheEntry(t, s, "busy", "x", time.Hour, false)

	fi, err := os.Stat(s.entryDir(key))
	if err != nil {
		t.Fatal(err)
	}
	listed := evictCandidate{dir: key.dir, mtime: fi.ModTime(), size: 1}

	// A hit lands between the listing and the retire.
	now := time.Now()
	if err := os.Chtimes(s.entryDir(key), now, now); err != nil {
		t.Fatal(err)
	}

	rmPath, retired, err := s.retireEntry(listed)
	if err != nil {
		t.Fatalf("retireEntry: %v", err)
	}
	if retired || rmPath != "" {
		t.Errorf("retireEntry retired a touched entry (rmPath=%q)", rmPath)
	}
	if !entryExists(t, s, key) {
		t.Error("touched entry vanished")
	}
}

func TestRetireEntryRetiresUnchangedEntry(t *testing.T) {
	s := newTestStore(t, WithMinAge(0))
	key := cacheEntry(t, s, "stale", "x", time.Hour, false)

	fi, err := os.Stat(s.entryDir(key))
	if err != nil {
		t.Fatal(err)
	}
	rmPath, retired, err := s.retireEntry(evictCandidate{dir: key.dir, mtime: fi.ModTime(), size: 1})
	if err != nil {
		t.Fatalf("retireEntry: %v", err)
	}
	if !retired {
		t.Fatal("retireEntry did not retire an unchanged entry")
	}
	if entryExists(t, s, key) {
		t.Error("entry still published after retire")
	}
	if !strings.HasPrefix(filepath.Base(rmPath), rmPrefix) {
		t.Errorf("retired path %q lacks the %q prefix", rmPath, rmPrefix)
	}
	if _, err := os.Stat(rmPath); err != nil {
		t.Errorf("retired dir missing before removal phase: %v", err)
	}
	// An interrupted pass leaves the retired dir to the startup sweep.
	stats, err := s.SweepDebris(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.RetiredRemoved != 1 {
		t.Errorf("sweep stats = %+v, want RetiredRemoved=1", stats)
	}
}

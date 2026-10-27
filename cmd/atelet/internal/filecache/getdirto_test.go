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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// treeFile is one regular file a treeFetcher writes.
type treeFile struct {
	content string
	mode    fs.FileMode
}

// releaseTree is the shape of an extracted gVisor release: an executable
// next to a directory of helpers.
var releaseTree = map[string]treeFile{
	"runsc":                  {"runsc bytes", 0o755},
	"gvisor-bin/containerd":  {"shim bytes", 0o755},
	"gvisor-bin/LICENSE":     {"license", 0o644},
	"gvisor-bin/docs/README": {"readme", 0o600},
}

// treeFetcher returns a DirFetcher writing files (relative name -> file)
// under the directory the store made, and a counter of how many times it
// ran.
func treeFetcher(files map[string]treeFile) (DirFetcher, *atomic.Int32) {
	var calls atomic.Int32
	return func(ctx context.Context, dstDir string) error {
		calls.Add(1)
		for rel, f := range files {
			path := filepath.Join(dstDir, rel)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte(f.content), f.mode); err != nil {
				return err
			}
		}
		return nil
	}, &calls
}

// checkTree asserts dst mirrors files: same content, every file a hard
// link to the cache copy, read-only with its execute bits kept.
func checkTree(t *testing.T, s *Store, key Key, dst string, files map[string]treeFile) {
	t.Helper()
	for rel, f := range files {
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil || string(got) != f.content {
			t.Errorf("%s: content = %q, %v; want %q", rel, got, err, f.content)
			continue
		}
		fi, err := os.Stat(filepath.Join(dst, rel))
		if err != nil {
			t.Fatal(err)
		}
		if want := 0o444 | f.mode&0o111; fi.Mode().Perm() != want {
			t.Errorf("%s: mode = %v, want %v", rel, fi.Mode().Perm(), want)
		}
		cfi, err := os.Stat(filepath.Join(s.dataPath(key), rel))
		if err != nil {
			t.Fatal(err)
		}
		if !os.SameFile(fi, cfi) {
			t.Errorf("%s: dst and cache copy are not one inode", rel)
		}
	}
}

func TestGetDirToMissThenHit(t *testing.T) {
	s := newTestStore(t)
	key, _ := SHA256Key("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	fetch, calls := treeFetcher(releaseTree)

	dst1 := dstPath(t, s, "t1")
	if err := s.GetDirTo(context.Background(), key, dst1, fetch); err != nil {
		t.Fatalf("GetDirTo (miss): %v", err)
	}
	checkTree(t, s, key, dst1, releaseTree)

	dst2 := dstPath(t, s, "t2")
	if err := s.GetDirTo(context.Background(), key, dst2, fetch); err != nil {
		t.Fatalf("GetDirTo (hit): %v", err)
	}
	checkTree(t, s, key, dst2, releaseTree)
	if calls.Load() != 1 {
		t.Errorf("fetch ran %d times, want 1", calls.Load())
	}

	// The consumer owns its directories: it can add a file beside the
	// links without the cache seeing it.
	if err := os.WriteFile(filepath.Join(dst1, "gvisor-bin", "mine"), []byte("x"), 0o600); err != nil {
		t.Errorf("consumer cannot write into its own mirrored dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.dataPath(key), "gvisor-bin", "mine")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("consumer's file leaked into the cache tree: %v", err)
	}
	// But not write through a link: the shared bytes are protected.
	if err := os.WriteFile(filepath.Join(dst1, "runsc"), []byte("mutated"), 0o755); err == nil {
		t.Error("write through consumer link succeeded, want permission error")
	}

	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir has %d leftover children", len(tmpChildren))
	}
	// Trees are built beside their destination; nothing of that remains.
	siblings, err := os.ReadDir(filepath.Dir(dst1))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range siblings {
		if e.Name() != filepath.Base(dst1) && e.Name() != filepath.Base(dst2) {
			t.Errorf("link-out left %q beside the destination", e.Name())
		}
	}
}

// TestGetDirToSameDestinationLoserSeesCompleteTree pins what a caller that
// loses the race for one destination gets: ErrExist, and a tree it can use,
// because the winner's tree was renamed into place whole.
func TestGetDirToSameDestinationLoserSeesCompleteTree(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("test://release")
	fetch, _ := treeFetcher(releaseTree)
	dst := dstPath(t, s, "shared")

	const n = 6
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() { errs[i] = s.GetDirTo(context.Background(), key, dst, fetch) })
	}
	wg.Wait()
	wins := 0
	for i, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, fs.ErrExist):
		default:
			t.Errorf("caller %d: %v, want nil or ErrExist", i, err)
		}
	}
	if wins != 1 {
		t.Errorf("%d callers won the destination, want exactly 1", wins)
	}
	checkTree(t, s, key, dst, releaseTree)
}

func TestGetDirToConcurrentCallersShareOneFetch(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("test://release")
	fetch, calls := treeFetcher(releaseTree)

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			errs[i] = s.GetDirTo(context.Background(), key, dstPath(t, s, "c"+string(rune('a'+i))), fetch)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	if calls.Load() != 1 {
		t.Errorf("fetch ran %d times, want 1", calls.Load())
	}
}

func TestGetDirToRejectsExistingDst(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("test://release")
	fetch, _ := treeFetcher(releaseTree)
	dst := dstPath(t, s, "taken")
	if err := os.Mkdir(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	err := s.GetDirTo(context.Background(), key, dst, fetch)
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("GetDirTo onto existing dst: %v, want ErrExist", err)
	}
	// The entry was still fetched and published; only the serve failed.
	if _, err := os.Stat(filepath.Join(s.dataPath(key), "runsc")); err != nil {
		t.Errorf("entry not published after serve failure: %v", err)
	}
}

func TestGetDirToRejectsBadArguments(t *testing.T) {
	s := newTestStore(t)
	fetch, calls := treeFetcher(releaseTree)
	key := URIKey("test://release")

	if err := s.GetDirTo(context.Background(), Key{}, dstPath(t, s, "z"), fetch); err == nil {
		t.Error("zero key accepted")
	}
	if err := s.GetDirTo(context.Background(), key, "relative/dst", fetch); err == nil {
		t.Error("relative dst accepted")
	}
	if err := s.GetDirTo(context.Background(), key, dstPath(t, s, "n"), nil); err == nil {
		t.Error("nil fetcher accepted")
	}
	if calls.Load() != 0 {
		t.Errorf("fetch ran %d times on rejected calls", calls.Load())
	}
}

func TestGetDirToEmptyTreeFails(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("test://empty")
	fetch, _ := treeFetcher(nil)
	err := s.GetDirTo(context.Background(), key, dstPath(t, s, "e"), fetch)
	if err == nil {
		t.Fatal("empty tree published")
	}
	if entryExists(t, s, key) {
		t.Error("entry published despite the failure")
	}
}

func TestGetDirToRejectsSymlinkInTree(t *testing.T) {
	s := newTestStore(t)
	key := URIKey("test://symlinked")
	fetch := func(ctx context.Context, dstDir string) error {
		if err := os.WriteFile(filepath.Join(dstDir, "runsc"), []byte("x"), 0o755); err != nil {
			return err
		}
		return os.Symlink("runsc", filepath.Join(dstDir, "alias"))
	}
	err := s.GetDirTo(context.Background(), key, dstPath(t, s, "l"), fetch)
	if err == nil {
		t.Fatal("tree with a symlink published")
	}
	if entryExists(t, s, key) {
		t.Error("entry published despite the failure")
	}
	tmpChildren, err := os.ReadDir(s.tmpDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(tmpChildren) != 0 {
		t.Errorf("tmp dir has %d leftover children after a failed publish", len(tmpChildren))
	}
}

// TestGetKindMismatchIsAnError pins that a key fetched as one kind is never
// silently served as the other: the entry exists, so it is not a miss that
// would trigger a refetch under the other kind.
func TestGetKindMismatchIsAnError(t *testing.T) {
	s := newTestStore(t)
	treeKey := URIKey("test://tree")
	treeFetch, treeCalls := treeFetcher(releaseTree)
	if err := s.GetDirTo(context.Background(), treeKey, dstPath(t, s, "t"), treeFetch); err != nil {
		t.Fatal(err)
	}
	fileKey := URIKey("test://file")
	fileFetch, fileCalls := countingFetcher("one file")
	if err := s.GetFileTo(context.Background(), fileKey, dstPath(t, s, "f"), fileFetch); err != nil {
		t.Fatal(err)
	}

	if err := s.GetFileTo(context.Background(), treeKey, dstPath(t, s, "t-as-file"), fileFetch); err == nil {
		t.Error("GetFileTo served a tree entry")
	}
	if err := s.GetFileCopyTo(context.Background(), treeKey, dstPath(t, s, "t-as-copy"), fileFetch); err == nil {
		t.Error("GetFileCopyTo served a tree entry")
	}
	if err := s.GetDirTo(context.Background(), fileKey, dstPath(t, s, "f-as-tree"), treeFetch); err == nil {
		t.Error("GetDirTo served a file entry")
	}
	if treeCalls.Load() != 1 || fileCalls.Load() != 1 {
		t.Errorf("fetches ran tree=%d file=%d times, want 1 each (a mismatch must not refetch)", treeCalls.Load(), fileCalls.Load())
	}
}

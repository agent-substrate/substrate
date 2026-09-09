//go:build linux

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

package tarutil

import (
	"archive/tar"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const testBlobPrefix = "blob-"

// splitTree builds the tree the round trip is checked against and returns it.
func splitTree(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	write := func(rel, content string, mode os.FileMode) {
		t.Helper()
		p := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir for %q: %v", rel, err)
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatalf("writing %q: %v", rel, err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatalf("chmod %q: %v", rel, err)
		}
	}
	write("a.txt", "hello", 0o644)
	write("sub/b.txt", "nested", 0o600)
	write("empty.txt", "", 0o644)
	if err := os.Symlink("a.txt", filepath.Join(src, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Link(filepath.Join(src, "a.txt"), filepath.Join(src, "hard.txt")); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	return src
}

func TestSplitRoundTrip(t *testing.T) {
	src := splitTree(t)
	mtime := time.Unix(1600000000, 0)
	if err := os.Chtimes(filepath.Join(src, "a.txt"), mtime, mtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	blobDir := t.TempDir()
	tarPath := filepath.Join(blobDir, "index.tar")
	if _, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix); err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}
	dst := t.TempDir()
	if err := ExtractSplit(tarPath, blobDir, dst); err != nil {
		t.Fatalf("ExtractSplit: %v", err)
	}

	t.Run("contents", func(t *testing.T) {
		for rel, want := range map[string]string{
			"a.txt":     "hello",
			"sub/b.txt": "nested",
			"empty.txt": "",
			"hard.txt":  "hello",
		} {
			got, err := os.ReadFile(filepath.Join(dst, rel))
			if err != nil {
				t.Errorf("reading %q: %v", rel, err)
				continue
			}
			if string(got) != want {
				t.Errorf("%q = %q, want %q", rel, got, want)
			}
		}
	})

	t.Run("modes", func(t *testing.T) {
		for rel, want := range map[string]os.FileMode{
			"a.txt":     0o644,
			"sub/b.txt": 0o600,
			"empty.txt": 0o644,
		} {
			st, err := os.Lstat(filepath.Join(dst, rel))
			if err != nil {
				t.Errorf("lstat %q: %v", rel, err)
				continue
			}
			if got := st.Mode().Perm(); got != want {
				t.Errorf("%q mode = %v, want %v", rel, got, want)
			}
		}
	})

	t.Run("mtime", func(t *testing.T) {
		st, err := os.Stat(filepath.Join(dst, "a.txt"))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if !st.ModTime().Equal(mtime) {
			t.Errorf("a.txt mtime = %v, want %v", st.ModTime(), mtime)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		got, err := os.Readlink(filepath.Join(dst, "link"))
		if err != nil {
			t.Fatalf("readlink: %v", err)
		}
		if got != "a.txt" {
			t.Errorf("link target = %q, want %q", got, "a.txt")
		}
	})

	// Diverting contents must not turn the archive's second link into a second
	// copy: the split records the link exactly as Create does, so only the
	// first of the pair ever gets a blob.
	t.Run("hardlink shares inode", func(t *testing.T) {
		a, err := os.Stat(filepath.Join(dst, "a.txt"))
		if err != nil {
			t.Fatalf("stat a.txt: %v", err)
		}
		h, err := os.Stat(filepath.Join(dst, "hard.txt"))
		if err != nil {
			t.Fatalf("stat hard.txt: %v", err)
		}
		if !os.SameFile(a, h) {
			t.Error("hard.txt is a copy, want the same inode as a.txt")
		}
	})
}

// The blobs must be links, not copies, and the archive must be left holding
// metadata alone. Together those are the whole reason the split exists: a
// checkpoint of a large tree costs one link per file instead of a rewrite.
func TestCreateSplitLinksContentsAndLeavesTheArchiveSmall(t *testing.T) {
	src := t.TempDir()
	big := filepath.Join(src, "big.bin")
	if err := os.WriteFile(big, make([]byte, 1<<20), 0o644); err != nil {
		t.Fatalf("writing big file: %v", err)
	}

	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	blobs, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix)
	if err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}
	if len(blobs) != 1 {
		t.Fatalf("blobs = %v, want exactly one", blobs)
	}

	srcInfo, err := os.Stat(big)
	if err != nil {
		t.Fatalf("stat source: %v", err)
	}
	blobInfo, err := os.Stat(filepath.Join(blobDir, blobs[0]))
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	if !os.SameFile(srcInfo, blobInfo) {
		t.Error("blob is a copy, want the same inode as the source file")
	}

	idx, err := os.Stat(tarPath)
	if err != nil {
		t.Fatalf("stat archive: %v", err)
	}
	if idx.Size() >= 1<<20 {
		t.Errorf("archive is %d bytes; it still carries the file contents", idx.Size())
	}
}

// An empty file is fully described by its header, so giving it a blob would be
// one more object to ship for no bytes.
func TestCreateSplitSkipsEmptyFiles(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "empty.txt"), nil, 0o644); err != nil {
		t.Fatalf("writing empty file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "full.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("writing non-empty file: %v", err)
	}

	blobDir := t.TempDir()
	blobs, err := CreateSplit(t.Context(), filepath.Join(t.TempDir(), "index.tar"), src, blobDir, testBlobPrefix)
	if err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}
	if len(blobs) != 1 {
		t.Errorf("blobs = %v, want one (the non-empty file alone)", blobs)
	}
	entries, err := os.ReadDir(blobDir)
	if err != nil {
		t.Fatalf("reading blob dir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("blob dir holds %d files, want 1", len(entries))
	}
}

// A restore dispatches on the snapshot's file names, so ExtractSplit is
// reached with archives Create wrote too.
func TestExtractSplitReadsAPlainArchive(t *testing.T) {
	src := splitTree(t)
	tarPath := filepath.Join(t.TempDir(), "plain.tar")
	if err := Create(t.Context(), tarPath, src); err != nil {
		t.Fatalf("Create: %v", err)
	}
	dst := t.TempDir()
	if err := ExtractSplit(tarPath, t.TempDir(), dst); err != nil {
		t.Fatalf("ExtractSplit: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil {
		t.Fatalf("reading a.txt: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("a.txt = %q, want %q", got, "hello")
	}
}

// The blob name travels through object storage inside the archive, so it is
// not trusted to name something inside the blob directory.
func TestExtractSplitRejectsBlobEscapes(t *testing.T) {
	for _, blob := range []string{"../secret", "sub/blob-0000", "..", ""} {
		t.Run(blob, func(t *testing.T) {
			tarPath := filepath.Join(t.TempDir(), "crafted.tar")
			writeTar(t, tarPath, tar.Header{
				Name:       "a.txt",
				Typeflag:   tar.TypeReg,
				Mode:       0o644,
				Format:     tar.FormatPAX,
				PAXRecords: map[string]string{blobRecord: blob},
			})
			blobDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(filepath.Dir(blobDir), "secret"), []byte("nope"), 0o600); err != nil {
				t.Fatalf("writing bait: %v", err)
			}
			err := ExtractSplit(tarPath, blobDir, t.TempDir())
			if blob == "" {
				// An absent record is not an escape: the entry simply has no
				// diverted contents, exactly like an empty file.
				if err != nil {
					t.Errorf("ExtractSplit with no blob record = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Errorf("ExtractSplit accepted blob name %q", blob)
			}
		})
	}
}

// Extracting a split archive without its blobs must fail rather than quietly
// produce a tree of empty files, which would read as a successful restore that
// lost every byte the actor had.
func TestExtractRefusesASplitArchiveWithoutBlobs(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	if _, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix); err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}
	err := Extract(tarPath, t.TempDir())
	if err == nil {
		t.Fatal("Extract accepted a split archive with no blob directory")
	}
	if !strings.Contains(err.Error(), "blob") {
		t.Errorf("error = %v, want it to name the missing blobs", err)
	}
}

// Extraction adopts the blobs rather than copying them, which is what keeps a
// restore from writing the whole tree a second time. The test asserts the
// consequence as well as the inode, because the consequence is what callers
// have to hold up: a write to the extracted file is a write to the blob.
func TestExtractSplitAdoptsBlobs(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	blobs, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix)
	if err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}

	// atelet stages a restore by copying out of the retained checkpoint, and
	// resetActorDirs has already removed the tree the blob was linked from, so
	// the extraction becomes the blob's only other owner. Reproduce that here.
	if err := os.RemoveAll(src); err != nil {
		t.Fatalf("dropping the source link: %v", err)
	}

	dst := t.TempDir()
	if err := ExtractSplit(tarPath, blobDir, dst); err != nil {
		t.Fatalf("ExtractSplit: %v", err)
	}

	extracted := filepath.Join(dst, "a.txt")
	blob := filepath.Join(blobDir, blobs[0])
	extractedInfo, err := os.Stat(extracted)
	if err != nil {
		t.Fatalf("stat extracted file: %v", err)
	}
	blobInfo, err := os.Stat(blob)
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	if !os.SameFile(extractedInfo, blobInfo) {
		t.Error("extracted file is a copy, want the blob's own inode")
	}

	if err := os.WriteFile(extracted, []byte("world"), 0o644); err != nil {
		t.Fatalf("writing through the extracted file: %v", err)
	}
	got, err := os.ReadFile(blob)
	if err != nil {
		t.Fatalf("reading blob: %v", err)
	}
	if string(got) != "world" {
		t.Errorf("blob = %q, want the write through the extracted file to reach it", got)
	}
}

// A filesystem that refuses the link must still produce the right tree, since
// a blob directory on a different filesystem than the destination is a node's
// layout rather than a fault.
func TestExtractSplitCopiesWhenTheLinkIsRefused(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	blobs, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix)
	if err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}

	// The nlink guard would otherwise short-circuit the link this test is about.
	if err := os.RemoveAll(src); err != nil {
		t.Fatalf("dropping the source link: %v", err)
	}

	restore := linkat
	t.Cleanup(func() { linkat = restore })
	linkat = func(int, string, int, string, int) error { return unix.EXDEV }

	dst := t.TempDir()
	if err := ExtractSplit(tarPath, blobDir, dst); err != nil {
		t.Fatalf("ExtractSplit: %v", err)
	}

	extracted := filepath.Join(dst, "a.txt")
	got, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("reading extracted file: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("a.txt = %q, want %q", got, "hello")
	}
	extractedInfo, err := os.Stat(extracted)
	if err != nil {
		t.Fatalf("stat extracted file: %v", err)
	}
	blobInfo, err := os.Stat(filepath.Join(blobDir, blobs[0]))
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	if os.SameFile(extractedInfo, blobInfo) {
		t.Error("extracted file shares the blob's inode, want a copy")
	}
}

// A blob someone else still holds a link to must be copied. Adopting it would
// carry the extracted tree's writes into whatever that other owner is keeping
// it for -- for ateom, the checkpoint every later restore reads.
func TestExtractSplitCopiesABlobItDoesNotOwn(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	blobs, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix)
	if err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}
	// CreateSplit links out of src, so the blob already has a second owner.
	blob := filepath.Join(blobDir, blobs[0])

	dst := t.TempDir()
	if err := ExtractSplit(tarPath, blobDir, dst); err != nil {
		t.Fatalf("ExtractSplit: %v", err)
	}

	extracted := filepath.Join(dst, "a.txt")
	got, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("reading extracted file: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("a.txt = %q, want %q", got, "hello")
	}
	extractedInfo, err := os.Stat(extracted)
	if err != nil {
		t.Fatalf("stat extracted file: %v", err)
	}
	blobInfo, err := os.Stat(blob)
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	if os.SameFile(extractedInfo, blobInfo) {
		t.Error("extracted file adopted a blob a second owner still holds")
	}
}

// An error the fallback does not cover has to reach the caller: a restore that
// quietly produced a tree of empty files would read as a success that lost
// every byte the actor had.
func TestExtractSplitFailsOnAnUnexpectedLinkError(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	blobDir := t.TempDir()
	tarPath := filepath.Join(t.TempDir(), "index.tar")
	if _, err := CreateSplit(t.Context(), tarPath, src, blobDir, testBlobPrefix); err != nil {
		t.Fatalf("CreateSplit: %v", err)
	}

	// The nlink guard would otherwise short-circuit the link this test is about.
	if err := os.RemoveAll(src); err != nil {
		t.Fatalf("dropping the source link: %v", err)
	}

	restore := linkat
	t.Cleanup(func() { linkat = restore })
	linkat = func(int, string, int, string, int) error { return unix.EIO }

	err := ExtractSplit(tarPath, blobDir, t.TempDir())
	if err == nil {
		t.Fatal("ExtractSplit accepted a failed link")
	}
	if !strings.Contains(err.Error(), "blob") {
		t.Errorf("error = %v, want it to name the blob", err)
	}
}

// The fallback for a blob directory the source cannot be linked into.
func TestCopyBlob(t *testing.T) {
	src := filepath.Join(t.TempDir(), "a.txt")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatalf("writing source: %v", err)
	}
	dst := filepath.Join(t.TempDir(), "blob-0000")
	if err := copyBlob(src, dst); err != nil {
		t.Fatalf("copyBlob: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading blob: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("blob = %q, want %q", got, "hello")
	}
	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatalf("stat source: %v", err)
	}
	blobInfo, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}
	if os.SameFile(srcInfo, blobInfo) {
		t.Error("copyBlob linked rather than copied")
	}
}

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

//go:build linux

package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestKernelCopyRange exercises the copy_file_range path directly. copySparse falls
// back to userspace when the kernel refuses, so without this the syscall could be
// broken and every other test would still pass.
func TestKernelCopyRange(t *testing.T) {
	const (
		size = 1 << 20
		off  = 64 << 10
	)
	dir := t.TempDir()

	payload := bytes.Repeat([]byte{0x5A}, 128<<10)
	srcPath := filepath.Join(dir, "src")
	src, err := os.Create(srcPath)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	defer src.Close()
	if err := src.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	if _, err := src.WriteAt(payload, off); err != nil {
		t.Fatalf("writing src: %v", err)
	}
	if err := src.Sync(); err != nil {
		t.Fatalf("syncing src: %v", err)
	}

	dstPath := filepath.Join(dir, "dst")
	dst, err := os.Create(dstPath)
	if err != nil {
		t.Fatalf("creating dst: %v", err)
	}
	defer dst.Close()
	if err := dst.Truncate(size); err != nil {
		t.Fatalf("sizing dst: %v", err)
	}

	// Short copies are legal, so loop like copySparse does.
	remaining := int64(len(payload))
	pos := int64(off)
	for remaining > 0 {
		n, err := kernelCopyRange(int(src.Fd()), int(dst.Fd()), pos, remaining)
		if errors.Is(err, errKernelCopyUnsupported) {
			t.Skipf("copy_file_range unavailable on this filesystem after %d of %d bytes",
				int64(len(payload))-remaining, int64(len(payload)))
		}
		if err != nil {
			t.Fatalf("kernelCopyRange at %d: %v", pos, err)
		}
		if n <= 0 {
			t.Fatalf("kernelCopyRange reported %d bytes copied", n)
		}
		pos += n
		remaining -= n
	}

	got := make([]byte, len(payload))
	if _, err := dst.ReadAt(got, off); err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("copied range does not match the source")
	}

	// Everything outside the copied range must still be untouched.
	head := make([]byte, off)
	if _, err := dst.ReadAt(head, 0); err != nil {
		t.Fatalf("reading dst head: %v", err)
	}
	if !bytes.Equal(head, make([]byte, off)) {
		t.Error("copy wrote outside the requested range")
	}
}

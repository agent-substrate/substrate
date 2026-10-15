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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func makeResolvConfWritable(path string) error {
	// 1. Try detaching a read-only file bind mount on path (e.g. /etc/resolv.conf).
	if err := syscall.Unmount(path, syscall.MNT_DETACH); err == nil {
		return nil
	}

	// 2. Fallback: shadow the parent directory (e.g. /etc) with a writable tmpfs
	// populated from the existing directory contents.
	dir := filepath.Dir(path)
	shadowDir, err := os.MkdirTemp("/tmp", "etc-shadow-*")
	if err != nil {
		return fmt.Errorf("creating shadow dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(shadowDir)
	}()

	if out, err := exec.Command("cp", "-a", dir+"/.", shadowDir+"/").CombinedOutput(); err != nil {
		return fmt.Errorf("copying %s to shadow: %w (%s)", dir, err, out)
	}
	if err := syscall.Mount("tmpfs", dir, "tmpfs", 0, "mode=0755"); err != nil {
		return fmt.Errorf("mounting tmpfs on %s: %w", dir, err)
	}
	if out, err := exec.Command("cp", "-a", shadowDir+"/.", dir+"/").CombinedOutput(); err != nil {
		return fmt.Errorf("populating tmpfs %s: %w (%s)", dir, err, out)
	}
	_ = os.Remove(path)
	return nil
}

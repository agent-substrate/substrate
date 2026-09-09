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

package cdiinject

import (
	"context"
	"debug/elf"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// StageSonameSymlinks writes each driver library's SONAME symlink (e.g.
// libcuda.so.1 -> libcuda.so.580.65.06) into rootfs, so programs that link against
// the SONAME resolve it. For each CDI library mount it reads the library's ELF
// DT_SONAME and, when that differs from the mounted filename, writes a relative
// symlink alongside the mount destination; the real library file arrives at runtime
// via the CDI bind-mount into the same directory.
//
// A library whose SONAME cannot be read is skipped rather than failing the actor, so
// one odd file cannot cost us the whole GPU. That is logged: the missing symlink
// surfaces much later, as a loader error in the workload, with nothing pointing back
// here.
// Every path component under rootfs comes from the actor image, so the writes go
// through os.Root: an image that ships a driver-mount's parent directory as a
// symlink out of the rootfs would otherwise have the kernel resolve it in ateom's
// mount namespace, where the shared image cache and other actors' bundles are
// mounted. Same treatment as createExtraDirs in internal/imagecache.
func StageSonameSymlinks(ctx context.Context, rootfs string, mounts []specs.Mount) error {
	root, err := os.OpenRoot(rootfs)
	if err != nil {
		return fmt.Errorf("while opening rootfs %q: %w", rootfs, err)
	}
	defer root.Close()

	for _, m := range mounts {
		base := filepath.Base(m.Destination)
		// Only shared libraries (…/lib…/foo.so.<version>).
		if !strings.Contains(base, ".so.") || m.Source == "" {
			continue
		}
		soname, err := elfSonameFn(m.Source)
		if err != nil {
			slog.WarnContext(ctx, "Skipping SONAME symlink for driver library",
				slog.String("library", m.Source), slog.Any("err", err))
			continue
		}
		if soname == "" || soname == base {
			continue
		}
		// The SONAME is read out of the library and joined into a path, so require a
		// bare filename rather than trusting the file's contents.
		if soname != filepath.Base(soname) || !filepath.IsLocal(soname) {
			slog.WarnContext(ctx, "Skipping driver library whose SONAME is not a filename",
				slog.String("library", m.Source), slog.String("soname", soname))
			continue
		}
		dir := strings.TrimPrefix(filepath.Dir(m.Destination), "/")
		if dir != "" && !filepath.IsLocal(dir) {
			return fmt.Errorf("driver mount %q escapes the rootfs", m.Destination)
		}
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		link := filepath.Join(dir, soname)
		_ = root.Remove(link)
		if err := root.Symlink(base, link); err != nil {
			return fmt.Errorf("symlink %s -> %s: %w", link, base, err)
		}
	}
	return nil
}

// elfSonameFn is elfSoname, as a var so tests can supply a SONAME without needing a
// real ELF file on disk.
var elfSonameFn = elfSoname

// elfSoname returns a shared library's DT_SONAME. It returns "" with a nil error for
// a library that simply carries no DT_SONAME entry, and an error when the file could
// not be opened or parsed as ELF.
func elfSoname(path string) (string, error) {
	f, err := elf.Open(path)
	if err != nil {
		return "", fmt.Errorf("opening ELF %s: %w", path, err)
	}
	defer f.Close()
	names, err := f.DynString(elf.DT_SONAME)
	if err != nil {
		return "", fmt.Errorf("reading DT_SONAME from %s: %w", path, err)
	}
	if len(names) == 0 {
		return "", nil
	}
	return names[0], nil
}

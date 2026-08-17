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

package imagecache

import (
	"io/fs"
	"os"
	"path/filepath"
)

// RemoveAllWritable removes path and everything under it, first making every
// directory owner-writable so its children can be unlinked. Unpacked image
// trees keep the image's (possibly read-only) directory modes, which atelet
// cannot remove as plain root without CAP_DAC_OVERRIDE — os.RemoveAll alone
// fails there with EACCES. atelet owns these files, so chmod needs no
// capability.
func RemoveAllWritable(path string) error {
	// Make dirs traversable/writable top-down (WalkDir visits a directory before
	// reading it, so chmod here lets the walk descend into otherwise-unreadable
	// dirs). Best-effort: ignore errors and let os.RemoveAll surface real ones.
	_ = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			_ = os.Chmod(p, 0o700)
		}
		return nil
	})
	return os.RemoveAll(path)
}

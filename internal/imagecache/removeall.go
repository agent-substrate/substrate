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
// directory owner-writable so its children can be unlinked, and, run as root,
// giving it to uid 0 before that. Unpacked image trees keep the image's
// (possibly read-only) directory modes, which plain root without
// CAP_DAC_OVERRIDE cannot remove: os.RemoveAll alone fails there with EACCES.
// They also keep the image's owners, and unlinking another user's entry from
// a sticky directory another user owns takes owning the directory or
// CAP_FOWNER; CAP_DAC_OVERRIDE does not cover it, and atelet does not hold
// CAP_FOWNER. The chown takes CAP_CHOWN, which atelet holds, and makes root
// the owner, so the chmod and the unlinks need nothing more. An unprivileged
// caller cannot chown and skips it.
func RemoveAllWritable(path string) error {
	asRoot := os.Geteuid() == 0
	// Fix up dirs top-down (WalkDir visits a directory before reading it, so
	// fixing it here lets the walk descend into otherwise-unreadable dirs).
	// Best-effort: ignore errors and let os.RemoveAll surface real ones.
	_ = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			if asRoot {
				_ = os.Lchown(p, 0, -1)
			}
			_ = os.Chmod(p, 0o700)
		}
		return nil
	})
	return os.RemoveAll(path)
}

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

package resources

import (
	"fmt"
	"path/filepath"
)

// ValidateSnapshotFileNames requires each name to be a distinct plain file name
// in the checkpoint directory or its "fs" subdirectory. Actual file access must
// still use os.Root so symlinks cannot escape that directory.
func ValidateSnapshotFileNames(files []string) error {
	seen := make(map[string]bool, len(files))
	for i, name := range files {
		dir := filepath.Dir(name)
		base := filepath.Base(name)
		switch {
		case !filepath.IsLocal(name) || name != filepath.Clean(name) || base == "." || (dir != "." && dir != "fs"):
			return fmt.Errorf("snapshotFiles[%d] %q is not a file name in the checkpoint directory", i, name)
		case seen[name]:
			return fmt.Errorf("snapshotFiles[%d] %q is duplicated", i, name)
		}
		seen[name] = true
	}
	return nil
}

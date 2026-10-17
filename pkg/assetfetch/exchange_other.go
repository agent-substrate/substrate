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

//go:build !linux

package assetfetch

import (
	"os"
	"path/filepath"
)

// exchange moves a over b, both entries of dir below root, if b exists. Unlike
// the Linux version it checks for b and then renames, so a b removed between
// the two is recreated. Plugins run on Linux; this keeps the package building
// and testable elsewhere.
func exchange(root *os.Root, dir, a, b string) error {
	target := filepath.Join(dir, b)
	if _, err := root.Lstat(target); err != nil {
		return err
	}
	return root.Rename(filepath.Join(dir, a), target)
}

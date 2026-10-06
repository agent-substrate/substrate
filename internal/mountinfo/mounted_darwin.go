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

package mountinfo

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mounted reports whether target is the root of one of the named filesystem types.
func Mounted(target string, fsTypes ...string) (bool, error) {
	resolved, err := filepath.EvalSymlinks(target)
	if err != nil {
		return false, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(resolved, &stat); err != nil {
		return false, err
	}
	if cString(stat.Mntonname[:]) != resolved {
		return false, nil
	}
	got := cString(stat.Fstypename[:])
	for _, want := range fsTypes {
		if got == want {
			return true, nil
		}
	}
	return false, nil
}

func cString(value []byte) string {
	for i, b := range value {
		if b == 0 {
			return string(value[:i])
		}
	}
	return string(value)
}

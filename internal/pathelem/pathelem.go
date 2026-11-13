// Copyright 2026 The Agent Substrate Authors
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

// Package pathelem checks that a string is one path element.
package pathelem

import (
	"path/filepath"
	"strings"
)

// IsPlain reports whether name is one path element that is safe to join
// under a directory: not empty, not "." or "..", and without separators or
// NUL.
func IsPlain(name string) bool {
	return name != "" && name != "." && name == filepath.Base(name) && filepath.IsLocal(name) && !strings.Contains(name, "\x00")
}

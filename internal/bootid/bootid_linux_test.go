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

package bootid

import "testing"

// TestReadFromHost reads the boot ID atelet and the workers read at startup,
// which only Linux exposes.
func TestReadFromHost(t *testing.T) {
	got, err := Read(Path)
	if err != nil || got == "" {
		t.Errorf("Read(%q) = (%q, %v), want a non-empty boot ID", Path, got, err)
	}
}

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

package ateompath

import (
	"strings"
	"testing"
)

func TestActorPathUsesUID(t *testing.T) {
	uid1 := "123e4567-e89b-12d3-a456-426614174000"
	uid2 := "987f6543-e21b-32d1-b654-246614174111"

	path1 := ActorPath(uid1)
	path2 := ActorPath(uid2)
	if path1 == path2 {
		t.Fatalf("different actor UIDs produced the same path %q", path1)
	}
	if want := "/actors/" + uid1; !strings.HasSuffix(path1, want) {
		t.Errorf("ActorPath(%q) = %q, want suffix %q", uid1, path1, want)
	}
}

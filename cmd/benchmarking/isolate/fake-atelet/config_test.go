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
	"testing"
	"time"
)

func TestResolveDelays(t *testing.T) {
	got, err := resolveDelays(time.Second, -1, 2*time.Second, 0, -1, -1)
	if err != nil {
		t.Fatalf("resolveDelays: %v", err)
	}
	want := delays{run: time.Second, restore: 2 * time.Second, checkpoint: 0, uploadPausedCheckpoint: time.Second, terminate: time.Second}
	if got != want {
		t.Errorf("resolveDelays = %+v, want %+v", got, want)
	}
	if _, err := resolveDelays(-time.Second, -1, -1, -1, -1, -1); err == nil {
		t.Error("resolveDelays accepted a negative default")
	}
}

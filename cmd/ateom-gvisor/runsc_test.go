//go:build linux

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
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

func TestKillArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.killArgs("my-container", "SIGTERM")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"kill",
		"my-container",
		"SIGTERM",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("killArgs() = %v, want %v", got, want)
	}
}

func TestWaitArgs(t *testing.T) {
	r := &runsc{
		path:     "/usr/bin/runsc",
		actorUID: "test-actor-123",
	}

	got := r.waitArgs("my-container")
	want := []string{
		"-log-format", "json",
		"--alsologtostderr",
		"-root", ateompath.RunSCStateDir("test-actor-123"),
		"wait",
		"my-container",
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("waitArgs() = %v, want %v", got, want)
	}
}

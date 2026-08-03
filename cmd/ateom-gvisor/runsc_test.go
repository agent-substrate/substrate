//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// fakeRunsc writes an executable stub at a unique path and returns it. Each stub
// gets its own path so the supportsAllowConnectedOnSave memo cannot leak between
// test cases.
func fakeRunsc(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runsc")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatalf("writing fake runsc: %v", err)
	}
	return path
}

func TestProbeAllowConnectedOnSave(t *testing.T) {
	tests := []struct {
		name   string
		script string
		want   bool
	}{
		{
			name:   "flag in stdout usage",
			script: "echo '  -allow-connected-on-save  allow checkpoint with connected sockets'\n",
			want:   true,
		},
		{
			name:   "flag in stderr usage",
			script: "echo '  -allow-connected-on-save' >&2\n",
			want:   true,
		},
		{
			name:   "flag absent from usage",
			script: "echo '  -detach  detach from the container'\n",
			want:   false,
		},
		{
			// A build that cannot be probed is assumed to support the flag,
			// preserving the behavior from before capability detection.
			name:   "probe failure assumes supported",
			script: "echo 'unknown command' >&2\nexit 1\n",
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := probeAllowConnectedOnSave(t.Context(), fakeRunsc(t, tt.script))
			if got != tt.want {
				t.Errorf("probeAllowConnectedOnSave = %v, want %v", got, tt.want)
			}
		})
	}
}

// The probe must ask runsc for its top-level flags. -allow-connected-on-save is
// not a subcommand flag, so the per-subcommand usage `runsc help start` prints
// lists only -h and -help even on builds that define it — probing that way
// reports "unsupported" on every build and silently drops the flag where it
// actually works. Real runsc builds were checked against this: the listing from
// `runsc flags` matches the flag's presence in the binary exactly.
func TestProbeAllowConnectedOnSaveUsesFlagsSubcommand(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	path := filepath.Join(dir, "runsc")
	script := "#!/bin/sh\nprintf '%s' \"$*\" > " + argsFile + "\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake runsc: %v", err)
	}

	probeAllowConnectedOnSave(t.Context(), path)

	got, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("probe did not invoke runsc: %v", err)
	}
	if string(got) != "flags" {
		t.Errorf("probe invoked `runsc %s`, want `runsc flags`", got)
	}
}

func TestProbeAllowConnectedOnSaveMissingBinary(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	if !probeAllowConnectedOnSave(t.Context(), missing) {
		t.Error("probeAllowConnectedOnSave on a missing binary = false, want true (fail open)")
	}
}

// The probe shells out, and cmdStart runs on every container start, so the
// result must be cached per binary path.
func TestSupportsAllowConnectedOnSaveMemoizes(t *testing.T) {
	path := fakeRunsc(t, "echo '  -"+allowConnectedOnSaveFlag+"'\n")
	t.Cleanup(func() { runscFlagSupport.Delete(path) })

	if !supportsAllowConnectedOnSave(t.Context(), path) {
		t.Fatal("first call = false, want true")
	}

	// Replacing the stub with one that reports no such flag must not change the
	// answer: a cached result means the binary is never re-probed.
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho '  -detach'\n"), 0o755); err != nil {
		t.Fatalf("rewriting fake runsc: %v", err)
	}
	if !supportsAllowConnectedOnSave(t.Context(), path) {
		t.Error("second call = false, want the memoized true")
	}
}

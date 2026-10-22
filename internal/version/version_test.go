// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package version

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestFormat(t *testing.T) {
	got := Format("v1.2.3", "abc123", "2026-01-02T03:04:05Z", "linux/arm64")
	want := "v1.2.3 commit=abc123 built=2026-01-02T03:04:05Z linux/arm64"
	if got != want {
		t.Errorf("Format() = %q, want %q", got, want)
	}
}

func TestString(t *testing.T) {
	want := Version + " commit=" + Commit + " built=" + BuildDate + " " + runtime.GOOS + "/" + runtime.GOARCH
	if got := String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestFromBuildInfo(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Version: "v0.4.1-0.20261012060913-c4129c94bca0+dirty"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "c4129c94"},
			{Key: "vcs.time", Value: "2026-10-12T06:09:13Z"},
			{Key: "vcs.modified", Value: "true"},
		},
	}
	tests := []struct {
		name                            string
		version, commit, buildDate      string
		info                            *debug.BuildInfo
		wantVersion, wantCommit, wantBD string
	}{
		{
			name:    "unstamped falls back to build info",
			version: "dev", commit: "unknown", buildDate: "unknown",
			info:        info,
			wantVersion: "v0.4.1-0.20261012060913-c4129c94bca0+dirty", wantCommit: "c4129c94-dirty", wantBD: "2026-10-12T06:09:13Z",
		},
		{
			// ko stamps Version alone; it must survive the fallback.
			name:    "stamped version kept",
			version: "v0.5.0", commit: "unknown", buildDate: "unknown",
			info:        info,
			wantVersion: "v0.5.0", wantCommit: "c4129c94-dirty", wantBD: "2026-10-12T06:09:13Z",
		},
		{
			name:    "fully stamped kept",
			version: "v0.5.0", commit: "abc", buildDate: "2026-01-01",
			info:        info,
			wantVersion: "v0.5.0", wantCommit: "abc", wantBD: "2026-01-01",
		},
		{
			name:    "no vcs info",
			version: "dev", commit: "unknown", buildDate: "unknown",
			info:        &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}},
			wantVersion: "dev", wantCommit: "unknown", wantBD: "unknown",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, c, bd := fromBuildInfo(tc.version, tc.commit, tc.buildDate, tc.info)
			if v != tc.wantVersion || c != tc.wantCommit || bd != tc.wantBD {
				t.Errorf("fromBuildInfo() = (%q, %q, %q), want (%q, %q, %q)", v, c, bd, tc.wantVersion, tc.wantCommit, tc.wantBD)
			}
		})
	}
}

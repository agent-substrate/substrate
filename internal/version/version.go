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

// Package version exposes build-time identity for substrate binaries.
// Values are set via -ldflags -X at build time; each one left unset
// falls back to runtime/debug.ReadBuildInfo() so plain `go build` /
// `go install` still produce something meaningful.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		Version, Commit, BuildDate = fromBuildInfo(Version, Commit, BuildDate, info)
	}
}

// fromBuildInfo fills each of version, commit and buildDate that still holds
// its unstamped default from info. Builds may stamp only some of them (ko
// stamps Version alone), so each is decided on its own.
func fromBuildInfo(version, commit, buildDate string, info *debug.BuildInfo) (string, string, string) {
	if version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	var revision, vcsTime string
	var modified bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.time":
			vcsTime = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if commit == "unknown" && revision != "" {
		commit = revision
		if modified {
			commit += "-dirty"
		}
	}
	if buildDate == "unknown" && vcsTime != "" {
		buildDate = vcsTime
	}
	return version, commit, buildDate
}

// String returns a human-readable single-line version summary,
// formatted to sit cleanly after cobra's auto "<binary> version "
// prefix.
func String() string {
	return Format(Version, Commit, BuildDate, Platform())
}

// Platform returns the binary's GOOS/GOARCH, e.g. "linux/amd64".
func Platform() string {
	return runtime.GOOS + "/" + runtime.GOARCH
}

// Format renders build identity the way String does, so that another binary's
// identity (e.g. one reported over RPC) prints in the same shape.
func Format(version, commit, buildDate, platform string) string {
	return fmt.Sprintf("%s commit=%s built=%s %s", version, commit, buildDate, platform)
}

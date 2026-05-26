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
// Values are set via -ldflags -X at build time; if unset, init() falls
// back to runtime/debug.ReadBuildInfo() so plain `go build` /
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
	if Commit != "unknown" {
		return
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			Commit = s.Value
		case "vcs.time":
			BuildDate = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				Commit += "-dirty"
			}
		}
	}
}

// String returns a human-readable single-line version summary,
// formatted to sit cleanly after cobra's auto "<binary> version "
// prefix.
func String() string {
	return fmt.Sprintf("%s commit=%s built=%s %s/%s",
		Version, Commit, BuildDate, runtime.GOOS, runtime.GOARCH)
}

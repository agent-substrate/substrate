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

// Package roottest gates tests that need root privileges (mounts, mknod,
// trusted.* xattrs, ...). Tests call [Require] up top and skip when
// unprivileged; importing this package is also the marker
// hack/run-root-tests.sh uses to discover which packages to rerun as root.
package roottest

import (
	"os"
	"testing"
)

// Require skips the test unless it is running as root. reason names the
// privileged operation the test needs.
func Require(tb testing.TB, reason string) {
	tb.Helper()
	if os.Geteuid() != 0 {
		tb.Skipf("needs root: %s (run hack/run-root-tests.sh to execute under sudo)", reason)
	}
}

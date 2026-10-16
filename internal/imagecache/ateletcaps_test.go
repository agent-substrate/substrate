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

package imagecache

import (
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Capability bits, from linux/capability.h.
const (
	capChown       = 0
	capDacOverride = 1
	capFowner      = 3
	capFsetid      = 4
)

// ateletCapEff is the effective capability set atelet runs with:
// manifests/ate-install/atelet.yaml drops every capability but CHOWN and
// DAC_OVERRIDE.
const ateletCapEff = 1<<capChown | 1<<capDacOverride

// ateletCapsChildEnv marks a test process that runAsRootAndAtelet started
// under atelet's capability set.
const ateletCapsChildEnv = "IMAGECACHE_TEST_ATELET_CAPS_CHILD"

// effectiveCaps returns this process's effective capability set, read from
// /proc/self/status.
func effectiveCaps(t *testing.T) uint64 {
	t.Helper()
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skipf("reading capabilities: %v", err)
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "CapEff:"); ok {
			caps, err := strconv.ParseUint(strings.TrimSpace(v), 16, 64)
			if err != nil {
				t.Fatalf("parsing CapEff %q: %v", v, err)
			}
			return caps
		}
	}
	t.Fatalf("no CapEff in /proc/self/status")
	return 0
}

// hasEffectiveCap reports whether this process holds capability bit in its
// effective set.
func hasEffectiveCap(t *testing.T, bit uint) bool {
	t.Helper()
	return effectiveCaps(t)&(1<<bit) != 0
}

// runAsRootAndAtelet runs body twice, as the subtests "root", with this
// process's capabilities, and "atelet-caps", in a child test process that
// setpriv(1) limits to atelet's capability set. Full root also holds
// CAP_FOWNER, which hides failures atelet hits (unlinking another user's file
// from another user's sticky directory, changing the mode of a file another
// user owns), so a case that must work in atelet runs under both. Callers
// gate on roottest.Require first: only root can reduce its bounding set and
// still hold CHOWN and DAC_OVERRIDE.
func runAsRootAndAtelet(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	t.Run("root", body)
	t.Run("atelet-caps", func(t *testing.T) {
		if os.Getenv(ateletCapsChildEnv) != "" {
			if got := effectiveCaps(t); got != ateletCapEff {
				t.Fatalf("CapEff = %#x, want atelet's %#x", got, ateletCapEff)
			}
			body(t)
			return
		}
		runUnderAteletCaps(t)
	})
}

// runUnderAteletCaps reruns the current test in a child process of this test
// binary whose bounding set holds only CHOWN and DAC_OVERRIDE, so the child
// runs as root with CapEff equal to ateletCapEff, and fails unless the child
// reports that test passed.
func runUnderAteletCaps(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("capability sets are Linux-only")
	}
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Fatalf("setpriv (util-linux) runs root-gated tests with atelet's capabilities; install it: %v", err)
	}
	parts := strings.Split(t.Name(), "/")
	for i, p := range parts {
		parts[i] = "^" + regexp.QuoteMeta(p) + "$"
	}
	args := []string{
		"--bounding-set=-all,+chown,+dac_override", "--inh-caps=-all", "--",
		os.Args[0], "-test.run=" + strings.Join(parts, "/"), "-test.count=1", "-test.v",
	}
	if d, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(d).String())
	}
	cmd := exec.Command(setpriv, args...)
	cmd.Env = append(os.Environ(), ateletCapsChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	// A child that selected no test exits 0 too, so require its PASS line.
	if err != nil || !bytes.Contains(out, []byte("--- PASS: "+t.Name()+" (")) {
		t.Fatalf("%s with atelet's capabilities (CapEff %#x): %v\n%s", t.Name(), ateletCapEff, err, out)
	}
}

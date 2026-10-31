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
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// Capability bits, from linux/capability.h.
const (
	capChown       = 0
	capDacOverride = 1
	capFowner      = 3
	capFsetid      = 4
	capSysAdmin    = 21
	capMknod       = 27
)

// capSet is a capability set a root-gated test runs under, as root with
// every other capability dropped.
type capSet struct {
	// bounding is the set in setpriv(1)'s --bounding-set syntax.
	bounding string
	// eff is the effective set the child process must report.
	eff uint64
}

var (
	// ateletCaps is the set manifests/ate-install/atelet.yaml grants atelet.
	ateletCaps = capSet{
		bounding: "-all,+chown,+dac_override,+fowner,+fsetid",
		eff:      1<<capChown | 1<<capDacOverride | 1<<capFowner | 1<<capFsetid,
	}
	// gvisorWorkerCaps is the part of workerGvisorCapabilities
	// (cmd/atecontroller/internal/controllers) that composing a rootfs uses.
	// Only the microVM worker also gets CAP_FSETID.
	gvisorWorkerCaps = capSet{
		bounding: "-all,+chown,+dac_override,+fowner,+mknod,+sys_admin",
		eff:      1<<capChown | 1<<capDacOverride | 1<<capFowner | 1<<capMknod | 1<<capSysAdmin,
	}
)

// TestAteletCapsMatchManifest checks that ateletCaps, which the root-gated
// tests run atelet's cases under, is the set the atelet container in
// manifests/ate-install/atelet.yaml gets, so a capability dropped from the
// manifest fails here rather than only on nodes.
func TestAteletCapsMatchManifest(t *testing.T) {
	raw, err := os.ReadFile("../../manifests/ate-install/atelet.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var caps *corev1.Capabilities
	for _, doc := range strings.Split(string(raw), "\n---") {
		var ds appsv1.DaemonSet
		if err := yaml.Unmarshal([]byte(doc), &ds); err != nil {
			t.Fatal(err)
		}
		if ds.Kind != "DaemonSet" || ds.Name != "atelet" {
			continue
		}
		for _, c := range ds.Spec.Template.Spec.Containers {
			if c.Name == "atelet" && c.SecurityContext != nil {
				caps = c.SecurityContext.Capabilities
			}
		}
	}
	if caps == nil {
		t.Fatal("atelet.yaml sets no capabilities on the atelet container")
	}
	if !slices.Equal(caps.Drop, []corev1.Capability{"ALL"}) {
		t.Errorf("atelet drops %v, want [ALL]", caps.Drop)
	}
	var got []string
	for _, c := range caps.Add {
		got = append(got, strings.ToLower(string(c)))
	}
	slices.Sort(got)
	want, ok := strings.CutPrefix(ateletCaps.bounding, "-all,+")
	if !ok {
		t.Fatalf("ateletCaps.bounding %q does not start with -all,+", ateletCaps.bounding)
	}
	wantSet := strings.Split(want, ",+")
	slices.Sort(wantSet)
	if !slices.Equal(got, wantSet) {
		t.Errorf("atelet.yaml gives atelet %v, but the tests run its cases under %v", got, wantSet)
	}
}

// capsChildEnv marks a test process that runUnderCaps started under a
// reduced capability set.
const capsChildEnv = "IMAGECACHE_TEST_CAPS_CHILD"

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

// runAsRootAndAtelet runs body twice, as the subtests "root", with this
// process's capabilities, and "atelet-caps", in a child test process limited
// to atelet's capability set. Full root holds capabilities atelet does not
// (CAP_MKNOD, CAP_SYS_ADMIN, CAP_DAC_READ_SEARCH and more), which can hide a
// failure atelet would hit, so a case that must work in atelet runs under
// both. Callers gate on roottest.Require first: only root can reduce its
// bounding set and still hold the capabilities it keeps.
func runAsRootAndAtelet(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	t.Run("root", body)
	runUnderCaps(t, "atelet-caps", ateletCaps, body)
}

// runUnderCaps runs body as the subtest name, in a child process of this test
// binary that runs as root with caps as its effective set, and fails unless
// the child reports that subtest passed.
func runUnderCaps(t *testing.T, name string, caps capSet, body func(t *testing.T)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		if os.Getenv(capsChildEnv) != "" {
			if got := effectiveCaps(t); got != caps.eff {
				t.Fatalf("CapEff = %#x, want %#x", got, caps.eff)
			}
			body(t)
			return
		}
		runChildUnderCaps(t, caps)
	})
}

// runChildUnderCaps reruns the current test in a child process of this test
// binary whose bounding set is caps, so the child runs as root with CapEff
// equal to caps.eff, and fails unless the child reports that test passed.
func runChildUnderCaps(t *testing.T, caps capSet) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("capability sets are Linux-only")
	}
	setpriv, err := exec.LookPath("setpriv")
	if err != nil {
		t.Fatalf("setpriv (util-linux) runs root-gated tests with reduced capabilities; install it: %v", err)
	}
	parts := strings.Split(t.Name(), "/")
	for i, p := range parts {
		parts[i] = "^" + regexp.QuoteMeta(p) + "$"
	}
	args := []string{
		"--bounding-set=" + caps.bounding, "--inh-caps=-all", "--",
		os.Args[0], "-test.run=" + strings.Join(parts, "/"), "-test.count=1", "-test.v",
	}
	if d, ok := t.Deadline(); ok {
		args = append(args, "-test.timeout="+time.Until(d).String())
	}
	cmd := exec.Command(setpriv, args...)
	cmd.Env = append(os.Environ(), capsChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	// A child that selected no test exits 0 too, so require its PASS line.
	if err != nil || !bytes.Contains(out, []byte("--- PASS: "+t.Name()+" (")) {
		t.Fatalf("%s with CapEff %#x: %v\n%s", t.Name(), caps.eff, err, out)
	}
}

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

package ateomnet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/vishvananda/netns"
)

// TestValidateProcSysPath pins the guard that keeps ensureProcSysOn from being
// pointed at an arbitrary file.
func TestValidateProcSysPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		ok   bool
	}{
		{"ipv4_forwarding", procSysIPv4Forwarding, true},
		{"ipv6_forwarding", procSysIPv6Forwarding, true},
		{"cleans_to_a_sysctl", "/proc/sys/kernel/../net/ipv4/ip_forward", true},
		{"mount_point_itself", procSysDir, false},
		{"sibling_prefix", "/proc/sysfoo/net/ipv4/ip_forward", false},
		{"escapes_the_tree", "/proc/sys/../etc/passwd", false},
		{"outside_the_tree", "/etc/passwd", false},
		{"relative", "proc/sys/net/ipv4/ip_forward", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateProcSysPath(tc.path)
			if tc.ok && err != nil {
				t.Errorf("validateProcSysPath(%q) = %v, want nil", tc.path, err)
			}
			if !tc.ok && err == nil {
				t.Errorf("validateProcSysPath(%q) = nil, want a refusal", tc.path)
			}
		})
	}
}

// TestEnsureProcSysOnRefusesForeignPath checks the guard is actually wired into
// the helper, not just available: a refused path must not be created.
func TestEnsureProcSysOnRefusesForeignPath(t *testing.T) {
	p := filepath.Join(t.TempDir(), "forwarding")
	if err := ensureProcSysOn(p); err == nil {
		t.Fatalf("ensureProcSysOn(%q) = nil, want a refusal for a path outside %s", p, procSysDir)
	}
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Errorf("ensureProcSysOn touched %q (stat err = %v); a refused path must be left alone", p, err)
	}
}

// TestProcSysIsSet covers the read that decides whether the write happens: a
// node already reading "1" is left byte-for-byte alone, and anything else
// (including unreadable) needs the write.
func TestProcSysIsSet(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name    string
		content string // empty means: do not create the file
		want    bool
	}{
		{"set", "1\n", true},
		{"set_with_trailing_content", "1 other-content\n", true},
		{"unset", "0\n", false},
		{"empty_file", "", false},
		{"absent", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(dir, tc.name)
			if tc.name != "absent" {
				if err := os.WriteFile(p, []byte(tc.content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := procSysIsSet(p); got != tc.want {
				t.Errorf("procSysIsSet(%s = %q) = %v, want %v", tc.name, tc.content, got, tc.want)
			}
		})
	}
}

// TestEnableForwardingInNetNS asserts the effect EnableForwarding exists for,
// in the place it runs: a throwaway netns standing in for the worker pod's.
//
// Both nodes are pinned to "0" first. A new netns inherits the parent's IPv4
// forwarding value (a host with forwarding on hands its namespaces "1"), so
// asserting on the transition this function is responsible for is both
// meaningful on any host and independent of that inheritance.
func TestEnableForwardingInNetNS(t *testing.T) {
	roottest.Require(t, "writing /proc/sys/net sysctls inside a fresh network namespace")

	withTestNetNS(t, func(netns.NsHandle) {
		writeSysctl(t, procSysIPv4Forwarding, "0")
		if got := readSysctl(t, procSysIPv4Forwarding); got != "0" {
			t.Fatalf("pinning %s to %q read back %q", procSysIPv4Forwarding, "0", got)
		}
		if err := EnableForwarding(); err != nil {
			t.Fatalf("EnableForwarding: %v", err)
		}
		if got := readSysctl(t, procSysIPv4Forwarding); got != "1" {
			t.Errorf("%s = %q after EnableForwarding, want %q", procSysIPv4Forwarding, got, "1")
		}

		// IPv6 is optional: a kernel built without it has no such node, and
		// EnableForwarding treats that as benign and returns nil.
		if _, err := os.Stat(procSysIPv6Forwarding); err != nil {
			t.Logf("no %s in this environment, skipping the IPv6 half: %v", procSysIPv6Forwarding, err)
		} else {
			writeSysctl(t, procSysIPv6Forwarding, "0")
			if err := EnableForwarding(); err != nil {
				t.Fatalf("EnableForwarding with %s pinned off: %v", procSysIPv6Forwarding, err)
			}
			if got := readSysctl(t, procSysIPv6Forwarding); got != "1" {
				t.Errorf("%s = %q after EnableForwarding, want %q", procSysIPv6Forwarding, got, "1")
			}
		}

		// Idempotent: the second call finds both nodes already set and must
		// neither fail nor need the read-only remount.
		if err := EnableForwarding(); err != nil {
			t.Errorf("second EnableForwarding: %v", err)
		}
	})
}

// readSysctl returns the trimmed contents of a sysctl node.
func readSysctl(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return strings.TrimSpace(string(b))
}

// writeSysctl pins a sysctl node to value.
func writeSysctl(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

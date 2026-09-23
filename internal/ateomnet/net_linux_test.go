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
	"context"
	"errors"
	"os"
	"testing"

	"github.com/agent-substrate/substrate/internal/roottest"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

func TestNamedNetNSRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{"", ".", "..", "/absolute", "../outside", "nested/name", "ateom-actor:uid/../../outside", "nul\x00name"} {
		t.Run(name, func(t *testing.T) {
			if err := removeNamedNetNS(name); !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("removeNamedNetNS(%q): got %v, want invalid name", name, err)
			}
			handle, err := CreateNetNSWithoutSwitching(name)
			if err == nil {
				handle.Close()
			}
			if !errors.Is(err, os.ErrInvalid) {
				t.Fatalf("CreateNetNSWithoutSwitching(%q): got %v, want invalid name", name, err)
			}
		})
	}
}

func TestRemoveNamedNetNSDoesNotFollowSymlinks(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	const targetName = "ateomnet-symlink-target-test"
	const linkName = "ateomnet-symlink-test"
	targetPath := "/run/netns/" + targetName
	linkPath := "/run/netns/" + linkName
	target, err := CreateNetNSWithoutSwitching(targetName)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	t.Cleanup(func() { _ = removeNamedNetNS(targetName) })
	before, err := os.Stat(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(linkPath) })
	if err := removeNamedNetNS(linkName); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(linkPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected symlink to be removed, got %v", err)
	}
	after, err := os.Stat(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("cleanup unmounted the symlink target")
	}
}

func TestCreateNetNSWithoutSwitchingReplacesALeftover(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	for _, state := range []string{"mounted", "unmounted"} {
		t.Run(state, func(t *testing.T) {
			name := "ateomnet-leftover-test-" + state
			path := "/run/netns/" + name
			t.Cleanup(func() { _ = removeNamedNetNS(name) })

			// Held open across the replacement below: unlinking the name
			// must not invalidate a handle the caller still has.
			first, err := CreateNetNSWithoutSwitching(name)
			if err != nil {
				t.Fatalf("first CreateNetNSWithoutSwitching: %v", err)
			}
			defer first.Close()
			if state == "unmounted" {
				if err := unix.Unmount(path, unix.MNT_DETACH); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("expected the leftover netns to remain: %v", err)
			}

			second, err := CreateNetNSWithoutSwitching(name)
			if err != nil {
				t.Fatalf("the name is wedged by its own leftover: %v", err)
			}
			defer second.Close()
			if !second.IsOpen() {
				t.Error("the replacement namespace is not open")
			}
			if second.Equal(first) {
				t.Error("the replacement is the leftover namespace, not a new one")
			}
			// The retained handle still names the original namespace, which
			// the replacement neither destroyed nor took over.
			if !first.IsOpen() {
				t.Error("the retained handle closed when its name was replaced")
			}
			if err := NetNSDo(context.Background(), first, func(context.Context) error {
				_, err := netlink.LinkList()
				return err
			}); err != nil {
				t.Errorf("the retained handle is no longer usable: %v", err)
			}
			for range 2 {
				if err := removeNamedNetNS(name); err != nil {
					t.Fatalf("removing namespace: %v", err)
				}
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expected namespace path to be removed, got %v", err)
				}
			}
		})
	}
}

// Only EROFS takes the remount path: any other error is reported as it is,
// and /proc/sys is left as it was found. Remounting it read-only on the way
// out would break every later write.
func TestSetNetSysctlReportsAnUnrelatedError(t *testing.T) {
	err := setNetSysctl("net/ipv4/ateomnet_no_such_sysctl", "0")
	if !errors.Is(err, unix.ENOENT) {
		t.Fatalf("setNetSysctl() on a missing key: got %v, want ENOENT", err)
	}
	var st unix.Statfs_t
	if err := unix.Statfs("/proc/sys", &st); err != nil {
		t.Fatalf("statfs /proc/sys: %v", err)
	}
	if st.Flags&unix.ST_RDONLY != 0 {
		t.Error("/proc/sys was left read-only")
	}
}

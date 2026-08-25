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

package kata

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The kernel requires overlay upperdir and workdir on the same filesystem and
// rejects a workdir nested inside (or equal to) upperdir — so they must be
// SIBLINGS under the container's subdirectory of the actor's upper base. The
// layout is also the snapshot tar's entry layout (<cid>/fs, <cid>/work), so a
// change here breaks every overlay mount AND every existing snapshot.
func TestUpperWorkDirsAreSiblings(t *testing.T) {
	const base = "/var/lib/ateom-gvisor/actors/uid/rootfs-upper"
	upper, work := UpperWorkDirs(base, "app")
	cidDir := filepath.Join(base, "app")
	if filepath.Dir(upper) != cidDir || filepath.Dir(work) != cidDir {
		t.Errorf("UpperWorkDirs = %q, %q; want both directly under %q", upper, work, cidDir)
	}
	if upper == work {
		t.Errorf("UpperWorkDirs: upper and work are the same directory %q", upper)
	}
	if strings.HasPrefix(work+"/", upper+"/") {
		t.Errorf("UpperWorkDirs: work %q is nested inside upper %q", work, upper)
	}
	// Tar-layout invariant: entries are <cid>/fs and <cid>/work.
	if upper != filepath.Join(base, "app", "fs") || work != filepath.Join(base, "app", "work") {
		t.Errorf("UpperWorkDirs = %q, %q; want the snapshot layout <base>/app/{fs,work}", upper, work)
	}
}

// The volume subtrees ride the ONE kataShared device (BindIntoShare): the host
// side is bind-mounted under the directory virtiofsd serves, and the guest
// re-opens the same relative path under its kataShared mount — find-paths
// re-opens open volume files by path on restore, so host and guest must agree.
func TestVolumeSubtreePaths(t *testing.T) {
	for name, guest := range map[string]string{
		"durable": GuestDurableVolumeDir("data"),
		"csi":     GuestCSIVolumeDir("data"),
	} {
		host := filepath.Join(SharedDir("uid"), name, "data")
		hostRel, err := filepath.Rel(SharedDir("uid"), host)
		if err != nil {
			t.Fatalf("Rel(host): %v", err)
		}
		guestRel := strings.TrimPrefix(guest, guestSharedDir)
		if hostRel != guestRel {
			t.Errorf("%s: host-relative path %q != guest-relative path %q; find-paths would re-open the wrong file", name, hostRel, guestRel)
		}
	}
}

func TestVirtiofsdArgs(t *testing.T) {
	args := virtiofsdArgs(VirtiofsdOptions{
		SocketPath: "/run/vm/virtiofsd.sock",
		SharedDir:  "/run/kata-containers/shared/sandboxes/uid/shared",
	})
	if !slices.Contains(args, "--cache=auto") {
		t.Errorf("args %v do not contain --cache=auto", args)
	}
	// The host kernel owns the overlay; the guest needs no xattr passthrough, so
	// the flag must never be emitted.
	if slices.Contains(args, "--xattr") {
		t.Errorf("args %v contain --xattr; the guest has no overlay to feed it to", args)
	}
}

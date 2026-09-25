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

package ateomcgroup

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/roottest"
)

// A process started with the leaf's SysProcAttr is born inside the leaf.
func TestActorLeafStartsProcessesInside(t *testing.T) {
	roottest.Require(t, "creates cgroups")
	root := ownCgroup(t)
	self := "/" + strings.TrimPrefix(root, Root)

	leaf, err := openActorLeaf(root, "ateomcgroup-test", 500)
	if err != nil {
		t.Skipf("cannot create a cgroup here: %v", err)
	}
	cmd := exec.Command("sleep", "30")
	cmd.SysProcAttr = leaf.SysProcAttr()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = leaf.Close()
	got, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid), "cgroup"))
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if want := "0::" + filepath.Join(self, "ateomcgroup-test"); !strings.Contains(string(got), want) {
		t.Errorf("process cgroup = %q, want %q", got, want)
	}
	if err := removeActorLeaf(root, "ateomcgroup-test"); err != nil {
		t.Errorf("removing the leaf after its process exited: %v", err)
	}
}

// Killing the leaf takes down everything in it, and leaves it removable.
func TestKillActorLeaf(t *testing.T) {
	roottest.Require(t, "creates cgroups")
	root := ownCgroup(t)
	leaf, err := openActorLeaf(root, "ateomcgroup-kill-test", 0)
	if err != nil {
		t.Skipf("cannot create a cgroup here: %v", err)
	}
	// A parent that forks a child, as virtiofsd does.
	cmd := exec.Command("sh", "-c", "sleep 30 & wait")
	cmd.SysProcAttr = leaf.SysProcAttr()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = leaf.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := KillLeafUnder(ctx, root, "ateomcgroup-kill-test"); err != nil {
		t.Fatalf("KillLeafUnder: %v", err)
	}
	_ = cmd.Wait()
	if err := removeActorLeaf(root, "ateomcgroup-kill-test"); err != nil {
		t.Errorf("removing the killed leaf: %v", err)
	}
	// Already gone: nothing to kill.
	if err := KillLeafUnder(ctx, root, "ateomcgroup-kill-test"); err != nil {
		t.Errorf("KillLeafUnder on a removed leaf: %v", err)
	}
}

// ownCgroup is the test process's own cgroup, under which it may create leaves.
func ownCgroup(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			return filepath.Join(Root, p)
		}
	}
	t.Skip("no cgroup v2")
	return ""
}

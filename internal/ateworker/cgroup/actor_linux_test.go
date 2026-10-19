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

package cgroup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/roottest"
	"golang.org/x/sys/unix"
)

const (
	benchHelperEnvVar = "ATE_CGROUP_BENCH_HELPER"
	benchCPUEnvVar    = "ATE_CGROUP_BENCH_CPU"
	benchSteadyActors = 8
	benchBurstIters   = 25_000_000
)

// TestMain runs the CPU-pinned worker loop when re-invoked as a benchmark
// child process, and otherwise runs the package test suite.
func TestMain(m *testing.M) {
	if mode := os.Getenv(benchHelperEnvVar); mode != "" {
		runCPUWeightBenchHelper(mode)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// A process started with the leaf's SysProcAttr is born inside the leaf.
func TestActorLeafStartsProcessesInside(t *testing.T) {
	roottest.Require(t, "creates cgroups")
	root := ownCgroup(t)
	self := "/" + strings.TrimPrefix(root, Root)

	leaf, err := openActorLeaf(root, "cgroup-test", 500)
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
	if want := "0::" + filepath.Join(self, "cgroup-test"); !strings.Contains(string(got), want) {
		t.Errorf("process cgroup = %q, want %q", got, want)
	}
	if err := removeActorLeaf(root, "cgroup-test"); err != nil {
		t.Errorf("removing the leaf after its process exited: %v", err)
	}
}

// Killing the leaf takes down everything in it, and leaves it removable.
func TestKillActorLeaf(t *testing.T) {
	roottest.Require(t, "creates cgroups")
	root := ownCgroup(t)
	leaf, err := openActorLeaf(root, "cgroup-kill-test", 0)
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
	if err := KillLeafUnder(ctx, root, "cgroup-kill-test"); err != nil {
		t.Fatalf("KillLeafUnder: %v", err)
	}
	_ = cmd.Wait()
	if err := removeActorLeaf(root, "cgroup-kill-test"); err != nil {
		t.Errorf("removing the killed leaf: %v", err)
	}
	// Already gone: nothing to kill.
	if err := KillLeafUnder(ctx, root, "cgroup-kill-test"); err != nil {
		t.Errorf("KillLeafUnder on a removed leaf: %v", err)
	}
}

// BenchmarkActorLeafCPUWeightContention measures the wall-clock completion time
// of a CPU-bound lifecycle transition burst contending on a single CPU core
// against 8 steady-state actor leaves running at DefaultCPUWeight (100),
// comparing transition priority (boost=true, MaxCPUWeight=10000) against
// unboosted priority (boost=false, DefaultCPUWeight=100).
func BenchmarkActorLeafCPUWeightContention(b *testing.B) {
	parent := delegatedCPUCgroup(b)
	cpu := firstUsableCPU(b)

	for i := range benchSteadyActors {
		name := fmt.Sprintf("steady-%d", i)
		leaf, _, _ := startCPUWeightBenchWorker(b, parent, name, DefaultCPUWeight, "steady", cpu)
		_ = leaf.Close()
	}

	transitionLeaf, trigW, ackR := startCPUWeightBenchWorker(b, parent, "transition", MaxCPUWeight, "burst", cpu)
	b.Cleanup(func() {
		_ = transitionLeaf.Close()
	})

	var buf [1]byte
	for _, tc := range []struct {
		name   string
		weight int
	}{
		{name: "boost=true", weight: MaxCPUWeight},
		{name: "boost=false", weight: DefaultCPUWeight},
	} {
		b.Run(tc.name, func(b *testing.B) {
			if err := transitionLeaf.SetCPUWeight(tc.weight); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := trigW.Write(buf[:]); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(ackR, buf[:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// startCPUWeightBenchWorker opens an actor leaf under parent with the given
// cpu.weight, starts a CPU-pinned child worker inside that leaf, and waits for
// its readiness byte before returning.
func startCPUWeightBenchWorker(b *testing.B, parent, name string, weight int, mode string, cpu int) (*ActorLeaf, io.WriteCloser, io.ReadCloser) {
	b.Helper()
	exe, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	leaf, err := openActorLeaf(parent, name, 0)
	if err != nil {
		b.Fatal(err)
	}
	if err := leaf.SetCPUWeight(weight); err != nil {
		_ = leaf.Close()
		b.Fatal(err)
	}
	cmd := exec.Command(exe)
	cmd.SysProcAttr = leaf.SysProcAttr()
	cmd.Env = append(os.Environ(),
		benchHelperEnvVar+"="+mode,
		benchCPUEnvVar+"="+strconv.Itoa(cpu),
	)
	stdinW, err := cmd.StdinPipe()
	if err != nil {
		_ = leaf.Close()
		b.Fatal(err)
	}
	stdoutR, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdinW.Close()
		_ = leaf.Close()
		b.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdinW.Close()
		_ = stdoutR.Close()
		_ = leaf.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = stdinW.Close()
		_ = stdoutR.Close()
		_ = leaf.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := KillLeafUnder(ctx, parent, name); err != nil {
			b.Errorf("KillLeafUnder(%q): %v", name, err)
		}
		_ = cmd.Wait()
		if err := removeActorLeaf(parent, name); err != nil {
			b.Errorf("removeActorLeaf(%q): %v", name, err)
		}
	})

	var ready [1]byte
	if _, err := io.ReadFull(stdoutR, ready[:]); err != nil {
		b.Fatalf("waiting for %s worker readiness: %v", name, err)
	}
	return leaf, stdinW, stdoutR
}

// runCPUWeightBenchHelper pins every thread in the current process to the
// configured CPU core, writes a readiness byte to stdout, and runs either a
// continuous busy loop ("steady") or a pipe-triggered CPU burst loop ("burst").
func runCPUWeightBenchHelper(mode string) {
	runtime.GOMAXPROCS(1)
	runtime.LockOSThread()

	cpu, err := strconv.Atoi(os.Getenv(benchCPUEnvVar))
	if err != nil {
		os.Exit(2)
	}
	var mask unix.CPUSet
	mask.Set(cpu)
	if err := unix.SchedSetaffinity(0, &mask); err != nil {
		os.Exit(3)
	}
	if entries, err := os.ReadDir("/proc/self/task"); err == nil {
		for _, entry := range entries {
			if tid, err := strconv.Atoi(entry.Name()); err == nil {
				_ = unix.SchedSetaffinity(tid, &mask)
			}
		}
	}

	var buf [1]byte
	if _, err := os.Stdout.Write(buf[:]); err != nil {
		os.Exit(4)
	}

	switch mode {
	case "steady":
		var acc uint64
		for {
			acc++
			if acc == ^uint64(0) {
				_, _ = os.Stdout.Write(buf[:])
			}
		}
	case "burst":
		for {
			if _, err := io.ReadFull(os.Stdin, buf[:]); err != nil {
				return
			}
			var acc uint64
			for i := range uint64(benchBurstIters) {
				acc += i*31 + 17
			}
			buf[0] = byte(acc)
			if _, err := os.Stdout.Write(buf[:]); err != nil {
				return
			}
		}
	}
}

// delegatedCPUCgroup creates and returns a temporary cgroup v2 directory with
// the cpu controller enabled in cgroup.subtree_control, walking up from the
// test process's own cgroup toward Root until it finds a writable ancestor that
// delegates cpu.
func delegatedCPUCgroup(tb testing.TB) string {
	tb.Helper()
	for dir := ownCgroup(tb); strings.HasPrefix(dir, Root); dir = filepath.Dir(dir) {
		parent, err := os.MkdirTemp(dir, "ate-cpuweight-bench-*")
		if err != nil {
			if dir == Root {
				break
			}
			continue
		}
		if err := os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("+cpu"), 0o644); err != nil {
			_ = os.Remove(parent)
			if dir == Root {
				break
			}
			continue
		}
		tb.Cleanup(func() {
			if err := os.Remove(parent); err != nil {
				tb.Errorf("removing temporary parent cgroup %q: %v", parent, err)
			}
		})
		return parent
	}
	tb.Skip("no writable cgroup v2 directory with cpu controller delegated")
	return ""
}

// firstUsableCPU returns the lowest CPU index permitted by the current
// thread's scheduling affinity mask.
func firstUsableCPU(tb testing.TB) int {
	tb.Helper()
	var mask unix.CPUSet
	if err := unix.SchedGetaffinity(0, &mask); err != nil {
		tb.Fatalf("SchedGetaffinity: %v", err)
	}
	for cpu := range runtime.NumCPU() * 4 {
		if mask.IsSet(cpu) {
			return cpu
		}
	}
	tb.Fatal("no usable CPU in affinity mask")
	return 0
}

// ownCgroup is the test process's own cgroup, under which it may create leaves.
func ownCgroup(tb testing.TB) string {
	tb.Helper()
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		tb.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			return filepath.Join(Root, p)
		}
	}
	tb.Skip("no cgroup v2")
	return ""
}

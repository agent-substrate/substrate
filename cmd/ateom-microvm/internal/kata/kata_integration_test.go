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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	task "github.com/containerd/containerd/api/runtime/task/v2"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"golang.org/x/sys/unix"
)

// TestKataLifecycle drives a real containerd-shim-kata-v2 (no containerd) through
// the full RUN lifecycle in a cloud-hypervisor micro-VM, then pause/resume, and
// tears it down. It requires a host with kata + /dev/kvm and is gated behind
// KATA_INTEGRATION=1.
//
// Env:
//
//	KATA_INTEGRATION=1            enable the test
//	KATA_SHIM=<path>             shim binary (default /opt/kata/bin/containerd-shim-kata-v2)
//	KATA_ROOTFS_SRC=<dir>        a populated rootfs to copy into the bundle (e.g. an
//	                             extracted busybox image). Required.
//	KATA_STDIO=1                 wire container stdout/stderr to files in the bundle.
//
// Run as root on a KVM-capable Linux host with kata installed, e.g.:
//
//	sudo KATA_INTEGRATION=1 KATA_ROOTFS_SRC=/tmp/bb/rootfs ./kata.test -test.v -test.run Lifecycle
func TestKataLifecycle(t *testing.T) {
	if os.Getenv("KATA_INTEGRATION") != "1" {
		t.Skip("set KATA_INTEGRATION=1 to run (requires kata + /dev/kvm)")
	}
	rootfsSrc := os.Getenv("KATA_ROOTFS_SRC")
	if rootfsSrc == "" {
		t.Fatal("KATA_ROOTFS_SRC is required (a populated rootfs dir to copy into the bundle)")
	}
	shim := os.Getenv("KATA_SHIM")
	if shim == "" {
		shim = "/opt/kata/bin/containerd-shim-kata-v2"
	}

	id := fmt.Sprintf("ateomchv-it-%d", os.Getpid())
	work := filepath.Join("/tmp", id)
	bundle := filepath.Join(work, "bundle")
	rootfs := filepath.Join(bundle, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(work) })

	// Populate the bundle rootfs (atelet's model: a pre-populated rootfs dir).
	if out, err := exec.Command("cp", "-a", rootfsSrc+"/.", rootfs+"/").CombinedOutput(); err != nil {
		t.Fatalf("copying rootfs: %v: %s", err, out)
	}
	writeBundleConfig(t, bundle, id)

	// containerd creates a "log" fifo in the bundle and reads it; the kata shim
	// opens it O_WRONLY for its logrus output (and blocks if nobody reads).
	// Provide it and drain to t.Log so we can see why the shim behaves as it does.
	logFifo := filepath.Join(bundle, "log")
	if err := unix.Mkfifo(logFifo, 0o700); err != nil {
		t.Fatalf("mkfifo log: %v", err)
	}
	go func() {
		f, err := os.OpenFile(logFifo, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		defer f.Close()
		buf := make([]byte, 4096)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				t.Logf("[shim-log] %s", buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()

	var createOpts CreateOptions
	if os.Getenv("KATA_STDIO") == "1" {
		createOpts.Stdout = filepath.Join(work, "stdout")
		createOpts.Stderr = filepath.Join(work, "stderr")
	}

	s := &Shim{
		Binary:       shim,
		ID:           id,
		Bundle:       bundle,
		Namespace:    "default",
		GRPCAddress:  filepath.Join(work, "fake-containerd.sock"),
		TTRPCAddress: filepath.Join(work, "fake-containerd.sock.ttrpc"),
		Diagnostics:  testWriter{t},
		Debug:        true,
		ConfigFile:   os.Getenv("KATA_CONF_FILE"), // optional: test a generated config
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Always attempt teardown so a failure mid-test doesn't leak a VMM.
	defer func() {
		_ = s.Kill(ctx, 9, true)
		shutCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		_ = s.Shutdown(shutCtx, true)
		c()
		_ = s.Close()
		clCtx, c2 := context.WithTimeout(context.Background(), 15*time.Second)
		_ = s.CleanupAction(clCtx)
		c2()
	}()

	if err := s.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	t.Logf("shim ttrpc address: %s", s.Address())

	pid, err := s.Create(ctx, createOpts)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Logf("Create ok, pid=%d", pid)

	// The CH api-socket should now exist for this sandbox id.
	clh := CLHSocketPath(id)
	if !waitFor(5*time.Second, func() bool { _, err := os.Stat(clh); return err == nil }) {
		t.Errorf("clh api-socket %q did not appear", clh)
	} else {
		t.Logf("clh api-socket present: %s", clh)
	}

	if _, err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Log("Start ok")

	assertStatus(t, ctx, s, tasktypes.Status_RUNNING)

	if err := s.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	assertStatus(t, ctx, s, tasktypes.Status_PAUSED)
	t.Log("Pause ok")

	if err := s.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	assertStatus(t, ctx, s, tasktypes.Status_RUNNING)
	t.Log("Resume ok")

	// Graceful-ish: kill the container, then delete, then shutdown (defer also
	// force-cleans).
	if err := s.Kill(ctx, 9, true); err != nil {
		t.Errorf("Kill: %v", err)
	}
	time.Sleep(time.Second)
	if err := s.Delete(ctx); err != nil {
		t.Logf("Delete (non-fatal): %v", err)
	}
	t.Log("lifecycle complete")
}

func assertStatus(t *testing.T, ctx context.Context, s *Shim, want tasktypes.Status) {
	t.Helper()
	resp, err := s.task.State(ctx, &task.StateRequest{ID: s.ID})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if resp.GetStatus() != want {
		t.Errorf("status = %v, want %v", resp.GetStatus(), want)
	}
}

func waitFor(d time.Duration, cond func() bool) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// writeBundleConfig emits an OCI spec mirroring what `ctr run --runtime
// io.containerd.kata.v2` produces (the proven-good shape). kata's OCI conversion
// nil-derefs without linux.resources, so a too-minimal spec crashes the shim.
func writeBundleConfig(t *testing.T, bundle, id string) {
	t.Helper()
	caps := []string{
		"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD",
		"CAP_NET_RAW", "CAP_SETGID", "CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP",
		"CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
	}
	spec := map[string]any{
		"ociVersion": "1.2.0",
		"process": map[string]any{
			"user": map[string]any{"uid": 0, "gid": 0, "additionalGids": []int{0, 10}},
			"args": []string{"sleep", "3600"},
			"env":  []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			"cwd":  "/",
			"capabilities": map[string]any{
				"bounding": caps, "effective": caps, "permitted": caps,
			},
			"rlimits":         []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}},
			"noNewPrivileges": true,
		},
		"root": map[string]any{"path": "rootfs"},
		"mounts": []map[string]any{
			{"destination": "/proc", "type": "proc", "source": "proc", "options": []string{"nosuid", "noexec", "nodev"}},
			{"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
			{"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
			{"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": []string{"nosuid", "noexec", "nodev"}},
			{"destination": "/sys", "type": "sysfs", "source": "sysfs", "options": []string{"nosuid", "noexec", "nodev", "ro"}},
			{"destination": "/run", "type": "tmpfs", "source": "tmpfs", "options": []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		},
		"linux": map[string]any{
			"resources": map[string]any{
				"devices": []map[string]any{
					{"allow": false, "access": "rwm"},
					{"allow": true, "type": "c", "major": 1, "minor": 3, "access": "rwm"},
					{"allow": true, "type": "c", "major": 1, "minor": 8, "access": "rwm"},
					{"allow": true, "type": "c", "major": 1, "minor": 7, "access": "rwm"},
					{"allow": true, "type": "c", "major": 5, "minor": 0, "access": "rwm"},
					{"allow": true, "type": "c", "major": 1, "minor": 5, "access": "rwm"},
					{"allow": true, "type": "c", "major": 1, "minor": 9, "access": "rwm"},
					{"allow": true, "type": "c", "major": 5, "minor": 1, "access": "rwm"},
					{"allow": true, "type": "c", "major": 136, "access": "rwm"},
					{"allow": true, "type": "c", "major": 5, "minor": 2, "access": "rwm"},
				},
				"cpu": map[string]any{"shares": 1024},
			},
			"cgroupsPath": "/ateomchv/" + id,
			"namespaces": []map[string]any{
				{"type": "pid"},
				{"type": "ipc"},
				{"type": "uts"},
				{"type": "mount"},
				{"type": "network"},
			},
			"maskedPaths":   []string{"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats", "/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware", "/sys/devices/virtual/powercap", "/proc/scsi"},
			"readonlyPaths": []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"},
		},
	}
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

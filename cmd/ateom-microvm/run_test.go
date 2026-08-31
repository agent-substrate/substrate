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

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
)

// A symlink planted in the image at /etc or /etc/resolv.conf must not be followed
// out of the rootfs, or the image picks what ateom overwrites as root.
func TestWriteGuestResolvConfSymlinkEscape(t *testing.T) {
	for _, tc := range []struct {
		name string
		link string // rootfs-relative path planted as a symlink to the canary
		// A planted resolv.conf is just unlinked; only an escaping directory fails.
		wantErr bool
	}{
		{name: "etc dir", link: "etc", wantErr: true},
		{name: "resolv.conf", link: "etc/resolv.conf"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			canary := filepath.Join(dir, "canary")
			if err := os.WriteFile(canary, []byte("INITIAL_STATE"), 0o644); err != nil {
				t.Fatal(err)
			}
			rootfs := filepath.Join(dir, "rootfs")
			if err := os.MkdirAll(filepath.Join(rootfs, filepath.Dir(tc.link)), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(canary, filepath.Join(rootfs, tc.link)); err != nil {
				t.Fatal(err)
			}

			if err := writeGuestResolvConf(rootfs); (err != nil) != tc.wantErr {
				t.Errorf("writeGuestResolvConf(%q) error = %v, wantErr %v", rootfs, err, tc.wantErr)
			}
			got, err := os.ReadFile(canary)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "INITIAL_STATE" {
				t.Errorf("canary = %q, want it untouched: the symlink was followed out of the rootfs", got)
			}
		})
	}
}

func TestWriteGuestResolvConf(t *testing.T) {
	want, err := os.ReadFile("/etc/resolv.conf")
	if err != nil || len(want) == 0 {
		t.Skipf("no host /etc/resolv.conf to copy: %v", err)
	}
	rootfs := t.TempDir()

	if err := writeGuestResolvConf(rootfs); err != nil {
		t.Fatalf("writeGuestResolvConf(%q) = %v", rootfs, err)
	}

	got, err := os.ReadFile(filepath.Join(rootfs, "etc", "resolv.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("guest resolv.conf = %q, want %q", got, want)
	}
	// A second boot of the same bundle must not fail on the file it just wrote.
	if err := writeGuestResolvConf(rootfs); err != nil {
		t.Errorf("writeGuestResolvConf(%q) second call = %v", rootfs, err)
	}
}

// A vsock socket that has gone missing means cloud-hypervisor stopped the VM
// (it unlinks the socket in the vsock device's shutdown), so the poll must give
// up at once instead of spending the whole timeout on a guest that is gone.
func TestDialAgentRetryGuestStopped(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "clh.sock")

	start := time.Now()
	_, err := dialAgentRetry(t.Context(), missing, 30*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want it to wrap errGuestStopped", missing, err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("dialAgentRetry(%q) took %v; want it to give up immediately, not poll for the full timeout", missing, elapsed)
	}
}

// The socket exists but nothing answers yet: that is every dial until the guest
// reaches kata-containers.target, so it must keep polling to the timeout.
func TestDialAgentRetryNotListeningYet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	// Leave the socket file behind, as cloud-hypervisor does while the VM runs:
	// connecting is then refused, which is what an unanswered CONNECT looks like.
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	const timeout = 700 * time.Millisecond
	start := time.Now()
	_, err = dialAgentRetry(t.Context(), path, timeout)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("dialAgentRetry(%q) succeeded, want an error", path)
	}
	if errors.Is(err, errGuestStopped) {
		t.Errorf("dialAgentRetry(%q) error = %v, want a plain dial error (the guest is still running)", path, err)
	}
	if elapsed < timeout {
		t.Errorf("dialAgentRetry(%q) gave up after %v, want it to keep polling for at least %v", path, elapsed, timeout)
	}
}

// A canceled context ends the poll with the context's error, so a caller that
// gives up on the restore is not left waiting out the timeout.
func TestDialAgentRetryContextCanceled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "clh.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("failed to create test socket: %v", err)
	}
	l.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close test listener: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := dialAgentRetry(ctx, path, time.Minute); !errors.Is(err, context.Canceled) {
		t.Errorf("dialAgentRetry(%q) error = %v, want context.Canceled", path, err)
	}
}

// resolveGuestMemMiB must honor a declared limit (minus the VMM reserve), fall back
// to the kata default only when the limit is unset, and error — never silently boot
// bigger than declared — when the reserve leaves too little to boot a guest.
func TestResolveGuestMemMiB(t *testing.T) {
	const (
		mib      = 1024 * 1024
		reserve  = 256  // vmmMemReserveMiB
		fallback = 2048 // kata-config default
	)
	tests := []struct {
		name        string
		declaredMiB int64 // 0 => unset
		wantMiB     int
		wantErr     bool
	}{
		{name: "unset falls back to kata default", declaredMiB: 0, wantMiB: fallback},
		{name: "declared honored minus reserve", declaredMiB: 1536, wantMiB: 1536 - reserve},
		{name: "just above minimum", declaredMiB: reserve + minGuestMemMiB, wantMiB: minGuestMemMiB},
		{name: "reserve exactly swallows limit", declaredMiB: reserve, wantErr: true},
		{name: "limit below reserve", declaredMiB: 128, wantErr: true},
		{name: "boot-hang band (too little guest RAM)", declaredMiB: 320, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveGuestMemMiB(tc.declaredMiB*mib, reserve, fallback)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveGuestMemMiB(%dMiB) = %d, nil; want an error", tc.declaredMiB, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveGuestMemMiB(%dMiB) unexpected error: %v", tc.declaredMiB, err)
			}
			if got != tc.wantMiB {
				t.Errorf("resolveGuestMemMiB(%dMiB) = %d, want %d", tc.declaredMiB, got, tc.wantMiB)
			}
		})
	}
}

func TestInitParams(t *testing.T) {
	// The agent path must be the one the kata guest image actually ships, since the
	// kernel silently panics on an init= that does not exist.
	if got := initParams(true); got != "init=/usr/bin/kata-agent" {
		t.Errorf("initParams(true) = %q", got)
	}
	// Without the agent as PID 1, systemd needs kata's target — it powers the guest
	// off within seconds otherwise — and networkd must stay masked, the agent owns eth0.
	systemd := initParams(false)
	for _, want := range []string{
		"systemd.unit=kata-containers.target",
		"systemd.mask=systemd-networkd.service",
		"systemd.mask=systemd-networkd.socket",
	} {
		if !strings.Contains(systemd, want) {
			t.Errorf("initParams(false) = %q, missing %q", systemd, want)
		}
	}
	if strings.Contains(systemd, "init=") {
		t.Errorf("initParams(false) = %q, must not override init", systemd)
	}
}

// The guest must log over virtio-console, not the emulated UART: the UART traps to
// the VMM per byte, which costs ~800ms of cold boot on the kata guest's boot log.
// Debug mode adds the UART back (with earlycon) to capture the early messages hvc0
// is too late to see.
func TestBuildVMConfigConsole(t *testing.T) {
	const id = "actor-1"
	consoleLog := kata.ConsoleLogPath(id)

	cfg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, false)
	if cfg.Console == nil || cfg.Console.Mode != "File" || cfg.Console.File != consoleLog {
		t.Errorf("Console = %+v, want File %q", cfg.Console, consoleLog)
	}
	if cfg.Serial == nil || cfg.Serial.Mode != "Off" {
		t.Errorf("Serial = %+v, want mode Off", cfg.Serial)
	}
	if !strings.Contains(cfg.Payload.Cmdline, "console=hvc0") {
		t.Errorf("cmdline = %q, want console=hvc0", cfg.Payload.Cmdline)
	}
	if strings.Contains(cfg.Payload.Cmdline, "earlycon") {
		t.Errorf("cmdline = %q, must not pay for earlycon outside debug mode", cfg.Payload.Cmdline)
	}

	dbg := buildVMConfig(id, "/vmlinux", "/rootfs.img", "", consoleLog, 256, 1, true, true)
	if dbg.Serial == nil || dbg.Serial.Mode != "File" || dbg.Serial.File != kata.SerialLogPath(id) {
		t.Errorf("debug Serial = %+v, want File %q", dbg.Serial, kata.SerialLogPath(id))
	}
	if !strings.Contains(dbg.Payload.Cmdline, "earlycon=") {
		t.Errorf("debug cmdline = %q, want an earlycon", dbg.Payload.Cmdline)
	}
}

// The SIGTERM path signals these ids over ttrpc, and the agent rejects an id it
// does not know with InvalidContainerId — which aborts the whole graceful
// shutdown, so a stale id here silently costs the guest its clean exit.
func TestWorkloadIDs(t *testing.T) {
	ctrs := []actorContainer{{name: "counter"}, {name: "sidecar"}}
	got := workloadIDs(ctrs)
	if want := []string{"counter", "sidecar"}; !slices.Equal(got, want) {
		t.Errorf("workloadIDs() = %v, want %v", got, want)
	}
}

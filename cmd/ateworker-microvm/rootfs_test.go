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
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
)

type guestCheck struct {
	name string
	cmd  string
}

// guestChecks lists the in-guest checks executed in kata-agent's debug console:
// the agent binary itself, a read-only ext4 root mount, no build-time hostname
// leaked into the image, the diagnostic tools DebugConsoleDump runs (ip, ps,
// ls, head, grep), and an intact dpkg database for vulnerability scanners.
var guestChecks = []guestCheck{
	{name: "agent", cmd: "kata-agent --version"},
	{name: "root-ro", cmd: `grep " / " /proc/mounts && grep -Eq "^[^ ]+ / ext4 ro[ ,]" /proc/mounts`},
	{name: "mount", cmd: `mount | grep -E " on / type ext4 \(ro[,)]"`},
	{name: "hostname", cmd: `cat /proc/sys/kernel/hostname && test "$(cat /proc/sys/kernel/hostname)" = localhost`},
	{name: "ip", cmd: "ip addr && ip route && ip neigh"},
	{name: "ps", cmd: `ps -N --ppid 2 -p 2 -o pid,comm && test "$(ps -o comm= -p 1)" = kata-agent`},
	{name: "coreutils", cmd: `ls -la / | head -n 4 && head -n 2 /etc/os-release && grep -c . /proc/mounts`},
	{name: "dpkg", cmd: `n="$(dpkg-query -W | wc -l)" && echo "${n} packages" && test "${n}" -gt 0 && test -z "$(dpkg --audit)"`},
}

const rootfsDockerfileRelPath = "../../hack/microvm-assets/rootfs/Dockerfile"

func TestParseDockerfileDebianImage(t *testing.T) {
	img, err := parseDockerfileArg(rootfsDockerfileRelPath, "DEBIAN_IMAGE")
	if err != nil {
		t.Fatalf("parseDockerfileArg(DEBIAN_IMAGE) = %v", err)
	}
	// TestGuestRootfsImage installs QEMU in this image with apt-get.
	if !strings.HasPrefix(img, "debian:") || !strings.Contains(img, "@sha256:") {
		t.Errorf("DEBIAN_IMAGE = %q, want a digest-pinned Debian image (debian:<tag>@sha256:<digest>)", img)
	}
}

func TestWrapLines(t *testing.T) {
	for _, tc := range []struct {
		in    string
		width int
		want  string
	}{
		{in: "", width: 4, want: ""},
		{in: "abcd", width: 4, want: "abcd"},
		{in: "abcdefgh", width: 4, want: "abcd\nefgh"},
		{in: "abcdefghi", width: 4, want: "abcd\nefgh\ni"},
	} {
		if got := wrapLines(tc.in, tc.width); got != tc.want {
			t.Errorf("wrapLines(%q, %d) = %q, want %q", tc.in, tc.width, got, tc.want)
		}
	}
}

// TestGuestRootfsImage boots a guest rootfs image under QEMU (in a container,
// using KVM when available and TCG otherwise) with the kernel command line
// buildVMConfig constructs, then runs guestChecks in kata-agent's serial debug
// console.
//
// Skipped during ordinary unit test runs unless ATEWORKER_TEST_ROOTFS_IMG and
// ATEWORKER_TEST_KERNEL are set (e.g. via hack/microvm-assets/test-rootfs.sh).
func TestGuestRootfsImage(t *testing.T) {
	imgEnv := os.Getenv("ATEWORKER_TEST_ROOTFS_IMG")
	kernelEnv := os.Getenv("ATEWORKER_TEST_KERNEL")
	if imgEnv == "" && kernelEnv == "" {
		t.Skip("set ATEWORKER_TEST_ROOTFS_IMG and ATEWORKER_TEST_KERNEL to boot-test a guest rootfs image")
	}
	if imgEnv == "" || kernelEnv == "" {
		t.Fatalf("both ATEWORKER_TEST_ROOTFS_IMG (%q) and ATEWORKER_TEST_KERNEL (%q) must be set", imgEnv, kernelEnv)
	}

	imgPath, err := filepath.Abs(imgEnv)
	if err != nil {
		t.Fatalf("resolve image path: %v", err)
	}
	kernelPath, err := filepath.Abs(kernelEnv)
	if err != nil {
		t.Fatalf("resolve kernel path: %v", err)
	}

	imgInfo, err := os.Stat(imgPath)
	if err != nil {
		t.Fatalf("stat rootfs image: %v", err)
	}
	const pmemAlign = 2 * 1024 * 1024
	if imgInfo.Size() == 0 || imgInfo.Size()%pmemAlign != 0 {
		t.Fatalf("rootfs.img size %d bytes is not a non-zero multiple of 2 MiB (%d)", imgInfo.Size(), pmemAlign)
	}
	if _, err := os.Stat(kernelPath); err != nil {
		t.Fatalf("stat kernel: %v", err)
	}

	arch := os.Getenv("ARCH")
	if arch == "" {
		arch = runtime.GOARCH
	}
	var qemuPkg, qemuBin, qemuMachine, serialConsole string
	switch arch {
	case "amd64":
		qemuPkg = "qemu-system-x86"
		qemuBin = "qemu-system-x86_64"
		qemuMachine = "q35"
		serialConsole = "ttyS0"
	case "arm64":
		qemuPkg = "qemu-system-arm"
		qemuBin = "qemu-system-aarch64"
		qemuMachine = "virt"
		serialConsole = "ttyAMA0"
	default:
		t.Fatalf("unsupported ARCH=%q (want amd64 or arm64)", arch)
	}

	qemuImage := os.Getenv("QEMU_IMAGE")
	if qemuImage == "" {
		qemuImage, err = parseDockerfileArg(rootfsDockerfileRelPath, "DEBIAN_IMAGE")
		if err != nil {
			t.Fatalf("resolve QEMU_IMAGE from Dockerfile: %v", err)
		}
	}

	containerCLI := os.Getenv("CONTAINER_CLI")
	if containerCLI == "" {
		containerCLI = "docker"
	}

	bootTimeout := 300 * time.Second
	if s := os.Getenv("BOOT_TIMEOUT"); s != "" {
		sec, err := strconv.Atoi(s)
		if err != nil || sec <= 0 {
			t.Fatalf("invalid BOOT_TIMEOUT=%q", s)
		}
		bootTimeout = time.Duration(sec) * time.Second
	}
	const checkTimeout = 120 * time.Second

	// Use the kernel command line ateworker constructs with --guest-debug, the only mode
	// that enables kata-agent's debug console, except drop
	// agent.debug_console_vport=1026 so kata-agent attaches its debug shell to
	// /dev/console (which the trailing console= directs to the serial port)
	// without needing a host vhost-vsock device. buildVMConfig's debug flag stays
	// off: it only adds earlycon for the VMM's UART, which the console= covers.
	const vportParam = " agent.debug_console_vport=1026"
	vmCfg := buildVMConfig("rootfs-test", "/vmlinux", "/rootfs.img", kata.WithAgentDebug(kata.BaseKernelParams), "/dev/null", 512, 2, true, false)
	if !strings.Contains(vmCfg.Payload.Cmdline, vportParam) {
		t.Fatalf("the --guest-debug kernel command line no longer sets %q; update how this test moves the debug console to the serial port: %q",
			strings.TrimSpace(vportParam), vmCfg.Payload.Cmdline)
	}
	cmdline := strings.ReplaceAll(vmCfg.Payload.Cmdline, vportParam, "")
	// ateworker boots root=/dev/pmem0p1 with DAX over virtio-pmem. If buildVMConfig
	// moves the root elsewhere, fail here rather than test a different boot path.
	if !strings.Contains(" "+cmdline+" ", " root=/dev/pmem0p1 ") {
		t.Fatalf("buildVMConfig no longer boots from root=/dev/pmem0p1; update the QEMU disk device in this test to match: %q", cmdline)
	}
	var rootDiskArgs string
	switch arch {
	case "amd64":
		rootDiskArgs = fmt.Sprintf(
			"-m 512M,maxmem=2G -object memory-backend-file,id=pmem0,mem-path=/rootfs.img,size=%d,readonly=on -device virtio-pmem-pci,memdev=pmem0",
			imgInfo.Size(),
		)
	case "arm64":
		// Debian's qemu-system-aarch64 is built without virtio-pmem-pci, so attach the
		// image as virtio-blk on arm64.
		cmdline = strings.ReplaceAll(cmdline, "root=/dev/pmem0p1", "root=/dev/vda1")
		cmdline = strings.ReplaceAll(cmdline, "rootflags=dax,", "rootflags=")
		rootDiskArgs = "-m 512M -drive file=/rootfs.img,if=none,id=root,format=raw,readonly=on -device virtio-blk-pci,drive=root"
	}
	cmdline += " console=" + serialConsole

	// QEMU runs natively in the container, so a guest of another arch boots under
	// TCG. The container is created before it starts so that it can always be
	// removed: ending the CLI attached to it does not stop it, and QEMU never
	// exits on its own.
	containerName := fmt.Sprintf("ateworker-rootfs-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	accel := "tcg"
	createArgs := []string{"create", "--rm", "-i", "--name", containerName}
	if arch == runtime.GOARCH && unix.Access("/dev/kvm", unix.R_OK|unix.W_OK) == nil {
		accel = "kvm"
		createArgs = append(createArgs, "--device", "/dev/kvm")
	}
	createArgs = append(createArgs,
		"-v", imgPath+":/rootfs.img:ro",
		"-v", kernelPath+":/vmlinux:ro",
		qemuImage,
		"sh", "-c",
		fmt.Sprintf(
			"apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends %s >/dev/null && "+
				"exec %s -nodefaults -no-user-config -machine %s,accel=%s -cpu max -smp 2 %s "+
				"-display none -monitor none -no-reboot -kernel /vmlinux -append %s "+
				"-object rng-random,id=rng,filename=/dev/urandom -device virtio-rng-pci,rng=rng -serial stdio",
			qemuPkg, qemuBin, qemuMachine, accel, rootDiskArgs, shellQuote(cmdline),
		),
	)

	ctx, cancel := context.WithTimeout(t.Context(), bootTimeout+checkTimeout)
	defer cancel()

	if out, err := exec.CommandContext(ctx, containerCLI, createArgs...).CombinedOutput(); err != nil {
		t.Fatalf("create container: %v\n%s", err, out)
	}
	var removeOnce sync.Once
	removeContainer := func() {
		removeOnce.Do(func() {
			rmCtx, rmCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer rmCancel()
			out, err := exec.CommandContext(rmCtx, containerCLI, "rm", "-f", containerName).CombinedOutput()
			// --rm already removed it if QEMU exited on its own.
			if err != nil && !strings.Contains(strings.ToLower(string(out)), "no such container") {
				t.Logf("remove container %s: %v\n%s", containerName, err, out)
			}
		})
	}
	defer removeContainer()

	t.Logf("Booting %s (%s, accel=%s)...", filepath.Base(imgPath), arch, accel)
	bootStart := time.Now()

	cmd := exec.CommandContext(ctx, containerCLI, "start", "-a", "-i", containerName)
	// If the context ends first, let the CLI exit and flush its output before it
	// is killed.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 10 * time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start container: %v", err)
	}
	// stopContainer removes the container, which ends QEMU and with it the CLI,
	// and waits for the CLI. Only then is stderr complete and safe to read.
	var stopOnce sync.Once
	stopContainer := func() {
		stopOnce.Do(func() {
			removeContainer()
			_ = stdin.Close()
			cancel()
			_ = cmd.Wait()
		})
	}
	defer stopContainer()

	linesCh := make(chan string, 256)
	scanErrCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			linesCh <- strings.TrimRight(scanner.Text(), "\r")
		}
		scanErrCh <- scanner.Err()
		close(linesCh)
	}()

	var logLines []string
	waitForLine := func(substr string, timeout time.Duration) error {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-linesCh:
				if !ok {
					return fmt.Errorf("guest console closed before printing %q (scan err: %v)", substr, <-scanErrCh)
				}
				logLines = append(logLines, line)
				if strings.Contains(line, substr) {
					return nil
				}
			case <-timer.C:
				return fmt.Errorf("timed out after %v waiting for %q", timeout, substr)
			}
		}
	}

	dumpFailureLogs := func(msg string) {
		t.Helper()
		stopContainer()
		start := 0
		if len(logLines) > 80 {
			start = len(logLines) - 80
		}
		t.Fatalf("%s\n--- serial console (last %d lines) ---\n%s\n--- container stderr ---\n%s",
			msg, len(logLines)-start, strings.Join(logLines[start:], "\n"), stderr.String())
	}

	if err := waitForLine("ttRPC server started", bootTimeout); err != nil {
		dumpFailureLogs(err.Error())
	}
	t.Logf("kata-agent started its ttRPC server in %v", time.Since(bootStart).Round(100*time.Millisecond))

	script := buildGuestCheckScript(guestChecks)
	// Wrap at 76 columns so no line approaches the guest serial tty's 4095-byte
	// canonical line-discipline limit.
	encoded := wrapLines(base64.StdEncoding.EncodeToString([]byte(script)), 76)
	payload := fmt.Sprintf("base64 -d <<'CHECKS' | bash 2>&1\n%s\nCHECKS\n", encoded)
	if _, err := io.WriteString(stdin, payload); err != nil {
		dumpFailureLogs(fmt.Sprintf("write guest check script to serial console: %v", err))
	}

	if err := waitForLine("== guest checks done", checkTimeout); err != nil {
		dumpFailureLogs(err.Error())
	}

	results := extractGuestCheckResults(logLines)
	t.Logf("Guest check output:\n%s", strings.Join(results, "\n"))

	for _, gc := range guestChecks {
		t.Run(gc.name, func(t *testing.T) {
			want := "OK:" + gc.name
			for _, line := range results {
				if line == want {
					return
				}
			}
			t.Errorf("check %q (%s) did not report %s", gc.name, gc.cmd, want)
		})
	}
}

func buildGuestCheckScript(checks []guestCheck) string {
	var b strings.Builder
	b.WriteString("check() {\n  echo\n  echo \"== $1: $2\"\n  if eval \"$2\"; then echo \"OK:$1\"; else echo \"FAIL:$1\"; fi\n}\n")
	b.WriteString("echo \"== guest checks\"\n")
	for _, c := range checks {
		fmt.Fprintf(&b, "check %s %s\n", c.name, shellQuote(c.cmd))
	}
	b.WriteString("echo \"== guest checks done\"\n")
	return b.String()
}

func wrapLines(s string, width int) string {
	var b strings.Builder
	for len(s) > width {
		b.WriteString(s[:width])
		b.WriteByte('\n')
		s = s[width:]
	}
	b.WriteString(s)
	return b.String()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func extractGuestCheckResults(lines []string) []string {
	var inBlock bool
	var out []string
	for _, line := range lines {
		if line == "== guest checks" {
			inBlock = true
		}
		if inBlock {
			out = append(out, line)
		}
		if line == "== guest checks done" && inBlock {
			break
		}
	}
	return out
}

func parseDockerfileArg(path, argName string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	prefix := "ARG " + argName + "="
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if val, ok := strings.CutPrefix(line, prefix); ok && val != "" {
			return val, nil
		}
	}
	return "", fmt.Errorf("%s not found in %s", prefix, path)
}

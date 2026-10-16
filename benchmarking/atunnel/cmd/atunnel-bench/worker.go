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

//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/fakeatelet"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/workerhost"
	"go.ate.dev/substrate/internal/nodepath"
)

const cgroupRoot = "/sys/fs/cgroup"

// workerCommand plays the rest of a worker pod around atunnel, in a
// privileged container: it serves the fake atelet on the node-local socket,
// starts atunnel (the tunnel subcommand) in a cgroup of its own with the CPU
// and memory limits, places an ingress actor in every actor's sandbox and the
// egress actor in the first, and replaces atunnel if it dies, logging why.
// Arguments after -- go to tunnel.
func workerCommand() *cobra.Command {
	var pkiDir, memory, shared, admin string
	var cpus float64
	var bodyBytes int
	cmd := &cobra.Command{
		Use:   "worker [flags] -- [tunnel flags]",
		Short: "Run the worker pod around atunnel: fake atelet, atunnel in its own cgroup, and the actors",
		RunE: func(cmd *cobra.Command, tunnelArgs []string) error {
			memBytes, err := parseBytes(memory)
			if err != nil {
				return fmt.Errorf("--tunnel-memory: %w", err)
			}
			self, err := os.Executable()
			if err != nil {
				return err
			}
			s := &supervisor{self: self, tunnelArgs: tunnelArgs, admin: admin, shared: shared, bodyBytes: bodyBytes, cpus: cpus, memBytes: memBytes}
			return s.run(cmd.Context(), pkiDir)
		},
	}
	f := cmd.Flags()
	f.StringVar(&pkiDir, "pki-dir", "/pki", "directory written by atunnel-bench pki")
	f.Float64Var(&cpus, "tunnel-cpus", 1, "CPU limit for atunnel, in cores; 0 for none")
	f.StringVar(&memory, "tunnel-memory", "1g", "memory limit for atunnel, with no swap, e.g. 512m or 1g; empty for none")
	f.StringVar(&shared, "shared-dir", "/shared", "directory shared with the load generator: the actors' sockets and the death log")
	f.StringVar(&admin, "tunnel-admin", "http://127.0.0.1:9090", "atunnel's admin base URL")
	f.IntVar(&bodyBytes, "body-bytes", 16, "response body size of the ingress actors")
	return cmd
}

// run serves the fake atelet and keeps an atunnel running until ctx ends.
func (s *supervisor) run(ctx context.Context, pkiDir string) error {
	atelet, err := fakeatelet.New(pkiDir)
	if err != nil {
		return err
	}
	go func() {
		if err := atelet.Serve(ctx, pkiDir, nodepath.WorkerSupportSocket); err != nil {
			slog.Error("fake atelet stopped", slog.Any("err", err))
		}
	}()
	if err := delegateCgroups(); err != nil {
		return fmt.Errorf("setting up cgroups: %w", err)
	}
	for incarnation := 1; ctx.Err() == nil; incarnation++ {
		if err := s.runOnce(ctx, incarnation); err != nil {
			return err
		}
	}
	return nil
}

type supervisor struct {
	self       string
	tunnelArgs []string
	admin      string
	shared     string
	bodyBytes  int
	cpus       float64
	memBytes   int64
}

// runOnce runs one atunnel, with its sandbox processes, until it exits. It
// returns nil when atunnel died and should be replaced.
func (s *supervisor) runOnce(ctx context.Context, incarnation int) error {
	dir := filepath.Join(cgroupRoot, "tunnel-"+strconv.Itoa(incarnation))
	fd, err := s.createCgroup(dir)
	if err != nil {
		return err
	}
	cmd := exec.Command(s.self, append([]string{"tunnel"}, s.tunnelArgs...)...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	// Started inside its cgroup, so nothing it does is counted elsewhere.
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: fd}
	err = cmd.Start()
	_ = syscall.Close(fd)
	if err != nil {
		return fmt.Errorf("starting atunnel: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	stopBench := context.AfterFunc(ctx, func() { _ = cmd.Process.Signal(syscall.SIGTERM) })
	defer stopBench()

	var helpers []*exec.Cmd
	defer func() {
		for _, h := range helpers {
			_ = h.Process.Kill()
			_ = h.Wait()
		}
	}()
	info, err := s.waitForSandboxes(ctx, exited)
	if err == nil {
		helpers, err = s.startSandboxProcesses(info)
	}
	var exitErr error
	if err != nil {
		// atunnel may have failed to start; its exit says why.
		_ = cmd.Process.Kill()
		exitErr = <-exited
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errExited) {
			s.logDeath(dir, exitErr)
			return removeCgroup(dir)
		}
		return err
	}
	exitErr = <-exited
	if ctx.Err() != nil {
		return nil
	}
	s.logDeath(dir, exitErr)
	return removeCgroup(dir)
}

var errExited = errors.New("atunnel exited")

// waitForSandboxes polls atunnel's /stats until it hosts its actors.
func (s *supervisor) waitForSandboxes(ctx context.Context, exited <-chan error) (workerhost.Info, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	for {
		var body struct {
			Tunnel workerhost.Info `json:"tunnel"`
		}
		resp, err := client.Get(s.admin + "/stats")
		if err == nil {
			err = json.NewDecoder(resp.Body).Decode(&body)
			_ = resp.Body.Close()
			if err == nil && len(body.Tunnel.Sandboxes) > 0 {
				return body.Tunnel, nil
			}
		}
		select {
		case <-ctx.Done():
			return workerhost.Info{}, ctx.Err()
		case err := <-exited:
			return workerhost.Info{}, fmt.Errorf("%w during startup: %v", errExited, err)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// startSandboxProcesses places an ingress actor in every sandbox and the egress
// actor in the first.
func (s *supervisor) startSandboxProcesses(info workerhost.Info) ([]*exec.Cmd, error) {
	var cmds []*exec.Cmd
	start := func(netns string, args ...string) error {
		cmd := exec.Command(s.self, append([]string{"netns-exec", netns, s.self}, args...)...)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		cmds = append(cmds, cmd)
		return nil
	}
	for i, sb := range info.Sandboxes {
		socket := filepath.Join(s.shared, "ingress-actor-"+strconv.Itoa(i)+".sock")
		if err := start(sb.NetNS, "ingress-actor", "--port", strconv.Itoa(info.ActorPort), "--body-bytes", strconv.Itoa(s.bodyBytes), "--stats-socket", socket); err != nil {
			return cmds, err
		}
	}
	if err := start(info.Sandboxes[0].NetNS, "egress-actor", "--socket", filepath.Join(s.shared, "egress-actor.sock")); err != nil {
		return cmds, err
	}
	fmt.Fprintf(os.Stderr, "worker: %d sandboxes with ingress actors, egress actor in %s\n", len(info.Sandboxes), info.Sandboxes[0].Actor)
	return cmds, nil
}

// logDeath records why atunnel stopped, with the OOM kills its cgroup
// counted, in the shared death log.
func (s *supervisor) logDeath(dir string, exitErr error) {
	ooms := oomKills(dir)
	line := fmt.Sprintf("%s atunnel exited (%v); OOM kills: %d\n", time.Now().Format(time.TimeOnly), exitErr, ooms)
	fmt.Fprint(os.Stderr, "worker: "+line)
	f, err := os.OpenFile(filepath.Join(s.shared, "tunnel-deaths.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.WriteString(line)
}

// delegateCgroups moves every process out of the container's root cgroup, so
// its controllers can be handed to child cgroups (cgroup v2 allows processes
// only in leaves), and enables the CPU and memory controllers for them.
func delegateCgroups() error {
	leaf := filepath.Join(cgroupRoot, "supervisor")
	if err := os.MkdirAll(leaf, 0o755); err != nil {
		return err
	}
	procs, err := os.ReadFile(filepath.Join(cgroupRoot, "cgroup.procs"))
	if err != nil {
		return err
	}
	for _, pid := range strings.Fields(string(procs)) {
		_ = os.WriteFile(filepath.Join(leaf, "cgroup.procs"), []byte(pid), 0o644)
	}
	return os.WriteFile(filepath.Join(cgroupRoot, "cgroup.subtree_control"), []byte("+cpu +memory"), 0o644)
}

// createCgroup makes a cgroup with atunnel's limits and returns a descriptor to
// start a process in it.
func (s *supervisor) createCgroup(dir string) (int, error) {
	if err := os.Mkdir(dir, 0o755); err != nil && !os.IsExist(err) {
		return -1, err
	}
	if s.cpus > 0 {
		const period = 100000
		if err := os.WriteFile(filepath.Join(dir, "cpu.max"), []byte(fmt.Sprintf("%d %d", int(s.cpus*period), period)), 0o644); err != nil {
			return -1, err
		}
	}
	if s.memBytes > 0 {
		if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte(strconv.FormatInt(s.memBytes, 10)), 0o644); err != nil {
			return -1, err
		}
		// No swap, as in a Kubernetes pod: over the limit is an OOM kill.
		if err := os.WriteFile(filepath.Join(dir, "memory.swap.max"), []byte("0"), 0o644); err != nil && !os.IsNotExist(err) {
			return -1, err
		}
	}
	return syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
}

func oomKills(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, "memory.events"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "oom_kill "); ok {
			n, _ := strconv.Atoi(v)
			return n
		}
	}
	return 0
}

// removeCgroup removes an emptied cgroup, waiting briefly for the kernel to
// finish with the dead process.
func removeCgroup(dir string) error {
	var err error
	for range 50 {
		if err = os.Remove(dir); err == nil || os.IsNotExist(err) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("removing %s: %w", dir, err)
}

func parseBytes(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	switch strings.ToLower(s[len(s)-1:]) {
	case "k":
		mult = 1 << 10
	case "m":
		mult = 1 << 20
	case "g":
		mult = 1 << 30
	}
	if mult > 1 {
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n * mult, nil
}

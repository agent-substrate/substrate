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

// Package kata drives a single containerd-shim-kata-v2 instance over ttrpc
// WITHOUT a containerd daemon.
//
// The shim uses containerd's standard runtime-v2 framework: invoking it with
// the "start" action (cwd = the OCI bundle) brings up a per-task ttrpc server
// and prints its socket address on stdout. We then speak the task v2 API
// (Create/Start/Pause/Resume/Kill/Delete/...) directly against that socket.
//
// This is the RUN half of ateom-microvm: kata boots the cloud-
// hypervisor micro-VM (guest kernel + rootfs + kata-agent) and runs the OCI
// container; ateom drives CH's snapshot/restore underneath (see internal/ch),
// against the api-socket kata exposes at CLHSocketPath(id).
//
// Empirically verified against kata 3.31.0 (containerd-shim-kata-v2): the
// "start" action returns "unix:///run/containerd/s/<hash>" and works with no
// containerd running.
package kata

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	task "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/ttrpc"
	"golang.org/x/sys/unix"
)

const (
	// DefaultShimBinary is the kata containerd shim v2 executable.
	DefaultShimBinary = "containerd-shim-kata-v2"

	// RuntimeID is the kata shim's containerd runtime identifier.
	RuntimeID = "io.containerd.kata.v2"

	// vcVMDir is where kata creates per-sandbox runtime state, including the
	// cloud-hypervisor API socket.
	vcVMDir = "/run/vc/vm"
)

// CLHSocketPath returns the cloud-hypervisor API socket kata creates for the
// sandbox with the given id. ateom drives CH snapshot/restore against it.
func CLHSocketPath(id string) string {
	return filepath.Join(vcVMDir, id, "clh-api.sock")
}

// Shim drives one containerd-shim-kata-v2 over ttrpc.
type Shim struct {
	// Binary is the shim executable; defaults to DefaultShimBinary on PATH.
	Binary string
	// ID is the container/sandbox id. It also names the per-sandbox runtime
	// dir, so CLHSocketPath(ID) locates the CH api-socket.
	ID string
	// Bundle is the OCI bundle directory (contains config.json and rootfs/).
	Bundle string
	// Namespace is the containerd namespace the shim runs under (e.g. "default").
	Namespace string
	// GRPCAddress is the containerd grpc address the shim records. We run no
	// containerd, but the shim uses it (with Namespace + ID) to derive its
	// deterministic ttrpc socket path, so it must be a stable unique path.
	GRPCAddress string
	// TTRPCAddress is the events-publish target. With no containerd to receive
	// them, kata logs publish failures but continues; point it anywhere stable.
	TTRPCAddress string
	// Diagnostics receives the start/delete action's stdout/stderr if set.
	Diagnostics io.Writer
	// Debug enables the shim's -debug flag (verbose logs to the bundle "log" fifo).
	Debug bool
	// ConfigFile, if set, is passed to the shim as KATA_CONF_FILE so it uses a
	// generated configuration.toml (pointing at runtime-fetched assets) instead
	// of the well-known /etc/kata-containers path.
	ConfigFile string

	addr   string
	conn   net.Conn
	client *ttrpc.Client
	task   task.TTRPCTaskService
	cmd    *exec.Cmd // the foreground shim server process we manage
}

func (s *Shim) binary() string {
	if s.Binary != "" {
		return s.Binary
	}
	return DefaultShimBinary
}

func (s *Shim) shimEnv() []string {
	env := append(os.Environ(),
		"TTRPC_ADDRESS="+s.TTRPCAddress,
		"GRPC_ADDRESS="+s.GRPCAddress,
		"NAMESPACE="+s.Namespace,
	)
	if s.ConfigFile != "" {
		env = append(env, "KATA_CONF_FILE="+s.ConfigFile)
	}
	return env
}

// Bootstrap launches the shim's ttrpc server in the FOREGROUND (on an explicit
// -socket) and connects to it. After Bootstrap the shim is up but no VM exists
// yet; call Create to boot the micro-VM and create the container.
//
// We deliberately do NOT use the containerd "start" action: it double-forks a
// detached daemon that dies silently in a minimal pod (PID 1 = ateom, no
// containerd). Running the server in the foreground as a child we manage is the
// path that works in-pod, and lets ateom own its lifecycle.
func (s *Shim) Bootstrap(ctx context.Context) error {
	if s.ID == "" || s.Bundle == "" {
		return fmt.Errorf("kata.Shim requires ID and Bundle")
	}

	// The socket dir is containerd convention; create it (no containerd does).
	if err := os.MkdirAll(shimSocketDir, 0o711); err != nil {
		return fmt.Errorf("while creating shim socket dir %q: %w", shimSocketDir, err)
	}
	socketPath := filepath.Join(shimSocketDir, "ateomchv-"+s.ID+".sock")
	_ = os.Remove(socketPath)

	// The shim opens <bundle>/log (O_WRONLY) for its logrus output and blocks
	// until a reader appears; create the fifo and drain it, or the server hangs
	// during logging setup and never binds its socket.
	if err := s.startLogDrain(); err != nil {
		return err
	}

	args := []string{
		"-namespace", s.Namespace,
		"-address", s.GRPCAddress,
		"-id", s.ID,
	}
	if s.Debug {
		args = append(args, "-debug")
	}
	args = append(args, "-socket", socketPath)

	// Deliberately NOT exec.CommandContext: the shim must outlive the
	// RunWorkload RPC whose ctx spawned it (gRPC cancels the ctx when the
	// handler returns, and CommandContext would then SIGKILL the shim under a
	// healthy running actor). Lifetime is managed via s.cmd (Close kills it);
	// the dial loop below honors ctx for bootstrap cancellation.
	cmd := exec.Command(s.binary(), args...)
	cmd.Dir = s.Bundle
	cmd.Env = s.shimEnv()
	cmd.Stdout = s.Diagnostics
	cmd.Stderr = s.Diagnostics
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("while starting shim server: %w", err)
	}
	s.cmd = cmd
	s.addr = socketPath

	// Wait for the server to bind + connect.
	deadline := time.Now().Add(15 * time.Second)
	var conn net.Conn
	var err error
	for {
		conn, err = net.DialTimeout("unix", socketPath, 2*time.Second)
		if err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			_ = cmd.Process.Kill()
			return fmt.Errorf("while dialing shim ttrpc socket %q: %w", socketPath, err)
		}
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	s.conn = conn
	s.client = ttrpc.NewClient(conn)
	s.task = task.NewTTRPCTaskClient(s.client)
	return nil
}

// shimSocketDir is where the kata shim binds its ttrpc socket (containerd
// convention: /run/containerd/s/).
const shimSocketDir = "/run/containerd/s"

// startLogDrain creates the <bundle>/log fifo the shim writes its logs to and
// drains it (to Diagnostics, else discarded). The shim opens it O_WRONLY and
// blocks until a reader exists, so this must run before launching the server.
func (s *Shim) startLogDrain() error {
	logFifo := filepath.Join(s.Bundle, "log")
	_ = os.Remove(logFifo)
	if err := unix.Mkfifo(logFifo, 0o700); err != nil {
		return fmt.Errorf("while creating shim log fifo: %w", err)
	}
	go func() {
		f, err := os.OpenFile(logFifo, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		defer f.Close()
		dst := io.Writer(io.Discard)
		if s.Diagnostics != nil {
			dst = s.Diagnostics
		}
		_, _ = io.Copy(dst, f)
	}()
	return nil
}

// Address returns the shim's ttrpc socket path (after Bootstrap).
func (s *Shim) Address() string { return s.addr }

// CreateOptions configures the container/task creation.
type CreateOptions struct {
	// Rootfs are mounts the shim should set up at <bundle>/rootfs. Leave nil
	// when the bundle's rootfs/ is already populated (atelet's model).
	Rootfs []*types.Mount
	// Stdin/Stdout/Stderr are fifo or file paths for the container's stdio.
	// Empty discards.
	Stdin  string
	Stdout string
	Stderr string
	// Terminal requests a pty for the container's process.
	Terminal bool
}

// Create boots the micro-VM and creates the container from the bundle. Returns
// the task pid (the in-VMM process as seen by the shim).
func (s *Shim) Create(ctx context.Context, o CreateOptions) (uint32, error) {
	resp, err := s.task.Create(ctx, &task.CreateTaskRequest{
		ID:       s.ID,
		Bundle:   s.Bundle,
		Rootfs:   o.Rootfs,
		Terminal: o.Terminal,
		Stdin:    o.Stdin,
		Stdout:   o.Stdout,
		Stderr:   o.Stderr,
	})
	if err != nil {
		return 0, fmt.Errorf("while calling task.Create: %w", err)
	}
	return resp.GetPid(), nil
}

// Start starts the created container's init process.
func (s *Shim) Start(ctx context.Context) (uint32, error) {
	resp, err := s.task.Start(ctx, &task.StartRequest{ID: s.ID})
	if err != nil {
		return 0, fmt.Errorf("while calling task.Start: %w", err)
	}
	return resp.GetPid(), nil
}

// Pause pauses the container (quiesce before snapshot).
func (s *Shim) Pause(ctx context.Context) error {
	if _, err := s.task.Pause(ctx, &task.PauseRequest{ID: s.ID}); err != nil {
		return fmt.Errorf("while calling task.Pause: %w", err)
	}
	return nil
}

// Resume resumes a paused container.
func (s *Shim) Resume(ctx context.Context) error {
	if _, err := s.task.Resume(ctx, &task.ResumeRequest{ID: s.ID}); err != nil {
		return fmt.Errorf("while calling task.Resume: %w", err)
	}
	return nil
}

// Kill sends a signal to the container. If all is true, signals every process.
func (s *Shim) Kill(ctx context.Context, signal uint32, all bool) error {
	if _, err := s.task.Kill(ctx, &task.KillRequest{ID: s.ID, Signal: signal, All: all}); err != nil {
		return fmt.Errorf("while calling task.Kill: %w", err)
	}
	return nil
}

// Delete deletes the container/task (after it has stopped).
func (s *Shim) Delete(ctx context.Context) error {
	if _, err := s.task.Delete(ctx, &task.DeleteRequest{ID: s.ID}); err != nil {
		return fmt.Errorf("while calling task.Delete: %w", err)
	}
	return nil
}

// Shutdown stops the shim (and its VMM). Pass now=true to force.
func (s *Shim) Shutdown(ctx context.Context, now bool) error {
	if s.task == nil {
		return nil
	}
	if _, err := s.task.Shutdown(ctx, &task.ShutdownRequest{ID: s.ID, Now: now}); err != nil {
		return fmt.Errorf("while calling task.Shutdown: %w", err)
	}
	return nil
}

// Close closes the ttrpc connection and stops the foreground shim server we
// launched (which also tears down its VMM).
func (s *Shim) Close() error {
	var firstErr error
	if s.client != nil {
		if err := s.client.Close(); err != nil {
			firstErr = err
		}
	}
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_, _ = s.cmd.Process.Wait()
	}
	return firstErr
}

// CleanupAction runs the shim's "delete" action, which tears down any leftover
// sandbox state for ID. Best-effort; safe to call after Shutdown.
//
// After a successful Shutdown the per-sandbox state dir is already gone, so the
// "delete" action reports "/run/vc/sbs/<id>: no such file or directory" and
// exits non-zero; that case is treated as success (nothing left to clean).
func (s *Shim) CleanupAction(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, s.binary(),
		"-namespace", s.Namespace,
		"-address", s.GRPCAddress,
		"-id", s.ID,
		"delete",
	)
	cmd.Dir = s.Bundle
	cmd.Env = s.shimEnv()
	out, err := cmd.CombinedOutput()
	if s.Diagnostics != nil {
		fmt.Fprintf(s.Diagnostics, "shim delete: %q (err=%v)\n", string(out), err)
	}
	if err != nil {
		if strings.Contains(string(out), "no such file or directory") {
			return nil // already torn down
		}
		return fmt.Errorf("while running shim delete: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

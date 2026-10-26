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

// Package shim drives a Kata containerd shim v2 (containerd-shim-kata-v2) the
// way containerd does, over its ttrpc task API, with no containerd daemon.
//
// One shim serves one sandbox: the first container created on it boots the
// VM, and later containers join that VM. The hypervisor comes from the Kata
// configuration file named in Config.ConfigPath.
package shim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	taskv2 "github.com/containerd/containerd/api/runtime/task/v2"
	"github.com/containerd/containerd/api/types"
	options "github.com/containerd/containerd/api/types/runtimeoptions/v1"
	"github.com/containerd/ttrpc"
	"github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/anypb"

	"go.ate.dev/substrate/internal/pidfd"
)

const (
	// namespace names the shim's socket and state. It is not a containerd
	// namespace: no containerd runs.
	namespace = "substrate"

	// address and publishBinary fill shim flags that runtime-rs requires but
	// does not use here: it hashes the address into its socket name, and
	// with no TTRPC_ADDRESS in its environment it logs events instead of
	// running the publish binary.
	address       = "none"
	publishBinary = "/bin/true"

	// shimPIDFile is where the shim's start command records the pid of the
	// daemon it forks, in the bundle directory.
	shimPIDFile = "shim.pid"

	// processSpecTypeURL types the Process spec of an Exec request.
	processSpecTypeURL = "types.containerd.io/opencontainers/runtime-spec/1/Process"
)

// Config says which shim to run and how it is configured.
type Config struct {
	// Binary is the path of containerd-shim-kata-v2.
	Binary string
	// ConfigPath is the Kata configuration file. It selects the hypervisor.
	// The shim reads it from the create request's runtime options, since it
	// accepts a KATA_CONF_FILE only when that names a shipped file.
	ConfigPath string
}

// Shim is a running shim daemon and its ttrpc connection.
type Shim struct {
	pid     int
	exited  <-chan struct{}
	client  *ttrpc.Client
	task    taskv2.TTRPCTaskService
	options *anypb.Any
}

// Start runs the shim's start command in bundleDir, which holds the first
// container's config.json, and connects to the daemon it leaves running. id is
// that container's ID, which the shim also uses to name the sandbox.
//
// attr applies to the start command, so the daemon, the hypervisor and the
// helpers it spawns all run in whatever cgroup attr names. It may be nil.
func Start(ctx context.Context, cfg Config, id, bundleDir string, attr *syscall.SysProcAttr) (_ *Shim, retErr error) {
	if cfg.Binary == "" || cfg.ConfigPath == "" {
		return nil, errors.New("shim binary and config path are required")
	}
	opts, err := anypb.New(&options.Options{ConfigPath: cfg.ConfigPath})
	if err != nil {
		return nil, fmt.Errorf("encoding runtime options: %w", err)
	}

	cmd := exec.CommandContext(ctx, cfg.Binary,
		"-namespace", namespace,
		"-address", address,
		"-publish-binary", publishBinary,
		"-id", id,
		"start")
	cmd.Dir = bundleDir
	// Leave out TTRPC_ADDRESS, with which the shim would forward its events to
	// it, and KATA_CONF_FILE, which the shim reads before the runtime option.
	cmd.Env = withoutEnv(os.Environ(), "TTRPC_ADDRESS", "KATA_CONF_FILE")
	cmd.SysProcAttr = attr
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running the shim start command: %w%s", err, detail(stderr.Bytes()))
	}

	socket, ok := strings.CutPrefix(strings.TrimSpace(string(out)), "unix://")
	if !ok {
		return nil, fmt.Errorf("shim start printed %q, want a unix:// address", out)
	}
	pid, err := readPID(filepath.Join(bundleDir, shimPIDFile))
	if err != nil {
		return nil, err
	}
	exited, err := pidfd.Exited(pid)
	if err != nil {
		return nil, fmt.Errorf("watching the shim: %w", err)
	}
	s := &Shim{pid: pid, exited: exited, options: opts}
	defer func() {
		if retErr != nil {
			_ = s.Close()
		}
	}()

	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("connecting to the shim: %w", err)
	}
	s.client = ttrpc.NewClient(conn)
	s.task = taskv2.NewTTRPCTaskClient(s.client)
	return s, nil
}

// Mount is a rootfs mount handed to the shim. A container with none uses the
// rootfs directory its config.json names.
type Mount struct {
	Type   string
	Source string
}

// CreateRequest creates a container, and the VM too if it is the first.
type CreateRequest struct {
	// ID names the container.
	ID string
	// Bundle holds the container's config.json.
	Bundle string
	// Rootfs overrides the rootfs directory in config.json.
	Rootfs []Mount
	// Stdout and Stderr are FIFOs the shim writes to. The caller must hold
	// their read ends open first, or the shim cannot open them.
	Stdout, Stderr string
}

// Create creates a container and returns its init pid in the guest.
func (s *Shim) Create(ctx context.Context, req CreateRequest) (uint32, error) {
	mounts := make([]*types.Mount, 0, len(req.Rootfs))
	for _, m := range req.Rootfs {
		mounts = append(mounts, &types.Mount{Type: m.Type, Source: m.Source})
	}
	resp, err := s.task.Create(ctx, &taskv2.CreateTaskRequest{
		ID:      req.ID,
		Bundle:  req.Bundle,
		Rootfs:  mounts,
		Stdout:  req.Stdout,
		Stderr:  req.Stderr,
		Options: s.options,
	})
	if err != nil {
		return 0, fmt.Errorf("creating container %q: %w", req.ID, err)
	}
	return resp.GetPid(), nil
}

// StartContainer starts the init process of a created container.
func (s *Shim) StartContainer(ctx context.Context, id string) error {
	if _, err := s.task.Start(ctx, &taskv2.StartRequest{ID: id}); err != nil {
		return fmt.Errorf("starting container %q: %w", id, err)
	}
	return nil
}

// Wait blocks until the process exits and returns its exit status. An empty
// execID names the container's init process.
func (s *Shim) Wait(ctx context.Context, id, execID string) (uint32, error) {
	resp, err := s.task.Wait(ctx, &taskv2.WaitRequest{ID: id, ExecID: execID})
	if err != nil {
		return 0, fmt.Errorf("waiting for %q: %w", id, err)
	}
	return resp.GetExitStatus(), nil
}

// Kill signals the process. An empty execID names the container's init process.
func (s *Shim) Kill(ctx context.Context, id, execID string, signal syscall.Signal) error {
	_, err := s.task.Kill(ctx, &taskv2.KillRequest{ID: id, ExecID: execID, Signal: uint32(signal)})
	if err != nil {
		return fmt.Errorf("signaling %q with %d: %w", id, signal, err)
	}
	return nil
}

// Exec creates, but does not start, an extra process in a running container.
// Start it with StartExec.
func (s *Shim) Exec(ctx context.Context, id, execID string, process *specs.Process, stdout, stderr string) error {
	spec, err := json.Marshal(process)
	if err != nil {
		return fmt.Errorf("encoding the process spec: %w", err)
	}
	_, err = s.task.Exec(ctx, &taskv2.ExecProcessRequest{
		ID:     id,
		ExecID: execID,
		Stdout: stdout,
		Stderr: stderr,
		Spec:   &anypb.Any{TypeUrl: processSpecTypeURL, Value: spec},
	})
	if err != nil {
		return fmt.Errorf("creating process %q in %q: %w", execID, id, err)
	}
	return nil
}

// StartExec starts a process created with Exec.
func (s *Shim) StartExec(ctx context.Context, id, execID string) error {
	if _, err := s.task.Start(ctx, &taskv2.StartRequest{ID: id, ExecID: execID}); err != nil {
		return fmt.Errorf("starting process %q in %q: %w", execID, id, err)
	}
	return nil
}

// Delete removes a stopped container, or an exec process if execID is set.
func (s *Shim) Delete(ctx context.Context, id, execID string) error {
	if _, err := s.task.Delete(ctx, &taskv2.DeleteRequest{ID: id, ExecID: execID}); err != nil {
		return fmt.Errorf("deleting %q: %w", id, err)
	}
	return nil
}

// Shutdown tells the daemon to exit once its containers are deleted. now skips
// that wait.
func (s *Shim) Shutdown(ctx context.Context, id string, now bool) error {
	if _, err := s.task.Shutdown(ctx, &taskv2.ShutdownRequest{ID: id, Now: now}); err != nil {
		return fmt.Errorf("shutting down the shim: %w", err)
	}
	return nil
}

// Cleanup runs the shim's delete command for the sandbox id, which removes the
// daemon's socket and the sandbox state it left behind. Run it once the daemon
// has exited. It works from the id alone, so it also clears what a daemon of an
// earlier worker process left. bundleDir is the bundle the daemon started in.
func Cleanup(ctx context.Context, binary, id, bundleDir string) error {
	cmd := exec.CommandContext(ctx, binary,
		"-namespace", namespace,
		"-address", address,
		"-publish-binary", publishBinary,
		"-id", id,
		"-bundle", bundleDir,
		"delete")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("running the shim delete command: %w%s", err, detail(stderr.Bytes()))
	}
	return nil
}

// Close drops the ttrpc connection. It does not stop the daemon.
func (s *Shim) Close() error {
	if s.client == nil {
		return nil
	}
	return s.client.Close()
}

func detail(b []byte) string {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return ""
	}
	return ": " + string(b)
}

func readPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("reading the shim pid: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("shim pid file %s holds %q", path, b)
	}
	return pid, nil
}

func withoutEnv(env []string, names ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		if !slices.Contains(names, name) {
			out = append(out, kv)
		}
	}
	return out
}

// Exited is closed once the shim daemon has exited.
func (s *Shim) Exited() <-chan struct{} { return s.exited }

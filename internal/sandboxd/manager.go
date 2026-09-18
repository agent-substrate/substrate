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

// Package sandboxd implements the small subset of containerd's shim manager,
// sandbox controller and task manager required by ateom-kata.
package sandboxd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sandboxapi "github.com/agent-substrate/substrate/internal/sandboxd/sandboxapi"
	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/containerd/ttrpc"
)

const supportedShimVersion = 3

type bootstrapResult struct {
	Version  int    `json:"version"`
	Address  string `json:"address"`
	Protocol string `json:"protocol"`
}

// Instance is a running Kata shim and its services.
type Instance struct {
	ID       string
	Bundle   string
	PID      int
	Endpoint string

	client     *ttrpc.Client
	Sandbox    sandboxapi.TTRPCSandboxService
	Checkpoint sandboxapi.TTRPCCheckpointService
	Task       taskapi.TTRPCTaskService
}

// ShimManager starts and tracks Kata shim v2 processes.
type ShimManager struct {
	binary    string
	namespace string
	address   string
	root      string
	kataConf  string

	mu        sync.RWMutex
	instances map[string]*Instance
}

type ShimManagerConfig struct {
	Binary, Namespace, Address, Root, KataConfig string
}

func NewShimManager(cfg ShimManagerConfig) (*ShimManager, error) {
	if !filepath.IsAbs(cfg.Binary) || !filepath.IsAbs(cfg.Root) {
		return nil, errors.New("shim binary and root must be absolute paths")
	}
	if cfg.Namespace == "" {
		cfg.Namespace = "sandboxd"
	}
	if cfg.Address == "" {
		cfg.Address = "unix:///run/sandboxd/sandboxd.sock"
	}
	if err := os.MkdirAll(cfg.Root, 0o700); err != nil {
		return nil, fmt.Errorf("create shim root: %w", err)
	}
	return &ShimManager{binary: cfg.Binary, namespace: cfg.Namespace, address: cfg.Address, root: cfg.Root, kataConf: cfg.KataConfig, instances: map[string]*Instance{}}, nil
}

func validID(id string) bool {
	if id == "" || id == "." || id == ".." || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !(r == '-' || r == '_' || r == '.' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
			return false
		}
	}
	return true
}

func (m *ShimManager) bundle(id string) (string, error) {
	if !validID(id) {
		return "", fmt.Errorf("invalid sandbox id %q", id)
	}
	return filepath.Join(m.root, id), nil
}

// Start creates the shim bundle and executes the containerd shim start
// bootstrap protocol. Kata runtime-rs currently emits a JSON v3 result.
func (m *ShimManager) Start(ctx context.Context, id string) (_ *Instance, retErr error) {
	bundle, err := m.bundle(id)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	_, exists := m.instances[id]
	m.mu.RUnlock()
	if exists {
		return nil, fmt.Errorf("shim %q already exists", id)
	}
	if err := os.MkdirAll(bundle, 0o700); err != nil {
		return nil, fmt.Errorf("create shim bundle: %w", err)
	}
	if err := ensureSandboxSpec(bundle); err != nil {
		return nil, err
	}
	// A previous sandboxd crash may leave Kata's deterministic shim socket
	// behind. The shim delete bootstrap removes that endpoint before start.
	cleanup := exec.Command(m.binary,
		"-id", id, "-namespace", m.namespace, "-address", m.address,
		"-bundle", bundle, "-publish-binary", "/bin/true", "delete")
	cleanup.Dir = bundle
	cleanup.Env = os.Environ()
	if m.kataConf != "" {
		cleanup.Env = append(cleanup.Env, "KATA_CONF_FILE="+m.kataConf)
	}
	_, _ = cleanup.CombinedOutput()
	defer func() {
		if retErr != nil {
			_ = os.RemoveAll(bundle)
		}
	}()
	logFile, err := os.OpenFile(filepath.Join(bundle, "log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create shim log: %w", err)
	}
	_ = logFile.Close()

	cmd := exec.CommandContext(ctx, m.binary,
		"-id", id, "-namespace", m.namespace, "-address", m.address,
		"-bundle", bundle, "-publish-binary", "/bin/true", "start")
	cmd.Dir = bundle
	cmd.Env = os.Environ()
	if m.kataConf != "" {
		cmd.Env = append(cmd.Env, "KATA_CONF_FILE="+m.kataConf)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("start Kata shim: %w: %s", err, strings.TrimSpace(string(out)))
	}
	boot, err := parseBootstrap(out)
	if err != nil {
		return nil, err
	}
	pidBytes, err := os.ReadFile(filepath.Join(bundle, "shim.pid"))
	if err != nil {
		return nil, fmt.Errorf("read shim.pid: %w", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
	if err != nil || pid <= 0 {
		return nil, fmt.Errorf("invalid shim.pid %q", strings.TrimSpace(string(pidBytes)))
	}
	conn, err := net.DialTimeout("unix", strings.TrimPrefix(boot.Address, "unix://"), 10*time.Second)
	if err != nil {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		return nil, fmt.Errorf("dial shim %q: %w", boot.Address, err)
	}
	client := ttrpc.NewClient(conn)
	inst := &Instance{ID: id, Bundle: bundle, PID: pid, Endpoint: boot.Address, client: client}
	inst.Sandbox = sandboxapi.NewTTRPCSandboxClient(client)
	inst.Checkpoint = sandboxapi.NewTTRPCCheckpointClient(client)
	inst.Task = taskapi.NewTTRPCTaskClient(client)
	if err := writeJSONAtomic(filepath.Join(bundle, "bootstrap.json"), boot); err != nil {
		_ = client.Close()
		return nil, err
	}
	m.mu.Lock()
	m.instances[id] = inst
	m.mu.Unlock()
	go m.monitor(inst)
	return inst, nil
}

func ensureSandboxSpec(bundle string) error {
	path := filepath.Join(bundle, "config.json")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// The sandbox service needs an OCI-shaped spec for bookkeeping, but no
	// host process is executed from this bundle. Workload specs live in the
	// task bundles prepared by atelet.
	const spec = `{"ociVersion":"1.2.0","process":{"terminal":false,"user":{"uid":0,"gid":0},"args":["/pause"],"cwd":"/"},"root":{"path":"rootfs"},"linux":{"namespaces":[]}}`
	if err := os.Mkdir(filepath.Join(bundle, "rootfs"), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return os.WriteFile(path, []byte(spec), 0o600)
}

func parseBootstrap(out []byte) (*bootstrapResult, error) {
	var boot bootstrapResult
	if err := json.Unmarshal(out, &boot); err != nil {
		return nil, fmt.Errorf("decode shim bootstrap: %w", err)
	}
	if boot.Version != supportedShimVersion {
		return nil, fmt.Errorf("shim bootstrap version %d, require %d", boot.Version, supportedShimVersion)
	}
	if boot.Protocol != "ttrpc" || !strings.HasPrefix(boot.Address, "unix://") {
		return nil, fmt.Errorf("unsupported shim endpoint %q+%q", boot.Protocol, boot.Address)
	}
	return &boot, nil
}

func writeJSONAtomic(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (m *ShimManager) Get(id string) (*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inst := m.instances[id]
	if inst == nil {
		return nil, fmt.Errorf("shim %q not found", id)
	}
	return inst, nil
}

func (m *ShimManager) monitor(inst *Instance) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := inst.Sandbox.PingSandbox(ctx, &sandboxapi.PingRequest{SandboxId: inst.ID})
		cancel()
		if err == nil {
			continue
		}
		if syscall.Kill(inst.PID, 0) == nil {
			continue // process alive, TTRPC may recover
		}
		_ = inst.client.Close()
		m.mu.Lock()
		if m.instances[inst.ID] == inst {
			delete(m.instances, inst.ID)
		}
		m.mu.Unlock()
		return
	}
}

func (m *ShimManager) Remove(id string) {
	m.mu.Lock()
	inst := m.instances[id]
	delete(m.instances, id)
	m.mu.Unlock()
	if inst != nil {
		_ = inst.client.Close()
	}
}

// Stop closes the client and ensures the exact shim process started for this
// sandbox is gone. ShutdownSandbox normally asks the shim server to exit; the
// signal fallback prevents a failed/partial CreateSandbox from leaking a shim.
func (m *ShimManager) Stop(id string) error {
	m.mu.Lock()
	inst := m.instances[id]
	delete(m.instances, id)
	m.mu.Unlock()
	if inst == nil {
		return nil
	}
	slog.Info("Stopping Kata shim", "sandbox_id", id, "pid", inst.PID)
	_ = inst.client.Close()
	if socket := strings.TrimPrefix(inst.Endpoint, "unix://"); socket != inst.Endpoint {
		_ = os.Remove(socket)
	}
	if processExited(inst.PID) {
		slog.Info("Kata shim already exited", "sandbox_id", id, "pid", inst.PID)
		return nil
	}
	if err := syscall.Kill(inst.PID, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	slog.Info("Sent SIGTERM to Kata shim", "sandbox_id", id, "pid", inst.PID)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if processExited(inst.PID) {
			slog.Info("Kata shim exited after SIGTERM", "sandbox_id", id, "pid", inst.PID)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := syscall.Kill(inst.PID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	slog.Warn("Kata shim required SIGKILL", "sandbox_id", id, "pid", inst.PID)
	return nil
}

// processExited recognizes and reaps an exited shim when ateom-kata is PID 1.
// A zombie still answers kill(pid, 0), which would otherwise force every clean
// shim shutdown down the SIGKILL fallback. If the shim is not our child,
// Wait4 returns ECHILD and the original liveness probe remains authoritative.
func processExited(pid int) bool {
	var status syscall.WaitStatus
	if waited, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil); err == nil && waited == pid {
		return true
	}
	return syscall.Kill(pid, 0) != nil
}

// TaskManager is the Task v3 facade used for both cold create and restored
// task adoption.
type TaskManager struct{ shims *ShimManager }

func NewTaskManager(shims *ShimManager) *TaskManager { return &TaskManager{shims: shims} }

func (m *TaskManager) Create(ctx context.Context, sid string, req *taskapi.CreateTaskRequest) (*taskapi.CreateTaskResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.Create(ctx, req)
}

func (m *TaskManager) Start(ctx context.Context, sid, tid string) (*taskapi.StartResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.Start(ctx, &taskapi.StartRequest{ID: tid})
}

func (m *TaskManager) State(ctx context.Context, sid, tid string) (*taskapi.StateResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.State(ctx, &taskapi.StateRequest{ID: tid})
}

func (m *TaskManager) RequireCreated(ctx context.Context, sid string, tids []string) error {
	for _, tid := range tids {
		st, err := m.State(ctx, sid, tid)
		if err != nil {
			return fmt.Errorf("state restored task %q: %w", tid, err)
		}
		if st.GetStatus() != tasktypes.Status_CREATED {
			return fmt.Errorf("restored task %q is %s, require CREATED", tid, st.GetStatus())
		}
	}
	return nil
}

func (m *TaskManager) Wait(ctx context.Context, sid, tid string) (*taskapi.WaitResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.Wait(ctx, &taskapi.WaitRequest{ID: tid})
}

func (m *TaskManager) Kill(ctx context.Context, sid, tid string, signal uint32, all bool) error {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return err
	}
	_, err = inst.Task.Kill(ctx, &taskapi.KillRequest{ID: tid, Signal: signal, All: all})
	return err
}

func (m *TaskManager) Delete(ctx context.Context, sid, tid string) (*taskapi.DeleteResponse, error) {
	inst, err := m.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Task.Delete(ctx, &taskapi.DeleteRequest{ID: tid})
}

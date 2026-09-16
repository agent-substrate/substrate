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

package sandboxd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	sandboxapi "github.com/agent-substrate/substrate/internal/sandboxd/sandboxapi"
	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

const podSandboxConfigTypeURL = "runtime.v1.PodSandboxConfig"
const vmRestoredTaskCheckpoint = "vm-restored-v1"
const operationIDOption = "io.agent-substrate.operation-id"

type RestoreTask struct {
	CheckpointKey string
	TaskID        string
	Bundle        string
	Terminal      bool
	Stdin         string
	Stdout        string
	Stderr        string
	Options       *anypb.Any
}

type SandboxController struct {
	shims *ShimManager
	tasks *TaskManager
	mu    sync.Mutex
	netns map[string]networkState
}

type networkState struct {
	path       string
	interfaces map[string]bool // true when an ingress qdisc predated sandboxd
}

func NewSandboxController(shims *ShimManager, tasks *TaskManager) *SandboxController {
	return &SandboxController{shims: shims, tasks: tasks, netns: map[string]networkState{}}
}

func (c *SandboxController) Create(ctx context.Context, sid, netns string, options *anypb.Any, annotations map[string]string) error {
	if err := requireSandboxOptions(options); err != nil {
		return err
	}
	if err := c.rememberNetNS(sid, netns); err != nil {
		return err
	}
	inst, err := c.shims.Start(ctx, sid)
	if err != nil {
		return err
	}
	_, err = inst.Sandbox.CreateSandbox(ctx, &sandboxapi.CreateSandboxRequest{
		SandboxId: sid, BundlePath: inst.Bundle, Options: options,
		NetnsPath: netns, Annotations: cloneMap(annotations),
	})
	if err != nil {
		_ = c.shutdown(context.WithoutCancel(ctx), sid)
		return fmt.Errorf("create Kata sandbox %q: %w", sid, err)
	}
	return nil
}

func (c *SandboxController) Start(ctx context.Context, sid string) error {
	inst, err := c.shims.Get(sid)
	if err != nil {
		return err
	}
	resp, err := inst.Sandbox.StartSandbox(ctx, &sandboxapi.StartSandboxRequest{SandboxId: sid})
	if err != nil {
		return fmt.Errorf("start Kata sandbox %q: %w", sid, err)
	}
	if resp.GetPid() == 0 || resp.GetCreatedAt() == nil {
		return errors.New("kata StartSandbox returned no pid or creation time")
	}
	return nil
}

// Ping verifies the sandbox shim is responsive before starting a heavy operation.
func (c *SandboxController) Ping(ctx context.Context, sid string) error {
	inst, err := c.shims.Get(sid)
	if err != nil {
		return err
	}
	_, err = inst.Sandbox.PingSandbox(ctx, &sandboxapi.PingRequest{SandboxId: sid})
	return err
}

// Status returns the sandbox state as reported by the shim.
func (c *SandboxController) Status(ctx context.Context, sid string) (*sandboxapi.SandboxStatusResponse, error) {
	inst, err := c.shims.Get(sid)
	if err != nil {
		return nil, err
	}
	return inst.Sandbox.SandboxStatus(ctx, &sandboxapi.SandboxStatusRequest{SandboxId: sid, Verbose: true})
}

func (c *SandboxController) Checkpoint(ctx context.Context, sid, output, operationID string, inventory map[string]string) error {
	if err := c.Ping(ctx, sid); err != nil {
		return fmt.Errorf("sandbox %q is not responsive before checkpoint: %w", sid, err)
	}
	st, err := c.Status(ctx, sid)
	if err != nil {
		return fmt.Errorf("sandbox %q status before checkpoint: %w", sid, err)
	}
	if st.GetState() != "SANDBOX_READY" {
		return fmt.Errorf("sandbox %q is in state %q, require SANDBOX_READY", sid, st.GetState())
	}
	inst, err := c.shims.Get(sid)
	if err != nil {
		return err
	}
	if err := prepareEmptyDir(output); err != nil {
		return fmt.Errorf("prepare checkpoint output: %w", err)
	}
	containers, err := checkpointContainers(inventory)
	if err != nil {
		return err
	}
	_, err = inst.Checkpoint.Checkpoint(ctx, &sandboxapi.CheckpointRequest{
		SandboxId: sid, OutputPath: output, Containers: containers,
		Options: map[string]string{operationIDOption: operationID},
	})
	if err != nil {
		return fmt.Errorf("checkpoint Kata sandbox %q: %w", sid, err)
	}
	return ValidateCheckpoint(output, operationID)
}

func (c *SandboxController) Restore(ctx context.Context, sid, checkpoint, netns, operationID string, options *anypb.Any, tasks []RestoreTask) (_ *sandboxapi.RestoreResponse, retErr error) {
	if err := requireSandboxOptions(options); err != nil {
		return nil, err
	}
	if err := c.rememberNetNS(sid, netns); err != nil {
		return nil, err
	}
	if err := ValidateCheckpoint(checkpoint); err != nil {
		return nil, err
	}
	reqContainers, err := restoreContainers(tasks)
	if err != nil {
		return nil, err
	}
	inst, err := c.shims.Start(ctx, sid)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = c.shutdown(context.WithoutCancel(ctx), sid)
		}
	}()
	resp, err := inst.Checkpoint.Restore(ctx, &sandboxapi.RestoreRequest{
		SandboxId: sid, CheckpointPath: checkpoint, SandboxConfig: options,
		NetnsPath: netns, Containers: reqContainers,
		Options: map[string]string{operationIDOption: operationID},
		AcceptedTaskRestoreModes: []sandboxapi.TaskRestoreMode{
			sandboxapi.TaskRestoreMode_TASK_RESTORE_MODE_VM_RESTORED,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("restore Kata sandbox %q: %w", sid, err)
	}
	if err := ValidateRestoreResult(reqContainers, resp); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if _, err := c.tasks.Create(ctx, sid, &taskapi.CreateTaskRequest{
			ID: task.TaskID, Bundle: task.Bundle, Terminal: task.Terminal,
			Stdin: task.Stdin, Stdout: task.Stdout, Stderr: task.Stderr,
			Options: task.Options, Checkpoint: vmRestoredTaskCheckpoint,
		}); err != nil {
			return nil, fmt.Errorf("adopt VM-restored task %q: %w", task.TaskID, err)
		}
		ids = append(ids, task.TaskID)
	}
	if err := c.tasks.RequireCreated(ctx, sid, ids); err != nil {
		return nil, err
	}
	st, err := c.Status(ctx, sid)
	if err != nil {
		return nil, fmt.Errorf("sandbox %q status after restore: %w", sid, err)
	}
	if st.GetState() != "SANDBOX_READY" {
		return nil, fmt.Errorf("restored sandbox %q is in state %q, require SANDBOX_READY", sid, st.GetState())
	}
	return resp, nil
}

// StartRestored explicitly starts every CREATED task. Restore itself
// must not re-run the OCI entrypoint.
func (c *SandboxController) StartRestored(ctx context.Context, sid string, tasks []RestoreTask) error {
	started := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if _, err := c.tasks.Start(ctx, sid, task.TaskID); err != nil {
			for _, id := range started {
				_ = c.tasks.Kill(context.WithoutCancel(ctx), sid, id, 9, true)
			}
			return fmt.Errorf("start restored task %q: %w", task.TaskID, err)
		}
		started = append(started, task.TaskID)
	}
	return nil
}

func (c *SandboxController) shutdown(ctx context.Context, sid string) error {
	inst, err := c.shims.Get(sid)
	if err != nil {
		return err
	}
	_, callErr := inst.Sandbox.ShutdownSandbox(ctx, &sandboxapi.ShutdownSandboxRequest{SandboxId: sid})
	stopErr := c.shims.Stop(sid)
	netErr := cleanupKataTaps(c.takeNetNS(sid))
	if rmErr := os.RemoveAll(inst.Bundle); rmErr != nil {
		return errors.Join(callErr, stopErr, netErr, rmErr)
	}
	return errors.Join(callErr, stopErr, netErr)
}

func (c *SandboxController) Shutdown(ctx context.Context, sid string) error {
	return c.shutdown(ctx, sid)
}

func (c *SandboxController) rememberNetNS(sid, netns string) error {
	state, err := inspectNetworkState(netns)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.netns[sid] = state
	c.mu.Unlock()
	return nil
}

func (c *SandboxController) takeNetNS(sid string) networkState {
	c.mu.Lock()
	defer c.mu.Unlock()
	netns := c.netns[sid]
	delete(c.netns, sid)
	return netns
}

var kataTapName = regexp.MustCompile(`^tap[0-9]+_kata$`)

func inspectNetworkState(netns string) (networkState, error) {
	state := networkState{path: netns, interfaces: map[string]bool{}}
	if netns == "" {
		return state, nil
	}
	out, err := exec.Command("nsenter", "--net="+netns, "--", "ip", "-o", "link", "show").CombinedOutput()
	if err != nil {
		return state, fmt.Errorf("inspect netns %q links: %w: %s", netns, err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimSpace(strings.SplitN(parts[1], "@", 2)[0])
		if name == "" || name == "lo" || strings.HasPrefix(name, "tunl") || kataTapName.MatchString(name) {
			continue
		}
		qdisc, err := exec.Command("nsenter", "--net="+netns, "--", "tc", "qdisc", "show", "dev", name).CombinedOutput()
		if err != nil {
			return state, fmt.Errorf("inspect qdisc on %q: %w: %s", name, err, qdisc)
		}
		state.interfaces[name] = strings.Contains(string(qdisc), "qdisc ingress ")
	}
	if len(state.interfaces) != 1 {
		return state, fmt.Errorf("netns %q has %d CNI interfaces, require exactly one", netns, len(state.interfaces))
	}
	return state, nil
}

// cleanupKataTaps removes only interfaces whose naming rule is defined by
// runtime-rs NetworkPair (tap{index}_kata). CNI-owned interfaces are untouched.
func cleanupKataTaps(state networkState) error {
	if state.path == "" {
		return nil
	}
	args := []string{"--net=" + state.path, "--", "ip", "-o", "link", "show"}
	out, err := exec.Command("nsenter", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("list Kata interfaces in netns %q: %w: %s", state.path, err, out)
	}
	hasKataTap := false
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimSpace(strings.SplitN(parts[1], "@", 2)[0])
		if !kataTapName.MatchString(name) {
			continue
		}
		hasKataTap = true
	}
	if !hasKataTap {
		return nil
	}
	for name, hadIngress := range state.interfaces {
		if hadIngress {
			continue
		}
		deleteOut, err := exec.Command("nsenter", "--net="+state.path, "--", "tc", "qdisc", "delete", "dev", name, "ingress").CombinedOutput()
		if err != nil {
			return fmt.Errorf("delete Kata ingress qdisc on %q: %w: %s", name, err, deleteOut)
		}
	}
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimSpace(strings.SplitN(parts[1], "@", 2)[0])
		if !kataTapName.MatchString(name) {
			continue
		}
		deleteOut, err := exec.Command("nsenter", "--net="+state.path, "--", "ip", "link", "delete", "dev", name).CombinedOutput()
		if err != nil {
			return fmt.Errorf("delete Kata interface %q: %w: %s", name, err, deleteOut)
		}
	}
	return nil
}

func requireSandboxOptions(options *anypb.Any) error {
	if options == nil || options.TypeUrl != podSandboxConfigTypeURL || len(options.Value) == 0 {
		return fmt.Errorf("sandbox options must be non-empty %q", podSandboxConfigTypeURL)
	}
	return nil
}

func checkpointContainers(inventory map[string]string) ([]*sandboxapi.CheckpointContainer, error) {
	keys := make([]string, 0, len(inventory))
	for key, id := range inventory {
		if key == "" || !validID(id) {
			return nil, fmt.Errorf("invalid checkpoint task %q=%q", key, id)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]*sandboxapi.CheckpointContainer, 0, len(keys))
	for _, key := range keys {
		out = append(out, &sandboxapi.CheckpointContainer{Name: key, Id: inventory[key]})
	}
	return out, nil
}

func restoreContainers(in []RestoreTask) ([]*sandboxapi.RestoreContainer, error) {
	seenKeys, seenIDs := map[string]bool{}, map[string]bool{}
	out := make([]*sandboxapi.RestoreContainer, 0, len(in))
	for _, task := range in {
		if task.CheckpointKey == "" || !validID(task.TaskID) || !filepath.IsAbs(task.Bundle) {
			return nil, fmt.Errorf("invalid restore task key=%q id=%q bundle=%q", task.CheckpointKey, task.TaskID, task.Bundle)
		}
		if seenKeys[task.CheckpointKey] || seenIDs[task.TaskID] {
			return nil, fmt.Errorf("duplicate restore task key=%q id=%q", task.CheckpointKey, task.TaskID)
		}
		seenKeys[task.CheckpointKey], seenIDs[task.TaskID] = true, true
		if _, err := os.Stat(filepath.Join(task.Bundle, "config.json")); err != nil {
			return nil, fmt.Errorf("restore task %q bundle: %w", task.TaskID, err)
		}
		out = append(out, &sandboxapi.RestoreContainer{
			CheckpointKey: task.CheckpointKey, Id: task.TaskID, Name: task.CheckpointKey, BundlePath: task.Bundle,
			Terminal: task.Terminal, Stdin: task.Stdin, Stdout: task.Stdout, Stderr: task.Stderr,
			Config: task.Options,
		})
	}
	return out, nil
}

func ValidateRestoreResult(want []*sandboxapi.RestoreContainer, got *sandboxapi.RestoreResponse) error {
	if got == nil {
		return errors.New("restore result is nil")
	}
	wantSet := map[string]bool{}
	for _, container := range want {
		wantSet[container.GetName()] = true
	}
	gotSet := map[string]bool{}
	for _, container := range got.GetContainers() {
		name := container.GetName()
		if name == "" || gotSet[name] {
			return fmt.Errorf("invalid or duplicate restored container %q", name)
		}
		if container.GetTaskRestoreMode() != sandboxapi.TaskRestoreMode_TASK_RESTORE_MODE_VM_RESTORED {
			return fmt.Errorf("restored container %q did not select VM-restored task adoption", name)
		}
		if container.GetTaskCheckpointImage() != "" {
			return fmt.Errorf("restored container %q returned an unexpected task checkpoint image", name)
		}
		gotSet[name] = true
	}
	if len(wantSet) != len(gotSet) {
		return fmt.Errorf("restore returned %d containers, require %d", len(gotSet), len(wantSet))
	}
	for name := range wantSet {
		if !gotSet[name] {
			return fmt.Errorf("restore result is missing container %q", name)
		}
	}
	return nil
}

func prepareEmptyDir(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("path must be absolute")
	}
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(path, 0o700)
	}
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return errors.New("checkpoint output directory is not empty")
	}
	return nil
}

func cloneMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

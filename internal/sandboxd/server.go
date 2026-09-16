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
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	taskapi "github.com/containerd/containerd/api/runtime/task/v3"
	containerdtypes "github.com/containerd/containerd/api/types"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

func ValidateOperationID(operationID string) error {
	if len(operationID) == 0 || len(operationID) > 128 || strings.TrimSpace(operationID) != operationID {
		return errors.New("operation ID must be a non-empty UUID of at most 128 bytes")
	}
	if _, err := uuid.Parse(operationID); err != nil {
		return fmt.Errorf("operation ID must be a UUID: %w", err)
	}
	return nil
}

// Task is the subset of task configuration needed by the embedded Kata runtime.
type Task struct {
	CheckpointKey string
	TaskID        string
	Bundle        string
	Terminal      bool
	Stdin         string
	Stdout        string
	Stderr        string
	RootfsFile    string
	RootfsFSType  string
	RootfsOptions []string
}

// Sandbox describes one Kata sandbox and its tasks.
type Sandbox struct {
	SandboxID        string
	NetNSPath        string
	PodSandboxConfig []byte
	Annotations      map[string]string
	Tasks            []*Task
}

// Runtime owns the Worker-Pod-local Kata lifecycle used by ateom-kata.
type Runtime struct {
	controller *SandboxController
	tasks      *TaskManager
}

func NewRuntime(controller *SandboxController, tasks *TaskManager) *Runtime {
	return &Runtime{controller: controller, tasks: tasks}
}

func (r *Runtime) Run(ctx context.Context, sb *Sandbox) (retErr error) {
	sb, err := validateSandbox(sb)
	if err != nil {
		return invalid(err)
	}
	options := &anypb.Any{TypeUrl: podSandboxConfigTypeURL, Value: sb.PodSandboxConfig}
	if err := r.controller.Create(ctx, sb.SandboxID, sb.NetNSPath, options, sb.Annotations); err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = r.controller.Shutdown(context.WithoutCancel(ctx), sb.SandboxID)
		}
	}()
	if err := r.controller.Start(ctx, sb.SandboxID); err != nil {
		return err
	}
	for _, task := range sb.Tasks {
		create, err := createTaskRequest(task)
		if err != nil {
			return invalid(err)
		}
		if _, err := r.tasks.Create(ctx, sb.SandboxID, create); err != nil {
			return fmt.Errorf("create task %q: %w", task.TaskID, err)
		}
		if _, err := r.tasks.Start(ctx, sb.SandboxID, task.TaskID); err != nil {
			return fmt.Errorf("start task %q: %w", task.TaskID, err)
		}
	}
	return nil
}

func (r *Runtime) Checkpoint(ctx context.Context, sandboxID, outputPath, operationID string, tasks map[string]string) error {
	if !validID(sandboxID) || len(tasks) == 0 {
		return invalid(errors.New("sandbox id and task inventory are required"))
	}
	if err := ValidateOperationID(operationID); err != nil {
		return invalid(err)
	}
	return r.controller.Checkpoint(ctx, sandboxID, outputPath, operationID, tasks)
}

func (r *Runtime) Restore(ctx context.Context, sb *Sandbox, checkpointPath, operationID string) error {
	sb, err := validateSandbox(sb)
	if err != nil {
		return invalid(err)
	}
	if err := ValidateOperationID(operationID); err != nil {
		return invalid(err)
	}
	tasks := make([]RestoreTask, 0, len(sb.Tasks))
	for _, task := range sb.Tasks {
		tasks = append(tasks, RestoreTask{
			CheckpointKey: task.CheckpointKey, TaskID: task.TaskID, Bundle: task.Bundle,
			Terminal: task.Terminal, Stdin: task.Stdin, Stdout: task.Stdout, Stderr: task.Stderr,
		})
	}
	options := &anypb.Any{TypeUrl: podSandboxConfigTypeURL, Value: sb.PodSandboxConfig}
	_, err = r.controller.Restore(ctx, sb.SandboxID, checkpointPath, sb.NetNSPath, operationID, options, tasks)
	if err != nil {
		return err
	}
	if err := r.controller.StartRestored(ctx, sb.SandboxID, tasks); err != nil {
		_ = r.controller.Shutdown(context.WithoutCancel(ctx), sb.SandboxID)
		return err
	}
	return nil
}

func (r *Runtime) State(ctx context.Context, sandboxID, taskID string) (*taskapi.StateResponse, error) {
	return r.tasks.State(ctx, sandboxID, taskID)
}

func (r *Runtime) Wait(ctx context.Context, sandboxID, taskID string) (*taskapi.WaitResponse, error) {
	return r.tasks.Wait(ctx, sandboxID, taskID)
}

func (r *Runtime) Kill(ctx context.Context, sandboxID, taskID string, signal uint32, all bool) error {
	return r.tasks.Kill(ctx, sandboxID, taskID, signal, all)
}

// Drain asks every workload process to exit cleanly. The caller owns the
// timeout and follows with Delete, which supplies the SIGKILL fallback.
func (r *Runtime) Drain(ctx context.Context, sb *Sandbox) error {
	for _, task := range sb.Tasks {
		if err := r.tasks.Kill(ctx, sb.SandboxID, task.TaskID, uint32(syscall.SIGTERM), true); err != nil {
			return fmt.Errorf("signal task %q during drain: %w", task.TaskID, err)
		}
	}
	for _, task := range sb.Tasks {
		if _, err := r.tasks.Wait(ctx, sb.SandboxID, task.TaskID); err != nil {
			return fmt.Errorf("wait task %q during drain: %w", task.TaskID, err)
		}
	}
	return nil
}

func (r *Runtime) Delete(ctx context.Context, sandboxID string, taskIDs []string) error {
	for i := len(taskIDs) - 1; i >= 0; i-- {
		taskID := taskIDs[i]
		state, err := r.tasks.State(ctx, sandboxID, taskID)
		if err != nil {
			return fmt.Errorf("state task %q before delete: %w", taskID, err)
		}
		if state.GetStatus() == tasktypes.Status_RUNNING || state.GetStatus() == tasktypes.Status_PAUSED {
			if err := r.tasks.Kill(ctx, sandboxID, taskID, 9, true); err != nil {
				return fmt.Errorf("kill task %q before delete: %w", taskID, err)
			}
			if _, err := r.tasks.Wait(ctx, sandboxID, taskID); err != nil {
				return fmt.Errorf("wait task %q before delete: %w", taskID, err)
			}
		}
		if _, err := r.tasks.Delete(ctx, sandboxID, taskID); err != nil {
			return fmt.Errorf("delete task %q: %w", taskID, err)
		}
	}
	return r.controller.Shutdown(ctx, sandboxID)
}

func validateSandbox(sb *Sandbox) (*Sandbox, error) {
	if sb == nil || !validID(sb.SandboxID) || sb.NetNSPath == "" || len(sb.PodSandboxConfig) == 0 || len(sb.Tasks) == 0 {
		return nil, errors.New("sandbox id, netns, PodSandboxConfig and tasks are required")
	}
	for _, task := range sb.Tasks {
		if task == nil {
			return nil, errors.New("sandbox task is required")
		}
	}
	return sb, nil
}

func createTaskRequest(task *Task) (*taskapi.CreateTaskRequest, error) {
	if task == nil || !validID(task.TaskID) || !filepath.IsAbs(task.Bundle) || !filepath.IsAbs(task.RootfsFile) || task.RootfsFSType == "" {
		return nil, errors.New("task id, absolute bundle/rootfs and rootfs fs type are required")
	}
	if _, err := os.Stat(filepath.Join(task.Bundle, "config.json")); err != nil {
		return nil, fmt.Errorf("task bundle: %w", err)
	}
	if err := prepareRootfsFile(task.Bundle, task.RootfsFile); err != nil {
		return nil, err
	}
	options := append([]string(nil), task.RootfsOptions...)
	hasLoop := false
	for _, option := range options {
		hasLoop = hasLoop || option == "loop"
	}
	if !hasLoop {
		options = append(options, "loop")
	}
	return &taskapi.CreateTaskRequest{
		ID: task.TaskID, Bundle: task.Bundle, Terminal: task.Terminal,
		Stdin: task.Stdin, Stdout: task.Stdout, Stderr: task.Stderr,
		Rootfs: []*containerdtypes.Mount{{Type: task.RootfsFSType, Source: task.RootfsFile, Options: options}},
	}, nil
}

func prepareRootfsFile(bundle, output string) error {
	if info, err := os.Lstat(output); err == nil {
		if !info.Mode().IsRegular() {
			return errors.New("rootfs_file must be a regular file")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	source := filepath.Join(bundle, "rootfs")
	if info, err := os.Stat(source); err != nil || !info.IsDir() {
		return errors.New("bundle rootfs directory is missing")
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return err
	}
	size, err := managedRootfsSize(source)
	if err != nil {
		return err
	}
	tmp := output + ".partial"
	_ = os.Remove(tmp)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	cmd := exec.Command("mkfs.ext4", "-q", "-F", "-d", source, tmp)
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("mkfs.ext4 rootfs: %w: %s", err, out)
	}
	if err := os.Rename(tmp, output); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func managedRootfsSize(root string) (int64, error) {
	const (
		minimum = int64(512 << 20)
		reserve = int64(128 << 20)
		align   = int64(1 << 20)
	)
	var content int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > 0 && content > (int64(^uint64(0)>>1)-info.Size()) {
			return errors.New("rootfs size overflow")
		}
		content += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("measure bundle rootfs: %w", err)
	}
	size := content + content/2 + reserve
	if size < minimum {
		size = minimum
	}
	return (size + align - 1) / align * align, nil
}

func invalid(err error) error { return status.Error(codes.InvalidArgument, err.Error()) }

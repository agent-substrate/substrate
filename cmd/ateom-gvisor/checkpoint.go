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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// Bound inspection, including a stopped container's diagnostic wait, so a
// broken sandbox cannot leave the checkpoint frozen indefinitely.
const checkpointInspectionTimeout = 10 * time.Second

// checkpointRunningWorkload freezes the sandbox before checking its application
// containers, so none can exit between inspection and saving. The pause
// container alone can outlive every application and still checkpoint successfully.
func (r *runsc) checkpointRunningWorkload(ctx context.Context, containers []*ateompb.Container, checkpointPath string) (retErr error) {
	if err := r.cmdPause(ctx, ocispec.PauseContainer); err != nil {
		return fmt.Errorf("while pausing sandbox for checkpoint: %w", err)
	}
	// Balance our pause even after a successful save: runsc queues the sandbox's
	// exit, but its tasks cannot finish exiting until this outer pause is undone.
	defer func() {
		resumeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), resumeTimeout)
		defer cancel()
		if err := r.cmdResume(resumeCtx, ocispec.PauseContainer); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("while resuming sandbox after checkpoint: %w", err))
		}
	}()

	inspectCtx, cancel := context.WithTimeout(ctx, checkpointInspectionTimeout)
	defer cancel()
	for _, container := range containers {
		name := container.GetName()
		var state specs.State
		if err := r.containerJSON(inspectCtx, "state", name, &state); err != nil {
			return fmt.Errorf("cannot checkpoint application container %q: %w", name, err)
		}
		if state.ID != name {
			return fmt.Errorf("cannot checkpoint application container %q: state returned ID %q", name, state.ID)
		}
		if state.Status == specs.StateStopped {
			var result struct {
				ID         string `json:"id"`
				ExitStatus *int   `json:"exitStatus"`
			}
			if err := r.containerJSON(inspectCtx, "wait", name, &result); err != nil {
				return fmt.Errorf("application container %q exited before checkpoint (exit code unknown: %w)", name, err)
			}
			if result.ID != name || result.ExitStatus == nil {
				return fmt.Errorf("application container %q exited before checkpoint (exit code unknown: invalid wait result)", name)
			}
			return fmt.Errorf("application container %q exited before checkpoint (exit code %d)", name, *result.ExitStatus)
		}
		if state.Status != specs.StateRunning {
			return fmt.Errorf("cannot checkpoint application container %q: state is %q, want running", name, state.Status)
		}
	}
	if err := r.cmdCheckpoint(ctx, ocispec.PauseContainer, checkpointPath); err != nil {
		return fmt.Errorf("while checkpointing pause: %w", err)
	}
	return nil
}

// containerJSON keeps stdout separate from runsc's stderr logs. These commands
// run only during bounded checkpoint inspection, including wait after stopped.
func (r *runsc) containerJSON(ctx context.Context, command, name string, dst any) error {
	cmd := exec.CommandContext(ctx, r.path,
		"-log-format", "json", "--alsologtostderr",
		"-root", runscStateDir(r.actorDirs), command, name)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("runsc %s: %w: %s", command, err, strings.TrimSpace(stderr.String()))
	}
	if err := json.Unmarshal(stdout.Bytes(), dst); err != nil {
		return fmt.Errorf("decoding runsc %s: %w", command, err)
	}
	return nil
}

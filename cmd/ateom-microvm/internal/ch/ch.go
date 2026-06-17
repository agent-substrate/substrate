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

// Package ch drives a single cloud-hypervisor instance over its REST
// api-socket: pause, snapshot, resume against a running VMM (e.g. the socket
// kata creates at /run/vc/vm/<id>/clh-api.sock), plus relaunching a fresh VMM
// from a snapshot directory for restore.
//
// This is the snapshot/restore half of the ateom-microvm model: kata
// owns RUN (boot the micro-VM + run the OCI container), and ateom drives the CH
// REST API underneath for suspend (pause+snapshot) and owns the bare-CH
// relaunch for restore. The REST wire format and the --restore CLI form are
// the ones cloud-hypervisor documents for snapshot/restore.
package ch

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

// Client talks to one cloud-hypervisor VMM over its unix api-socket.
type Client struct {
	apiSocket string
	api       *apiClient
}

// NewClient returns a Client bound to a cloud-hypervisor api-socket path. The
// socket need not exist yet; use WaitReady to block until the VMM answers.
func NewClient(apiSocket string) *Client {
	return &Client{apiSocket: apiSocket, api: newAPIClient(apiSocket)}
}

// APISocket returns the api-socket path this client is bound to.
func (c *Client) APISocket() string { return c.apiSocket }

// Ping returns nil if the VMM api-socket answers vmm.ping.
func (c *Client) Ping(ctx context.Context) error {
	return c.api.get(ctx, "/api/v1/vmm.ping")
}

// WaitReady blocks until the api-socket answers vmm.ping or the deadline passes.
func (c *Client) WaitReady(ctx context.Context, deadline time.Duration) error {
	end := time.Now().Add(deadline)
	for {
		if err := c.Ping(ctx); err == nil {
			return nil
		}
		if !time.Now().Before(end) {
			return fmt.Errorf("cloud-hypervisor api socket %q not ready after %s", c.apiSocket, deadline)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// State returns the VM state as reported by vm.info (e.g. "Running", "Paused").
func (c *Client) State(ctx context.Context) (string, error) {
	var info struct {
		State string `json:"state"`
	}
	if err := c.api.getJSON(ctx, "/api/v1/vm.info", &info); err != nil {
		return "", err
	}
	return info.State, nil
}

// Pause pauses the running guest (quiescing it before snapshot). Idempotent:
// already-paused is success (CH itself 500s on pausing a paused VM, which would
// otherwise wedge checkpoint retries after a partial earlier attempt).
func (c *Client) Pause(ctx context.Context) error {
	if state, err := c.State(ctx); err == nil && state == "Paused" {
		return nil
	}
	return c.api.put(ctx, "/api/v1/vm.pause", nil)
}

// Resume resumes a paused guest (after snapshot or restore).
func (c *Client) Resume(ctx context.Context) error {
	return c.api.put(ctx, "/api/v1/vm.resume", nil)
}

// Snapshot writes the (paused) guest's state to destDir as a CH snapshot
// (config.json + state.json + memory-ranges). The guest must be paused first.
func (c *Client) Snapshot(ctx context.Context, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("while creating snapshot dir %q: %w", destDir, err)
	}
	return c.api.put(ctx, "/api/v1/vm.snapshot", snapshotConfig{DestinationURL: SnapshotURL(destDir)})
}

// Shutdown best-effort tears down the VM and the VMM process behind the socket.
func (c *Client) Shutdown(ctx context.Context) error {
	_ = c.api.put(ctx, "/api/v1/vm.shutdown", nil)
	return c.api.put(ctx, "/api/v1/vmm.shutdown", nil)
}

// SnapshotURL returns the file:// URL cloud-hypervisor expects for a snapshot
// destination or restore source directory.
func SnapshotURL(dir string) string { return "file://" + dir }

// RestoreOptions configures relaunching a fresh cloud-hypervisor process from a
// snapshot directory (the restore half of suspend/resume).
type RestoreOptions struct {
	// Binary is the cloud-hypervisor executable (defaults to "cloud-hypervisor"
	// on PATH if empty).
	Binary string
	// APISocket is the api-socket path the new VMM should listen on.
	APISocket string
	// SourceDir is the snapshot directory to restore from.
	SourceDir string
	// MemoryRestoreMode selects how guest RAM is brought back: "" or "copy" for
	// eager copy (CH default), "ondemand" for userfaultfd demand-paging.
	MemoryRestoreMode string
	// ExtraArgs are appended verbatim (e.g. cgroup/log flags).
	ExtraArgs []string
	// Stdout/Stderr capture the VMM process output (e.g. a vmm.log file).
	Stdout io.Writer
	Stderr io.Writer
}

// RestoreArgs builds the cloud-hypervisor argv for restoring from a snapshot.
// Pure (no I/O) so it can be unit tested.
func RestoreArgs(o RestoreOptions) []string {
	restoreArg := "source_url=" + SnapshotURL(o.SourceDir)
	switch o.MemoryRestoreMode {
	case "ondemand":
		// ondemand uses userfaultfd; prefault must be off.
		restoreArg += ",memory_restore_mode=ondemand,prefault=off"
	case "", "copy":
		// Eager copy is CH's default; leave memory_restore_mode unset.
	}
	args := []string{"--api-socket", o.APISocket, "--restore", restoreArg}
	return append(args, o.ExtraArgs...)
}

// Restore launches a fresh cloud-hypervisor process with --restore and waits
// until its api-socket answers. The restored VM comes back paused; call Resume
// on the returned Client to run it. The caller owns cmd (must Wait/Kill it).
func Restore(ctx context.Context, o RestoreOptions) (cmd *exec.Cmd, client *Client, err error) {
	if o.APISocket == "" {
		return nil, nil, fmt.Errorf("RestoreOptions.APISocket is required")
	}
	if o.SourceDir == "" {
		return nil, nil, fmt.Errorf("RestoreOptions.SourceDir is required")
	}
	bin := o.Binary
	if bin == "" {
		bin = "cloud-hypervisor"
	}
	// A stale socket from a prior VMM blocks bind; remove it best-effort.
	_ = os.Remove(o.APISocket)

	// Deliberately NOT exec.CommandContext: the restored VMM must outlive the
	// Restore RPC whose ctx launched it (gRPC cancels the ctx when the handler
	// returns, which would SIGKILL the freshly restored VM). The caller owns
	// cmd; the WaitReady below honors ctx for bootstrap cancellation.
	cmd = exec.Command(bin, RestoreArgs(o)...)
	cmd.Stdout = o.Stdout
	cmd.Stderr = o.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("while starting cloud-hypervisor --restore: %w", err)
	}

	client = NewClient(o.APISocket)
	if err := client.WaitReady(ctx, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, nil, fmt.Errorf("while waiting for restored VMM: %w", err)
	}
	return cmd, client, nil
}

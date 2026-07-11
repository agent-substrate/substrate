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

package kata

// Each container's rootfs is an overlay: its OCI image served read-only over virtio-fs
// (the lower) plus a guest tmpfs (the writable upper). The upper is in guest RAM, so
// rootfs writes ride along in the memory snapshot and persist across suspend/resume.
// This file holds the overlay-specific helpers.

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// FsTag is the virtio-fs tag kata uses for the shared filesystem. The CH fs
	// device Tag and the agent mount Source must both be this value.
	FsTag = "kataShared"
	// typeVirtioFS / virtioFSDriver are the agent fstype + driver for it.
	typeVirtioFS   = "virtiofs"
	virtioFSDriver = "virtio-fs"
	// guestSharedDir is where the agent mounts the kataShared tag in the guest;
	// per-container rootfs then lives at <guestSharedDir>/<cid>/rootfs.
	guestSharedDir = "/run/kata-containers/shared/containers/"
)

// SharedDir is the host directory virtiofsd serves into the guest as the RO base.
// Its layout (<cid>/rootfs) is what find-paths re-opens by path on restore.
func SharedDir(id string) string {
	return filepath.Join("/run/kata-containers/shared/sandboxes", id, "shared")
}

// VirtiofsdSocketPath is the vhost-user-fs socket CH connects to for the fs device.
func VirtiofsdSocketPath(id string) string { return filepath.Join(VMDir(id), "virtiofsd.sock") }

// OverlayUpperBase is the in-guest mount point for one container's overlay upper/work.
// It lives under /run (tmpfs) so the upper's writes are in guest RAM and ride along in
// the memory-only snapshot (rootfs writes persist). Keyed on the container id, which is
// stable across the actor's restore lineage.
func OverlayUpperBase(containerID string) string { return "/run/ateom-upper/" + containerID }

// GuestSharedRootfs is the in-guest path the kataShared mount exposes a container's
// rootfs at. A carrier container with this as Root.Path makes the agent bind it to
// /run/kata-containers/<cid>/rootfs — a stable per-container path the overlay then
// uses as its lowerdir.
func GuestSharedRootfs(containerID string) string { return guestSharedDir + containerID + "/rootfs" }

// VirtiofsdOptions configures StartVirtiofsd.
type VirtiofsdOptions struct {
	Binary     string // virtiofsd executable; defaults to "virtiofsd"
	SocketPath string // vhost-user socket CH connects to (VirtiofsdSocketPath)
	SharedDir  string // directory to serve (SharedDir(id))
	Log        io.Writer
}

// StartVirtiofsd launches virtiofsd in find-paths migration mode serving o.SharedDir
// on o.SocketPath, and waits for the socket to appear. The returned cmd outlives the
// caller's ctx (CH demand-pages from it under the running VM); the caller owns it.
func StartVirtiofsd(ctx context.Context, o VirtiofsdOptions) (*exec.Cmd, error) {
	bin := o.Binary
	if bin == "" {
		bin = "virtiofsd"
	}
	_ = os.Remove(o.SocketPath)
	cmd := exec.Command(bin,
		"--socket-path="+o.SocketPath,
		"--shared-dir="+o.SharedDir,
		// The shared dir is served strictly read-only (the overlay's RO lower; the
		// carrier remounts it ro and the guest's overlayfs lowerdir is immutable), so
		// aggressively cache it in the guest for read performance — there is no
		// host<>guest write churn to invalidate.
		// TODO: cache=always serves stale data for any writable virtio-fs mount. If we
		// later need one (e.g. projected volumes), prefer keeping this mount fully
		// read-only and writing such volumes via a separate mechanism (e.g. a writer
		// that execs into the sandbox) rather than dropping back to cache=auto/none.
		"--cache=always",
		"--thread-pool-size=1",
		"--announce-submounts",
		"--migration-mode", "find-paths",
	)
	cmd.Stdout = o.Log
	cmd.Stderr = o.Log
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting virtiofsd: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(o.SocketPath); err == nil {
			return cmd, nil
		}
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	_ = cmd.Process.Kill()
	return nil, fmt.Errorf("virtiofsd socket %q did not appear", o.SocketPath)
}

// ReconstructSharedDirFromImage bind-mounts a container's OCI image rootfs at
// <cid>/rootfs under SharedDir(restoreID) so virtiofsd serves it as the read-only lower.
// The bind copies nothing on the host (virtiofsd serves files to the guest on demand).
// The path is identical on every node — find-paths migration re-opens the lower by path
// — given a deterministic image unpack. cid is stable across the actor's lineage.
func ReconstructSharedDirFromImage(ctx context.Context, bundleRootfs, restoreID, cid string) error {
	if cid == "" {
		return fmt.Errorf("ReconstructSharedDirFromImage: empty container id")
	}
	dst := filepath.Join(SharedDir(restoreID), cid, "rootfs")
	// Drop any stale bind first (lazy if busy), then ensure a clean mountpoint. Not
	// RemoveAll: that would chase a live bind into bundleRootfs.
	if err := exec.Command("umount", dst).Run(); err != nil {
		_ = exec.Command("umount", "-l", dst).Run()
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("creating shared dir %q: %w", dst, err)
	}
	cmd := exec.CommandContext(ctx, "mount", "--bind", bundleRootfs, dst)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bind-mounting image rootfs %q -> %q: %w (%s)", bundleRootfs, dst, err, strings.TrimSpace(stderr.String()))
	}
	// Ensure the standard OCI mountpoints exist even for minimal images: the container
	// mounts /proc,/sys,/dev over them, and find-paths re-opens the lower by path on
	// restore, so the layout must match on every node. (Bind still writable; ignore EEXIST.)
	for _, d := range []string{"proc", "sys", "dev"} {
		_ = os.MkdirAll(filepath.Join(dst, d), 0o755)
	}
	// Remount read-only: the lower is immutable, so all writes go to the tmpfs upper and
	// it stays byte-identical across reconstructions (required by find-paths migration).
	ro := exec.CommandContext(ctx, "mount", "-o", "remount,bind,ro", dst)
	var roErr strings.Builder
	ro.Stderr = &roErr
	if err := ro.Run(); err != nil {
		return fmt.Errorf("remounting overlay lower read-only %q: %w (%s)", dst, err, strings.TrimSpace(roErr.String()))
	}
	return nil
}

// CreateSandboxForActor creates the guest sandbox with the kataShared virtio-fs mount
// (the RO base backing every container's rootfs). Mirrors kata startSandbox.
func (a *AgentClient) CreateSandboxForActor(ctx context.Context, sandboxID, hostname string) error {
	return a.CreateSandbox(ctx, &agentpb.CreateSandboxRequest{
		Hostname:  hostname,
		SandboxId: sandboxID,
		Storages: []*agentpb.Storage{{
			Driver:     virtioFSDriver,
			Source:     FsTag,
			Fstype:     typeVirtioFS,
			MountPoint: guestSharedDir,
		}},
	})
}

// CreateCarrier creates a "carrier" container (id == cid): rootfs = the kataShared
// virtio-fs base for that container, created but NOT started. This makes the agent's
// setup_bundle bind the base to /run/kata-containers/<cid>/rootfs — the stable path the
// overlay uses as its lowerdir (a bare virtio-fs submount is not reliably visible there).
func (a *AgentClient) CreateCarrier(ctx context.Context, cid string, spec *specs.Spec) error {
	pbSpec := SpecToAgentPB(spec)
	// Readonly: the carrier only exists to materialize the base bind; its rootfs (the
	// overlay lower) must stay immutable. Overlay writes go to the tmpfs upper.
	pbSpec.Root = &agentpb.Root{Path: GuestSharedRootfs(cid), Readonly: true}
	if pbSpec.Linux != nil {
		pbSpec.Linux.CgroupsPath = "/ateomchv/" + cid + "-carrier"
	}
	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: cid,
		ExecId:      cid,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating carrier container %q: %w", cid, err)
	}
	return nil
}

// StartOverlayWorkload creates + starts one container with an overlayfs rootfs:
// lower = the carrier's resolved bind (/run/kata-containers/<cid>/rootfs from the RO
// virtio-fs base), upper/work = <upperBase>/{fs,work} on a guest tmpfs so rootfs writes
// land in guest RAM (captured by the memory-only snapshot → persist). The agent creates
// the upper/work dirs (create_directory) before mounting the overlay.
func (a *AgentClient) StartOverlayWorkload(ctx context.Context, cid, workloadID, upperBase string, spec *specs.Spec) error {
	const createDir = "io.katacontainers.volume.overlayfs.create_directory"
	sharedBase := "/run/kata-containers/" + cid + "/rootfs"
	base := "/run/kata-containers/" + workloadID
	lower := base + "/lower"
	ovlRoot := base + "/rootfs"
	upper := upperBase + "/fs"
	work := upperBase + "/work"

	storages := []*agentpb.Storage{
		{
			Driver:     virtioFSDriver,
			Source:     sharedBase,
			MountPoint: lower,
			Fstype:     "bind",
			Options:    []string{"bind"},
		},
		{
			Driver:        "overlayfs",
			Source:        "overlay",
			Fstype:        "overlay",
			MountPoint:    ovlRoot,
			DriverOptions: []string{createDir + "=" + upper, createDir + "=" + work},
			Options:       []string{"lowerdir=" + lower, "upperdir=" + upper, "workdir=" + work},
		},
	}
	pbSpec := SpecToAgentPB(spec)
	pbSpec.Root = &agentpb.Root{Path: ovlRoot, Readonly: false}
	// Per-workload cgroup: the shaped spec carries the actor-wide /ateomchv/<actorName>
	// (spec.go), which collides across an actor's containers — mirror the carrier's
	// per-id path so each workload gets its own cgroup.
	if pbSpec.Linux != nil {
		pbSpec.Linux.CgroupsPath = "/ateomchv/" + workloadID
	}

	if err := a.CreateContainer(ctx, &agentpb.CreateContainerRequest{
		ContainerId: workloadID,
		ExecId:      workloadID,
		Storages:    storages,
		OCI:         pbSpec,
	}); err != nil {
		return fmt.Errorf("creating overlay workload %q: %w", workloadID, err)
	}
	if err := a.StartContainer(ctx, workloadID); err != nil {
		return fmt.Errorf("starting overlay workload %q: %w", workloadID, err)
	}
	return nil
}

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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"go.ate.dev/substrate/cmd/ateworker-kata/internal/shim"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/ateworker/cgroup"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/sizing"
)

// shimClient is the part of *shim.Shim the worker uses. Tests stand a fake in.
type shimClient interface {
	Create(ctx context.Context, req shim.CreateRequest) (uint32, error)
	StartContainer(ctx context.Context, id string) error
	Wait(ctx context.Context, id, execID string) (uint32, error)
	Kill(ctx context.Context, id, execID string, signal syscall.Signal) error
	Delete(ctx context.Context, id, execID string) error
	Shutdown(ctx context.Context, id string, now bool) error
	Exited() <-chan struct{}
	Close() error
}

var _ shimClient = (*shim.Shim)(nil)

const (
	// vmEndTimeout bounds the wait for the VM to end once its containers are
	// killed.
	vmEndTimeout = 15 * time.Second
	// shimExitTimeout bounds the wait for the shim to exit after its shutdown.
	// The shim removes its cgroup by first moving its processes to the root of
	// the cgroup hierarchy. Where that root is a cgroup with delegated
	// controllers, as the pod cgroup is after cgroup.Delegate, the move fails
	// and the shim retries instead of exiting. The cgroup kill that follows
	// ends it.
	shimExitTimeout = time.Second
	// shimTimeout bounds teardown's dealings with the shim, which leave the rest
	// of the caller's time for the removal of the VM's state.
	shimTimeout = 30 * time.Second
	// killWait bounds the wait for the VM to end after SIGKILL, so a broken VM
	// cannot hold a drain open.
	killWait = 5 * time.Second
)

// vmContainer is one container of an actor's VM.
type vmContainer struct {
	name string
	// id is the container's ID in the shim.
	id string
}

// vm is the Kata VM of one actor: its shim, and what the worker holds open for
// it. The first container boots the VM, and the VM ends with it.
type vm struct {
	shim shimClient
	// sandboxID is the actor UID. It names the VM's shim, cgroup leaf and state.
	sandboxID string
	// bundle is the bundle of the first container, which the shim runs in.
	bundle     string
	containers []vmContainer
	// closers are the log FIFOs and pipes, and the block rootfs images, which
	// are closed when the VM is torn down.
	closers []io.Closer
}

// sandboxContainer is the container whose end ends the VM.
func (v *vm) sandboxContainer() vmContainer { return v.containers[0] }

// bootPlan is what boot needs, from the request that starts an actor.
type bootPlan struct {
	attribution resources.ActorAttribution
	actorDirs   *ateworkerpb.ActorDirs
	containers  []*ateworkerpb.Container
	size        sizing.SandboxSize
	config      kataConfig
	configPath  string
	shimBinary  string
}

// ociBundlePath is the container's OCI bundle.
func ociBundlePath(actorDirs *ateworkerpb.ActorDirs, containerName string) string {
	return filepath.Join(actorDirs.GetOciBundleDir(), containerName)
}

// hasVolumes reports whether any container mounts a volume.
func hasVolumes(containers []*ateworkerpb.Container) bool {
	for _, c := range containers {
		if len(c.GetDurableDirVolumeMounts())+len(c.GetExternalVolumeMounts())+
			len(c.GetSystemInfoVolumeMounts())+len(c.GetImageVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// boot builds the containers' bundles, starts the shim in the actor's cgroup
// leaf, and creates and starts each container, the first of which boots the VM.
// On error v holds what exists so far, for teardown.
func (v *vm) boot(ctx context.Context, p *bootPlan, logger *actorlog.ActorLogger) error {
	uid := p.attribution.UID
	var creates []shim.CreateRequest
	for i, c := range p.containers {
		bootsVM := i == 0
		name := c.GetName()
		bundle := ociBundlePath(p.actorDirs, name)
		if err := imagecache.SetupBundleRootfs(bundle); err != nil {
			return fmt.Errorf("while composing %q rootfs: %w", name, err)
		}
		if err := actornet.WriteRootfsEtcFiles(filepath.Join(bundle, "rootfs"), ocispec.Hostname); err != nil {
			return fmt.Errorf("while writing the /etc files of %q: %w", name, err)
		}
		spec := ocispec.Build(ocispec.Options{ActorUID: uid, ActorDirs: p.actorDirs, Container: c})
		ocispec.ShapeKata(spec, ocispec.KataOptions{ActorUID: uid, ContainerName: name, BootsVM: bootsVM, Size: p.size})
		if err := ocispec.Save(bundle, spec); err != nil {
			return err
		}
		id := ocispec.KataContainerID(uid, name, bootsVM)
		v.containers = append(v.containers, vmContainer{name: name, id: id})

		req := shim.CreateRequest{ID: id, Bundle: bundle}
		if !p.config.sharesHostDirs() {
			block, err := newBlockRootfs(ctx, bundle)
			if err != nil {
				return fmt.Errorf("while building the block rootfs of %q: %w", name, err)
			}
			v.closers = append(v.closers, closerFunc(block.close))
			req.Rootfs = []shim.Mount{block.mount()}
		}
		var err error
		if req.Stdout, req.Stderr, err = v.startLogs(logger, p.attribution, bundle, name); err != nil {
			return err
		}
		creates = append(creates, req)
	}

	// QEMU's QMP socket is a file in the shim's working directory, the first
	// bundle. A QEMU of an earlier activation of the actor leaves it there, and
	// the bind of the next one fails on it.
	if err := os.Remove(filepath.Join(creates[0].Bundle, "qmp.sock")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// The shim and everything it starts run in the actor's cgroup leaf, which
	// the first container's cgroupsPath names too.
	leaf, err := cgroup.OpenActorLeaf(uid, p.size.MilliCPU)
	if err != nil {
		return fmt.Errorf("while creating the actor cgroup: %w", err)
	}
	defer leaf.Close()
	s, err := shim.Start(ctx, shim.Config{Binary: p.shimBinary, ConfigPath: p.configPath}, v.sandboxID, creates[0].Bundle, leaf.SysProcAttr())
	if err != nil {
		return err
	}
	v.shim = s

	for i, req := range creates {
		slog.InfoContext(ctx, "Creating Kata container", slog.String("container", v.containers[i].name), slog.String("hypervisor", p.config.hypervisor))
		if _, err := v.shim.Create(ctx, req); err != nil {
			return err
		}
		if err := v.shim.StartContainer(ctx, req.ID); err != nil {
			return err
		}
	}
	return nil
}

// startLogs makes the container's stdout and stderr FIFOs and copies what the
// shim writes to them into the actor's log, tagged with the container's name.
// It returns the FIFO paths.
func (v *vm) startLogs(logger *actorlog.ActorLogger, a resources.ActorAttribution, bundle, name string) (stdout, stderr string, _ error) {
	var paths [2]string
	for i, stream := range []string{"stdout", "stderr"} {
		path := filepath.Join(bundle, stream)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			return "", "", fmt.Errorf("while creating the %s FIFO of %q: %w", stream, name, err)
		}
		// Read-write, so the shim can open the write end before anything is
		// written, and so there is never an end of file.
		fifo, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			return "", "", err
		}
		pipe, err := logger.StartJSONLogPipe(a, name)
		if err != nil {
			_ = fifo.Close()
			return "", "", fmt.Errorf("while starting the log pipe of %q: %w", name, err)
		}
		go func() {
			// Read until teardown closes the FIFO.
			_, _ = io.Copy(pipe, fifo)
			_ = pipe.Close()
		}()
		v.closers = append(v.closers, fifo)
		paths[i] = path
	}
	return paths[0], paths[1], nil
}

// closerFunc makes a function an io.Closer.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// signalAll signals the init process of every container, and returns the
// errors joined. A container that is already gone is an error too.
func (v *vm) signalAll(ctx context.Context, signal syscall.Signal) error {
	var errs []error
	for _, c := range v.containers {
		if err := v.shim.Kill(ctx, c.id, "", signal); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// waitGone returns once the VM is over: the container that boots it has exited,
// or the shim has. It returns ctx's error if ctx ends first.
func (v *vm) waitGone(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		// A wait that fails means the shim or its container is gone, which is the
		// end of the VM too.
		_, _ = v.shim.Wait(ctx, v.sandboxContainer().id, "")
	}()
	select {
	case <-v.shim.Exited():
		return nil
	case <-exited:
		return ctx.Err()
	}
}

// stop asks the containers to exit with SIGTERM, waits for the VM to end until
// deadline, and then kills them. deadline is shared with the caller's other
// waits, so a caller that has spent most of it elsewhere leaves the containers
// what is left. After SIGKILL it waits afterKill for the VM to end.
func (v *vm) stop(ctx context.Context, deadline time.Time, afterKill time.Duration) error {
	if err := v.signalAll(ctx, syscall.SIGTERM); err != nil {
		slog.WarnContext(ctx, "Failed to send SIGTERM to the containers", slog.String("sandbox", v.sandboxID), slog.Any("err", err))
	}
	termCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	err := v.waitGone(termCtx)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	slog.WarnContext(ctx, "Grace period expired; killing the containers", slog.String("sandbox", v.sandboxID))
	if err := v.signalAll(ctx, syscall.SIGKILL); err != nil {
		slog.WarnContext(ctx, "Failed to send SIGKILL to the containers (they may have exited)", slog.String("sandbox", v.sandboxID), slog.Any("err", err))
	}
	killCtx, cancelKill := context.WithTimeout(ctx, afterKill)
	defer cancelKill()
	if err := v.waitGone(killCtx); err != nil {
		return fmt.Errorf("the containers of %s did not exit after SIGKILL: %w", v.sandboxID, err)
	}
	return nil
}

// teardown ends the VM and removes what it left on the host: the shim and its
// state, the cgroup leaf, and the files and devices the worker made for it. It
// tries every step and returns what failed. Killing the cgroup leaf, which needs
// Linux 5.14, leaves no process behind whatever the shim did.
func (v *vm) teardown(ctx context.Context, shimBinary string) error {
	var errs []error
	if v.shim != nil {
		v.stopShim(ctx)
	}
	errs = append(errs, removeHostState(ctx, v.sandboxID, shimBinary, v.bundle))
	if v.shim != nil {
		_ = v.shim.Close()
	}
	for _, c := range slices.Backward(v.closers) {
		if err := c.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// stopShim kills the containers, deletes them, and shuts the shim down, in the
// time shimTimeout allows. Each step is best effort: removeHostState clears
// what a shim that did not take part leaves.
func (v *vm) stopShim(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, shimTimeout)
	defer cancel()
	_ = v.signalAll(ctx, syscall.SIGKILL)
	waitCtx, cancelWait := context.WithTimeout(ctx, vmEndTimeout)
	_ = v.waitGone(waitCtx)
	cancelWait()
	for _, c := range slices.Backward(v.containers) {
		if err := v.shim.Delete(ctx, c.id, ""); err != nil {
			slog.DebugContext(ctx, "Failed to delete a Kata container", slog.String("container", c.name), slog.Any("err", err))
		}
	}
	if err := v.shim.Shutdown(ctx, v.sandboxID, true); err != nil {
		slog.DebugContext(ctx, "Failed to shut the shim down", slog.String("sandbox", v.sandboxID), slog.Any("err", err))
	}
	select {
	case <-v.shim.Exited():
	case <-time.After(shimExitTimeout):
		slog.InfoContext(ctx, "The shim did not exit; killing its cgroup", slog.String("sandbox", v.sandboxID))
	case <-ctx.Done():
	}
}

// shimDirs returns the directories runtime-rs keeps a sandbox's state in, named
// by the sandbox ID. The second is where a jailed Firecracker has its root.
func shimDirs(sandboxID string) []string {
	return []string{
		filepath.Join("/run/kata", sandboxID),
		filepath.Join("/run/kata/firecracker", sandboxID),
		filepath.Join("/run/kata-containers/shared/sandboxes", sandboxID),
	}
}

// removeHostState removes what a VM of the actor leaves on the host once the
// shim has been asked to stop: whatever still runs in its cgroup leaf, the shim's
// socket and state, and the leaf. It works from the actor UID alone, so it also
// clears the leftovers of a VM this process did not start. bundle is the bundle
// the shim started in.
func removeHostState(ctx context.Context, actorUID, shimBinary, bundle string) error {
	slog.DebugContext(ctx, "Removing the host state of an actor VM", slog.String("actorUID", actorUID))
	var errs []error
	if err := cgroup.KillActorLeaf(ctx, actorUID); err != nil {
		errs = append(errs, err)
	}
	if err := shim.Cleanup(ctx, shimBinary, actorUID, bundle); err != nil {
		errs = append(errs, err)
	}
	// What the shim's delete command leaves of a VM that was killed, which had
	// no chance to clean up itself, such as the shared tree of a hypervisor that
	// shares no filesystem or the jailer root of a Firecracker that never
	// stopped.
	for _, dir := range shimDirs(actorUID) {
		if err := removeTree(dir); err != nil {
			errs = append(errs, err)
		}
	}
	// The block rootfs images of the actor's bundles.
	if err := detachLoopsUnder(filepath.Dir(bundle)); err != nil {
		errs = append(errs, err)
	}
	if err := cgroup.RemoveActorLeaf(actorUID); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// removeTree unmounts what is mounted under dir, then removes dir. It removes
// nothing if a mount is left, since removing a bind mount's directory removes
// what the mount shows.
func removeTree(dir string) error {
	if err := imagecache.UnmountAllUnder(dir); err != nil {
		return err
	}
	mounted, err := imagecache.MountedUnder(dir)
	if err != nil {
		return err
	}
	if mounted {
		return fmt.Errorf("%s still has mounts under it", dir)
	}
	return os.RemoveAll(dir)
}

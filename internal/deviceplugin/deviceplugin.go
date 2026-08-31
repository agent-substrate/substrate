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

// Package deviceplugin advertises host device nodes (for example /dev/kvm) to
// kubelet as extended resources, so a worker pod can be granted just those
// devices instead of running privileged.
//
// Device access is gated by the cgroup v2 device controller, which denies by
// default before DAC is consulted: no capability, hostPath mount, or
// supplemental group grants it. Kubelet's device manager adds the narrow allow
// rule, emitting the device node and a matching cgroup allow for each device.
package deviceplugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// deviceCount is how many allocatable units of each device are advertised.
// These are shareable pseudo-devices, so the count means nothing except as an
// exhaustion limit; keep it far above any max-pods-per-node setting so device
// accounting never constrains scheduling.
const deviceCount = 4096

// registerTimeout bounds a single registration attempt against kubelet.
const registerTimeout = 30 * time.Second

// Extended resource names advertised by atelet and requested by worker pods.
// Both sides import these so the strings cannot drift apart.
const (
	// ResourceKVM grants /dev/kvm, which the micro-VM runtime needs to create a
	// VM (cloud-hypervisor fails with EPERM on VmCreate without it).
	ResourceKVM = "ate.dev/kvm"
)

// SandboxDevices are the host devices a sandbox runtime needs a grant for.
// atelet advertises whichever of these exist on its node.
//
// Only devices the container runtime denies by default belong here. The
// micro-VM runtime also opens /dev/net/tun, but that is in the runtime's
// default allow-list, so the worker gets it as an ordinary bind mount instead.
var SandboxDevices = []HostDevice{
	{ResourceName: ResourceKVM, Path: "/dev/kvm"},
}

// HostDevice is a device node advertised to kubelet under ResourceName.
type HostDevice struct {
	// ResourceName is the fully-qualified extended resource name pods request,
	// e.g. "ate.dev/kvm".
	ResourceName string
	// Path is the device node, e.g. "/dev/kvm". It is exposed to the container
	// at the same path.
	Path string
}

// Present reports whether the device node exists on the node as a character
// device. devRoot is where the node's /dev is mounted for inspection, our own
// container having a minimal /dev of its own; Allocate still reports Path,
// which kubelet resolves on the node.
func (d HostDevice) Present(devRoot string) bool {
	fi, err := os.Stat(d.resolve(devRoot))
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// resolve maps the device's host path into devRoot ("/dev/kvm" ->
// "<devRoot>/kvm").
func (d HostDevice) resolve(devRoot string) string {
	return filepath.Join(devRoot, strings.TrimPrefix(d.Path, "/dev/"))
}

// Available returns the subset of devs present on this node, looking under
// devRoot. atelet runs on every node, so advertising a resource only where its
// device exists keeps pods requesting it off nodes that cannot run them.
func Available(devs []HostDevice, devRoot string) []HostDevice {
	out := make([]HostDevice, 0, len(devs))
	for _, d := range devs {
		if d.Present(devRoot) {
			out = append(out, d)
		}
	}
	return out
}

// Plugin serves the kubelet device plugin API for a single HostDevice.
type Plugin struct {
	pluginapi.UnimplementedDevicePluginServer

	dev     HostDevice
	socket  string
	devices []*pluginapi.Device
}

var _ pluginapi.DevicePluginServer = (*Plugin)(nil)

// New builds a Plugin for dev. Call Run to serve it.
func New(dev HostDevice) *Plugin {
	devices := make([]*pluginapi.Device, 0, deviceCount)
	for i := range deviceCount {
		devices = append(devices, &pluginapi.Device{
			ID:     fmt.Sprintf("%s-%d", filepath.Base(dev.Path), i),
			Health: pluginapi.Healthy,
		})
	}
	return &Plugin{
		dev: dev,
		// One socket per resource, in the directory kubelet watches. The name is
		// derived from the resource so two plugins never collide.
		socket:  filepath.Join(pluginapi.DevicePluginPath, socketName(dev.ResourceName)),
		devices: devices,
	}
}

// socketName maps a resource name to a socket filename ("ate.dev/kvm" ->
// "ate.dev-kvm.sock").
func socketName(resourceName string) string {
	return filepath.Base(filepath.Dir(resourceName)) + "-" + filepath.Base(resourceName) + ".sock"
}

// Run serves the plugin and keeps it registered until ctx is cancelled. Kubelet
// forgets registered plugins when it restarts and signals that by recreating its
// socket, so re-register then or the resource disappears from the node.
func (p *Plugin) Run(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("while creating fsnotify watcher: %w", err)
	}
	defer watcher.Close()
	if err := watcher.Add(pluginapi.DevicePluginPath); err != nil {
		return fmt.Errorf("while watching %q: %w", pluginapi.DevicePluginPath, err)
	}

	for ctx.Err() == nil {
		srv, err := p.serveAndRegister(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Device plugin registration failed; retrying",
				slog.String("resource", p.dev.ResourceName), slog.Any("err", err))
			if !sleepCtx(ctx, 5*time.Second) {
				break
			}
			continue
		}
		slog.InfoContext(ctx, "Device plugin registered",
			slog.String("resource", p.dev.ResourceName), slog.String("device", p.dev.Path))

		p.waitForKubeletRestart(ctx, watcher)
		srv.Stop()
	}
	return ctx.Err()
}

// serveAndRegister starts the gRPC server on the plugin socket and registers it
// with kubelet. The returned server is stopped by the caller.
func (p *Plugin) serveAndRegister(ctx context.Context) (*grpc.Server, error) {
	// A leftover socket from a previous run would make Listen fail with EADDRINUSE.
	if err := os.Remove(p.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("while removing stale socket %q: %w", p.socket, err)
	}
	lis, err := net.Listen("unix", p.socket)
	if err != nil {
		return nil, fmt.Errorf("while listening on %q: %w", p.socket, err)
	}

	srv := grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(srv, p)
	go func() {
		if err := srv.Serve(lis); err != nil {
			slog.ErrorContext(ctx, "Device plugin server stopped",
				slog.String("resource", p.dev.ResourceName), slog.Any("err", err))
		}
	}()

	if err := p.register(ctx); err != nil {
		srv.Stop()
		return nil, err
	}
	return srv, nil
}

// register tells kubelet which resource this plugin's socket serves.
func (p *Plugin) register(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	conn, err := grpc.NewClient("unix://"+pluginapi.KubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("while connecting to kubelet: %w", err)
	}
	defer conn.Close()

	_, err = pluginapi.NewRegistrationClient(conn).Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(p.socket),
		ResourceName: p.dev.ResourceName,
	})
	if err != nil {
		return fmt.Errorf("while registering %q with kubelet: %w", p.dev.ResourceName, err)
	}
	return nil
}

// waitForKubeletRestart blocks until kubelet recreates its socket (meaning it
// restarted and dropped our registration) or ctx is cancelled.
func (p *Plugin) waitForKubeletRestart(ctx context.Context, watcher *fsnotify.Watcher) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Name == pluginapi.KubeletSocket && event.Has(fsnotify.Create) {
				slog.InfoContext(ctx, "Kubelet restarted; re-registering device plugin",
					slog.String("resource", p.dev.ResourceName))
				return
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			slog.ErrorContext(ctx, "Device plugin socket watch error",
				slog.String("resource", p.dev.ResourceName), slog.Any("err", err))
		}
	}
}

// GetDevicePluginOptions implements the device plugin API; the defaults are
// correct here.
func (p *Plugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// ListAndWatch streams the device list to kubelet. The set is static, so send it
// once and hold the stream open until shutdown.
func (p *Plugin) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices}); err != nil {
		return fmt.Errorf("while sending device list: %w", err)
	}
	<-stream.Context().Done()
	return nil
}

// Allocate returns the device node for each requested container. Kubelet turns
// each DeviceSpec into a device node plus a matching cgroup allow, so the
// container gets this device and no other.
func (p *Plugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	resp := &pluginapi.AllocateResponse{
		ContainerResponses: make([]*pluginapi.ContainerAllocateResponse, 0, len(req.GetContainerRequests())),
	}
	for range req.GetContainerRequests() {
		resp.ContainerResponses = append(resp.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: []*pluginapi.DeviceSpec{{
				HostPath:      p.dev.Path,
				ContainerPath: p.dev.Path,
				// Read/write, but not mknod: the container is handed this node,
				// not the ability to mint new ones.
				Permissions: "rw",
			}},
		})
	}
	return resp, nil
}

// sleepCtx waits for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

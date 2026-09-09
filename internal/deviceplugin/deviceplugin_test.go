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

package deviceplugin

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Each resource gets its own socket in the directory kubelet watches, and two
// resources must never collide on one filename.
func TestSocketPathsAreDistinctAndInKubeletDir(t *testing.T) {
	kvm := New(HostDevice{ResourceName: "ate.dev/kvm", Path: "/dev/kvm"})
	other := New(HostDevice{ResourceName: "ate.dev/other", Path: "/dev/other"})

	if kvm.socket == other.socket {
		t.Errorf("sockets collide: %q", kvm.socket)
	}
	for _, p := range []*Plugin{kvm, other} {
		if got := filepath.Dir(p.socket) + "/"; got != pluginapi.DevicePluginPath {
			t.Errorf("socket %q is not in %q", p.socket, pluginapi.DevicePluginPath)
		}
		if filepath.Ext(p.socket) != ".sock" {
			t.Errorf("socket %q should end in .sock", p.socket)
		}
	}
}

// The advertised count must stay far above any max-pods-per-node setting, or
// kubelet would report the resource exhausted and stop scheduling workers.
func TestDeviceCountExceedsMaxPodsPerNode(t *testing.T) {
	// The largest supported max-pods values are in the low hundreds.
	const highestRealisticMaxPods = 256
	if deviceCount <= highestRealisticMaxPods {
		t.Errorf("deviceCount = %d, want well above %d", deviceCount, highestRealisticMaxPods)
	}
	p := New(HostDevice{ResourceName: "ate.dev/kvm", Path: "/dev/kvm"})
	if len(p.devices) != deviceCount {
		t.Errorf("advertised %d devices, want %d", len(p.devices), deviceCount)
	}
	seen := make(map[string]bool, len(p.devices))
	for _, d := range p.devices {
		if seen[d.ID] {
			t.Fatalf("duplicate device ID %q", d.ID)
		}
		seen[d.ID] = true
		if d.Health != pluginapi.Healthy {
			t.Errorf("device %q health = %q, want %q", d.ID, d.Health, pluginapi.Healthy)
		}
	}
}

// Allocate must hand back exactly the one device node, read/write but not
// mknod, once per requested container.
func TestAllocateReturnsOnlyTheRequestedDevice(t *testing.T) {
	p := New(HostDevice{ResourceName: "ate.dev/kvm", Path: "/dev/kvm"})

	resp, err := p.Allocate(context.Background(), &pluginapi.AllocateRequest{
		ContainerRequests: []*pluginapi.ContainerAllocateRequest{
			{DevicesIds: []string{"kvm-0"}},
			{DevicesIds: []string{"kvm-1"}},
		},
	})
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if got := len(resp.GetContainerResponses()); got != 2 {
		t.Fatalf("got %d container responses, want 2", got)
	}
	for i, cr := range resp.GetContainerResponses() {
		devs := cr.GetDevices()
		if len(devs) != 1 {
			t.Fatalf("container %d: got %d devices, want 1", i, len(devs))
		}
		d := devs[0]
		if d.GetHostPath() != "/dev/kvm" || d.GetContainerPath() != "/dev/kvm" {
			t.Errorf("container %d: paths = %q -> %q, want /dev/kvm both", i, d.GetHostPath(), d.GetContainerPath())
		}
		if d.GetPermissions() != "rw" {
			t.Errorf("container %d: permissions = %q, want rw (no mknod)", i, d.GetPermissions())
		}
	}
}

// A node without the device must not advertise the resource, so the scheduler
// keeps pods that need it off that node.
func TestAvailableFiltersToPresentDevices(t *testing.T) {
	// /dev/null is a character device everywhere the tests run; resolving it
	// under "/" mirrors how the node's /dev is mounted at a different root.
	devs := []HostDevice{
		{ResourceName: "ate.dev/null", Path: "/dev/null"},
		{ResourceName: "ate.dev/absent", Path: "/dev/definitely-not-a-device"},
	}
	got := Available(devs, "/dev")
	if len(got) != 1 || got[0].ResourceName != "ate.dev/null" {
		t.Fatalf("Available() = %+v, want only ate.dev/null", got)
	}
}

// A regular file at the device path is not a device; advertising it would grant
// a meaningless resource.
func TestPresentRejectsNonDevice(t *testing.T) {
	regular := filepath.Join(t.TempDir(), "kvm")
	if err := os.WriteFile(regular, nil, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if (HostDevice{ResourceName: "ate.dev/kvm", Path: "/dev/kvm"}).Present(filepath.Dir(regular)) {
		t.Errorf("Present() = true for a regular file at %q", regular)
	}
}

// ListAndWatch sends the full device list, then holds the stream open until the
// stream context is cancelled (kubelet keeps it open for the plugin's lifetime).
func TestListAndWatchSendsDevicesThenBlocks(t *testing.T) {
	p := New(HostDevice{ResourceName: "ate.dev/kvm", Path: "/dev/kvm"})
	ctx, cancel := context.WithCancel(context.Background())
	stream := &fakeListAndWatchStream{ctx: ctx, sent: make(chan []*pluginapi.Device, 1)}

	done := make(chan error, 1)
	go func() { done <- p.ListAndWatch(&pluginapi.Empty{}, stream) }()

	// The first send happens immediately; wait for it rather than sleeping.
	select {
	case got := <-stream.sent:
		if len(got) != deviceCount {
			t.Errorf("sent %d devices, want %d", len(got), deviceCount)
		}
	case err := <-done:
		t.Fatalf("ListAndWatch returned before sending: %v", err)
	}

	select {
	case err := <-done:
		t.Fatalf("ListAndWatch returned while the stream was open: %v", err)
	default:
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("ListAndWatch() after cancel = %v, want nil", err)
	}
}

type fakeListAndWatchStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan []*pluginapi.Device
}

func (f *fakeListAndWatchStream) Context() context.Context { return f.ctx }

func (f *fakeListAndWatchStream) Send(resp *pluginapi.ListAndWatchResponse) error {
	f.sent <- resp.GetDevices()
	return nil
}

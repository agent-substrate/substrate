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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	sandboxapi "github.com/agent-substrate/substrate/internal/sandboxd/sandboxapi"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseBootstrap(t *testing.T) {
	got, err := parseBootstrap([]byte(`{"version":3,"protocol":"ttrpc","address":"unix:///run/containerd/s/x"}`))
	if err != nil || got.Address != "unix:///run/containerd/s/x" {
		t.Fatalf("parseBootstrap() = %#v, %v", got, err)
	}
	for _, bad := range []string{
		`{"version":2,"protocol":"ttrpc","address":"unix:///x"}`,
		`{"version":3,"protocol":"grpc","address":"unix:///x"}`,
		`not-json`,
	} {
		if _, err := parseBootstrap([]byte(bad)); err == nil {
			t.Errorf("parseBootstrap(%q) succeeded", bad)
		}
	}
}

func TestValidateRestoreResult(t *testing.T) {
	want := []*sandboxapi.RestoreContainer{{Name: "app", CheckpointKey: "app", Id: "new-app"}}
	good := &sandboxapi.RestoreResponse{
		Containers: []*sandboxapi.RestoredContainer{{
			Name:            "app",
			TaskRestoreMode: sandboxapi.TaskRestoreMode_TASK_RESTORE_MODE_VM_RESTORED,
		}},
	}
	if err := ValidateRestoreResult(want, good); err != nil {
		t.Fatal(err)
	}
	good.Containers[0].Name = "wrong"
	if err := ValidateRestoreResult(want, good); err == nil {
		t.Fatal("mismatched container name accepted")
	}
	good.Containers[0].Name = "app"
	good.Containers[0].TaskRestoreMode = sandboxapi.TaskRestoreMode_TASK_RESTORE_MODE_UNSPECIFIED
	if err := ValidateRestoreResult(want, good); err == nil {
		t.Fatal("unspecified task restore mode accepted")
	}
}

func TestValidateCheckpoint(t *testing.T) {
	dir := t.TempDir()
	state := []byte("vm-state")
	rootfs := []byte("rootfs")
	if err := os.Mkdir(filepath.Join(dir, "storage"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "vm"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "vm", "state"), state, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "storage", "rootfs-base.raw"), rootfs, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := `{
  "format_version": 10,
  "hypervisor": "qemu",
  "sandbox_id": "source",
  "tasks": [{"checkpoint_key":"app","task_id":"source-app"}],
  "rootfs": [{"checkpoint_key":"app","file":"storage/rootfs-base.raw","format":"raw","device_id":"rootfs0","index":0,"driver_option":"blk","virt_path":"/dev/vda","is_direct":null,"discard_unmap":false,"aio":"iouring","queue_size":128,"num_queues":1,"logical_sector_size":0,"physical_sector_size":0,"serial_override":"","size":6,"sha256":"` + sum(rootfs) + `"}],
  "block_volumes": [],
  "network_macs": [],
  "memory_hotplug_size": 1073741824,
  "vcpu_hotplug_count": 1,
  "vm_state": [{"file":"vm/state","size":8,"sha256":"` + sum(state) + `"}],
  "runtime_fingerprint": "fingerprint"
}`
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCheckpoint(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "vm", "state"), []byte("corrupt!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCheckpoint(dir); err == nil {
		t.Fatal("corrupt state accepted")
	}
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type startSandboxService struct {
	sandboxapi.TTRPCSandboxService
	request  *sandboxapi.StartSandboxRequest
	response *sandboxapi.StartSandboxResponse
	err      error
}

func (f *startSandboxService) StartSandbox(_ context.Context, req *sandboxapi.StartSandboxRequest) (*sandboxapi.StartSandboxResponse, error) {
	f.request = req
	return f.response, f.err
}

func TestSandboxControllerStart(t *testing.T) {
	newController := func(service sandboxapi.TTRPCSandboxService) *SandboxController {
		shims := &ShimManager{instances: map[string]*Instance{
			"sandbox-1": {ID: "sandbox-1", Sandbox: service},
		}}
		return NewSandboxController(shims, NewTaskManager(shims))
	}

	t.Run("passes sandbox id and accepts complete response", func(t *testing.T) {
		service := &startSandboxService{response: &sandboxapi.StartSandboxResponse{
			Pid: 42, CreatedAt: timestamppb.Now(),
		}}
		if err := newController(service).Start(context.Background(), "sandbox-1"); err != nil {
			t.Fatal(err)
		}
		if service.request == nil || service.request.GetSandboxId() != "sandbox-1" {
			t.Fatalf("StartSandbox request = %+v", service.request)
		}
	})

	for _, tc := range []struct {
		name     string
		response *sandboxapi.StartSandboxResponse
	}{
		{name: "missing pid", response: &sandboxapi.StartSandboxResponse{CreatedAt: timestamppb.Now()}},
		{name: "missing creation time", response: &sandboxapi.StartSandboxResponse{Pid: 42}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &startSandboxService{response: tc.response}
			if err := newController(service).Start(context.Background(), "sandbox-1"); err == nil {
				t.Fatal("Start accepted an incomplete response")
			}
		})
	}

	t.Run("preserves shim error", func(t *testing.T) {
		shimErr := errors.New("shim start failed")
		service := &startSandboxService{err: shimErr}
		err := newController(service).Start(context.Background(), "sandbox-1")
		if !errors.Is(err, shimErr) {
			t.Fatalf("Start error = %v, want wrapped shim error", err)
		}
	})
}

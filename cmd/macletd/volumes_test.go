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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
)

type fakeMountOperations struct {
	mounted map[string]bool
	mounts  []string
	unmount []string
}

func (f *fakeMountOperations) Mounted(_ context.Context, target string) (bool, error) {
	return f.mounted[target], nil
}

func (f *fakeMountOperations) MountNFS(_ context.Context, remote, target string) error {
	f.mounts = append(f.mounts, remote+" -> "+target)
	f.mounted[target] = true
	return nil
}

func (f *fakeMountOperations) Unmount(_ context.Context, target string) error {
	f.unmount = append(f.unmount, target)
	delete(f.mounted, target)
	return nil
}

func nfsVolume(name, mountPath string) *hostruntimepb.DurableVolume {
	return &hostruntimepb.DurableVolume{
		Name: name, MountPath: mountPath, VolumeId: "volume-" + name, Driver: nfsCSIDriver,
		VolumeContext: map[string]string{"server": "nfs.internal", "share": "/exports", "subdir": name},
	}
}

func TestNFSRemote(t *testing.T) {
	for _, tc := range []struct {
		name    string
		volume  *hostruntimepb.DurableVolume
		want    string
		wantErr bool
	}{
		{name: "DNS with subdirectory", volume: nfsVolume("data", "/workspace"), want: "nfs.internal:/exports/data"},
		{name: "IPv6", volume: &hostruntimepb.DurableVolume{Name: "data", Driver: nfsCSIDriver, VolumeContext: map[string]string{"server": "2001:db8::1", "share": "/exports"}}, want: "[2001:db8::1]:/exports"},
		{name: "unsupported driver", volume: &hostruntimepb.DurableVolume{Name: "data", Driver: "block.csi.example", VolumeContext: map[string]string{"server": "nfs.internal", "share": "/exports"}}, wantErr: true},
		{name: "path traversal", volume: &hostruntimepb.DurableVolume{Name: "data", Driver: nfsCSIDriver, VolumeContext: map[string]string{"server": "nfs.internal", "share": "/exports", "subdir": "../other"}}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := nfsRemote(tc.volume)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("nfsRemote() = %q, %v, want %q, error=%v", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestNFSVolumeStageIsIdempotentAndUnstages(t *testing.T) {
	root := t.TempDir()
	mounts := &fakeMountOperations{mounted: map[string]bool{}}
	stager := &nfsVolumeStager{root: root, mounts: mounts}
	request := []*hostruntimepb.DurableVolume{
		nfsVolume("data", "/workspace"),
		nfsVolume("data", "/var/lib/workspace"),
	}

	first, err := stager.Stage(context.Background(), "actor-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].HostPath != first[1].HostPath || first[0].Tag == first[1].Tag {
		t.Fatalf("staged volumes = %#v", first)
	}
	wantTarget := filepath.Join(root, "actor-1", "data")
	if want := []string{"nfs.internal:/exports/data -> " + wantTarget}; !reflect.DeepEqual(mounts.mounts, want) {
		t.Fatalf("mount calls = %v, want %v", mounts.mounts, want)
	}
	second, err := stager.Stage(context.Background(), "actor-1", request)
	if err != nil || !reflect.DeepEqual(first, second) || len(mounts.mounts) != 1 {
		t.Fatalf("second Stage = %#v, %v; mount calls = %v", second, err, mounts.mounts)
	}

	conflict := nfsVolume("data", "/workspace")
	conflict.VolumeId = "different"
	if _, err := stager.Stage(context.Background(), "actor-1", []*hostruntimepb.DurableVolume{conflict}); err == nil {
		t.Fatal("Stage accepted conflicting persisted volume identity")
	}
	if err := stager.Unstage(context.Background(), "actor-1"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mounts.unmount, []string{wantTarget}) {
		t.Fatalf("unmount calls = %v", mounts.unmount)
	}
	if _, err := os.Stat(filepath.Join(root, "actor-1")); !os.IsNotExist(err) {
		t.Fatalf("stage directory remains: %v", err)
	}
}

func TestWriteVMVolumeConfigSeparatesHostPaths(t *testing.T) {
	bundle := t.TempDir()
	volumes := []stagedVolume{{Name: "data", MountPath: "/workspace", Tag: "ate-data", HostPath: "/private/nfs/data"}}
	if err := writeVMVolumeConfig(bundle, volumes); err != nil {
		t.Fatal(err)
	}
	host, err := os.ReadFile(filepath.Join(bundle, vmVolumeConfigName))
	if err != nil || !strings.Contains(string(host), "/private/nfs/data") {
		t.Fatalf("host config = %q, %v", host, err)
	}
	guest, err := os.ReadFile(filepath.Join(bundle, guestConfigDirectory, "volumes.json"))
	if err != nil || strings.Contains(string(guest), "/private/nfs/data") || !strings.Contains(string(guest), "/workspace") {
		t.Fatalf("guest config = %q, %v", guest, err)
	}
	if err := writeVMVolumeConfig(bundle, nil); err != nil {
		t.Fatal(err)
	}
}

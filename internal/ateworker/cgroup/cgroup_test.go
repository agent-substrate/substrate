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

package cgroup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSetWorkerCPUWeight(t *testing.T) {
	root := t.TempDir()
	leaf := filepath.Join(root, workerLeaf)
	if err := os.Mkdir(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	// No cpu controller, no cpu.weight: nothing to write.
	if err := setWorkerCPUWeight(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(leaf, "cpu.weight")); !os.IsNotExist(err) {
		t.Errorf("cpu.weight was created: %v", err)
	}
	if err := os.WriteFile(filepath.Join(leaf, "cpu.weight"), []byte("100\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := setWorkerCPUWeight(root); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(leaf, "cpu.weight"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != workerCPUWeight {
		t.Errorf("cpu.weight = %q, want %q", got, workerCPUWeight)
	}
}

func TestParseCgroupPath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "private namespace", in: "0::/\n", want: "/"},
		{name: "host namespace", in: "0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n", want: "/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope"},
		{name: "hybrid hierarchy", in: "12:pids:/\n0::/\n", want: "/"},
		{name: "cgroup v1 only", in: "12:pids:/\n11:memory:/\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCgroupPath(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseCgroupPath() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("parseCgroupPath() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDelegateRejectsSharedNamespace(t *testing.T) {
	dir := t.TempDir()
	procCgroup := filepath.Join(dir, "cgroup")
	if err := os.WriteFile(procCgroup, []byte("0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := delegate(context.Background(), procCgroup, root); err == nil {
		t.Fatal("delegate() succeeded outside a private cgroup namespace")
	}
	if _, err := os.Stat(filepath.Join(root, workerLeaf)); !os.IsNotExist(err) {
		t.Errorf("delegate() created the worker leaf anyway (stat err: %v)", err)
	}
}

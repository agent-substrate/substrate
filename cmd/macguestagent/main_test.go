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
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type fakeMounts struct {
	mounted map[string]bool
	calls   []string
}

func (f *fakeMounts) Mounted(_ context.Context, target string) (bool, error) {
	return f.mounted[target], nil
}

func (f *fakeMounts) MountVirtioFS(_ context.Context, tag, target string) error {
	f.calls = append(f.calls, tag+" -> "+target)
	f.mounted[target] = true
	return nil
}

func TestReconcileMountsConfigBeforeVolumesAndIsIdempotent(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(root, "config")
	workspace := filepath.Join(root, "workspace")
	mounts := &fakeMounts{mounted: map[string]bool{}}
	r := &reconciler{configMount: config, mounts: mounts}

	// The first pass can mount the fixed configuration share but cannot read
	// its contents until the fixture simulates what the share contains.
	if err := r.reconcile(context.Background()); err == nil {
		t.Fatal("reconcile succeeded without volumes.json")
	}
	if want := []string{"ate-config -> " + config}; !reflect.DeepEqual(mounts.calls, want) {
		t.Fatalf("mount calls = %v, want %v", mounts.calls, want)
	}
	b, err := json.Marshal([]guestVolume{{Name: "data", MountPath: workspace, Tag: "ate-data"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "volumes.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ate-config -> " + config, "ate-data -> " + workspace}; !reflect.DeepEqual(mounts.calls, want) {
		t.Fatalf("mount calls = %v, want %v", mounts.calls, want)
	}
	if err := r.reconcile(context.Background()); err != nil || len(mounts.calls) != 2 {
		t.Fatalf("idempotent reconcile = %v, calls %v", err, mounts.calls)
	}
}

func TestReadVolumesRejectsNestedAndUnknownConfiguration(t *testing.T) {
	for _, raw := range []string{
		`[{"name":"data","mountPath":"/workspace","tag":"ate-data"},{"name":"cache","mountPath":"/workspace/cache","tag":"ate-cache"}]`,
		`[{"name":"data","mountPath":"/workspace","tag":"ate-data","secret":"value"}]`,
	} {
		file := filepath.Join(t.TempDir(), "volumes.json")
		if err := os.WriteFile(file, []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readVolumes(file); err == nil {
			t.Fatalf("readVolumes accepted %s", raw)
		}
	}
}

func TestRequireEmptyRefusesToCoverData(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "important"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireEmpty(directory); err == nil {
		t.Fatal("requireEmpty accepted non-empty mount point")
	}
}

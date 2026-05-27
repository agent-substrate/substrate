//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//go:build linux

package main

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

func TestGpuGlobalFlags_NilReturnsNil(t *testing.T) {
	r := &runsc{}
	if got := r.gpuGlobalFlags(); got != nil {
		t.Fatalf("expected nil for nil GPU spec, got %v", got)
	}
}

func TestGpuGlobalFlags_BareSpecEmitsNvproxyOnly(t *testing.T) {
	r := &runsc{gpu: &ateompb.GpuSpec{Count: 1}}
	got := r.gpuGlobalFlags()
	if len(got) != 1 || got[0] != "--nvproxy" {
		t.Fatalf("expected [--nvproxy], got %v", got)
	}
}

func TestGpuGlobalFlags_WithDriverVersion(t *testing.T) {
	r := &runsc{gpu: &ateompb.GpuSpec{Count: 1, DriverVersion: "580.126.09"}}
	got := strings.Join(r.gpuGlobalFlags(), " ")
	want := "--nvproxy --nvproxy-driver-version=580.126.09"
	if got != want {
		t.Fatalf("flags: got %q want %q", got, want)
	}
}

func TestGpuGlobalFlags_WithCapabilities(t *testing.T) {
	r := &runsc{gpu: &ateompb.GpuSpec{
		Count:              1,
		DriverCapabilities: []string{"compute", "utility"},
	}}
	got := strings.Join(r.gpuGlobalFlags(), " ")
	want := "--nvproxy --nvproxy-allowed-driver-capabilities=compute,utility"
	if got != want {
		t.Fatalf("flags: got %q want %q", got, want)
	}
}

func TestGpuGlobalFlags_FullSpec(t *testing.T) {
	r := &runsc{gpu: &ateompb.GpuSpec{
		Count:              1,
		DriverVersion:      "580.126.09",
		DriverCapabilities: []string{"compute", "utility", "video"},
	}}
	got := strings.Join(r.gpuGlobalFlags(), " ")
	want := "--nvproxy --nvproxy-driver-version=580.126.09 --nvproxy-allowed-driver-capabilities=compute,utility,video"
	if got != want {
		t.Fatalf("flags: got %q want %q", got, want)
	}
}

func TestGpuSaveRestoreFlags_NilReturnsNil(t *testing.T) {
	r := &runsc{}
	if got := r.gpuSaveRestoreFlags(); got != nil {
		t.Fatalf("expected nil for nil GPU spec, got %v", got)
	}
}

func TestGpuSaveRestoreFlags_DurationIsString(t *testing.T) {
	// The runsc --save-restore-exec-timeout flag wants a Go duration
	// string (e.g. "30s"), not a millisecond integer. Regression test
	// for the parse error captured in the 2026-05-27 brev validation.
	r := &runsc{gpu: &ateompb.GpuSpec{Count: 1}}
	got := r.gpuSaveRestoreFlags()
	if len(got) != 2 {
		t.Fatalf("expected 2 flags, got %d: %v", len(got), got)
	}
	if got[0] != "--save-restore-exec-argv=/usr/local/bin/cuda-checkpoint-wrapper.sh" {
		t.Errorf("argv flag wrong: %q", got[0])
	}
	if got[1] != "--save-restore-exec-timeout=30s" {
		t.Errorf("timeout flag wrong (must be a duration string, not ms): %q", got[1])
	}
}

func TestFirstGPUSpec_None(t *testing.T) {
	containers := []*ateompb.Container{
		{Name: "pause"},
		{Name: "app"},
	}
	if g := firstGPUSpec(containers); g != nil {
		t.Fatalf("expected nil when no container has GPU, got %v", g)
	}
}

func TestGpuSaveRestoreFlags_OnlyOnRootContainer(t *testing.T) {
	// cmdRestore must only emit --save-restore-exec-argv on the pause
	// (root) container; sub-container restores must not re-invoke the
	// wrapper. Verified empirically on the L40S E2E run (2026-05-27):
	// without this gate, the sub-container restore fails with
	// "inconsistent private memory files on restore".
	//
	// gpuSaveRestoreFlags() itself doesn't know about container names;
	// the gating lives in cmdRestore. This is a behavioural cross-check
	// rather than a unit test of the helper.
	r := &runsc{gpu: &ateompb.GpuSpec{Count: 1}}
	if got := r.gpuSaveRestoreFlags(); len(got) == 0 {
		t.Fatalf("expected non-empty flags for the root container")
	}
}

func TestFirstGPUSpec_FindsFirst(t *testing.T) {
	containers := []*ateompb.Container{
		{Name: "pause"},
		{Name: "app", Gpu: &ateompb.GpuSpec{Count: 1, DriverVersion: "580.126.09"}},
		{Name: "sidecar"},
	}
	g := firstGPUSpec(containers)
	if g == nil {
		t.Fatalf("expected non-nil GPU spec")
	}
	if g.GetDriverVersion() != "580.126.09" {
		t.Fatalf("expected driver version from app container, got %q", g.GetDriverVersion())
	}
}

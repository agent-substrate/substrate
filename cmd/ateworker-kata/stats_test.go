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
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// writeLeaf writes the files of a cgroup leaf. A nil entry leaves the file out.
func writeLeaf(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadCgroup(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		want    cgroupUsage
		wantSet uint64
		wantErr error
	}{
		{
			name: "every file",
			files: map[string]string{
				"memory.current": "1000\n",
				"memory.peak":    "1500\n",
				"memory.stat":    "anon 10\ninactive_file 300\nactive_file 7\n",
				"cpu.stat":       "usage_usec 4242\nuser_usec 4000\n",
			},
			want:    cgroupUsage{memoryCurrent: 1000, memoryPeak: 1500, inactiveFile: 300, cpuUsageUsec: 4242},
			wantSet: 700,
		},
		{
			name:    "only memory.current: the rest reads as zero",
			files:   map[string]string{"memory.current": "1000\n"},
			want:    cgroupUsage{memoryCurrent: 1000},
			wantSet: 1000,
		},
		{
			name:    "cache above the total saturates",
			files:   map[string]string{"memory.current": "100\n", "memory.stat": "inactive_file 300\n"},
			want:    cgroupUsage{memoryCurrent: 100, inactiveFile: 300},
			wantSet: 0,
		},
		{
			name:    "no memory.current",
			files:   map[string]string{"memory.peak": "1\n"},
			wantErr: fs.ErrNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "leaf")
			writeLeaf(t, dir, tt.files)
			got, err := readCgroup(dir)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("readCgroup() = %v, want %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got != tt.want {
				t.Errorf("readCgroup() = %+v, want %+v", got, tt.want)
			}
			if got.workingSet() != tt.wantSet {
				t.Errorf("workingSet() = %d, want %d", got.workingSet(), tt.wantSet)
			}
		})
	}
}

func TestReadCgroupMissingDirectory(t *testing.T) {
	if _, err := readCgroup(filepath.Join(t.TempDir(), "gone")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("readCgroup() = %v, want a not-exist error", err)
	}
}

func TestReadCgroupMalformedMemoryCurrent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "leaf")
	writeLeaf(t, dir, map[string]string{"memory.current": "lots\n"})
	if _, err := readCgroup(dir); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Errorf("readCgroup() = %v, want a parse error that is not a missing leaf", err)
	}
}

func TestGetWorkloadStats(t *testing.T) {
	s, _ := newTestService(t, 2)
	// Identity of an actor the worker does not host.
	const stranger = "1b1a6a3c-7d5e-4f8a-9c2d-3e4f5a6b7c8d"

	if _, err := s.GetWorkloadStats(t.Context(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: stranger}); apierror.Code(err) != codes.NotFound {
		t.Errorf("GetWorkloadStats() for a stranger = %v, want NotFound", err)
	}
	if _, err := s.GetWorkloadStats(t.Context(), &ateworkerpb.GetWorkloadStatsRequest{}); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("GetWorkloadStats() without an actor = %v, want InvalidArgument", err)
	}

	if _, err := s.hostActor(t.Context(), testAttribution(testUID)); err != nil {
		t.Fatal(err)
	}
	// Hosted, but the VM has no cgroup yet.
	if _, err := s.GetWorkloadStats(t.Context(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testUID}); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("GetWorkloadStats() while booting = %v, want FailedPrecondition", err)
	}

	writeLeaf(t, filepath.Join(s.cgroupRoot, testUID), map[string]string{
		"memory.current": "1000\n", "memory.peak": "1500\n",
		"memory.stat": "inactive_file 300\n", "cpu.stat": "usage_usec 77\n",
	})
	resp, err := s.GetWorkloadStats(t.Context(), &ateworkerpb.GetWorkloadStatsRequest{ActorUid: testUID})
	if err != nil {
		t.Fatalf("GetWorkloadStats() = %v", err)
	}
	got := resp.GetSample()
	want := &ateworkerpb.WorkloadStatsSample{
		Atespace:              "team",
		ActorName:             "agent",
		ActorUid:              testUID,
		SandboxClass:          "kata",
		Source:                ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		MemoryCurrentBytes:    1000,
		MemoryPeakBytes:       1500,
		MemoryWorkingSetBytes: 700,
		CpuUsageUsec:          77,
	}
	opts := []cmp.Option{protocmp.Transform(), protocmp.IgnoreFields(&ateworkerpb.WorkloadStatsSample{}, "observed_at_unix_nano", "epoch_unix_nano")}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("sample mismatch (-want +got):\n%s", diff)
	}
	if got.GetEpochUnixNano() == 0 || got.GetObservedAtUnixNano() == 0 {
		t.Errorf("sample = %v, want an epoch and a time", got)
	}
}

func TestGetActiveWorkloadStats(t *testing.T) {
	s, _ := newTestService(t, 2)
	empty, err := s.GetActiveWorkloadStats(t.Context(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil || len(empty.GetSamples()) != 0 {
		t.Fatalf("GetActiveWorkloadStats() on an available worker = %v, %v, want no samples", empty, err)
	}

	measured, pending := testUID, "1b1a6a3c-7d5e-4f8a-9c2d-3e4f5a6b7c8d"
	for _, uid := range []string{measured, pending} {
		if _, err := s.hostActor(t.Context(), testAttribution(uid)); err != nil {
			t.Fatal(err)
		}
	}
	writeLeaf(t, filepath.Join(s.cgroupRoot, measured), map[string]string{"memory.current": "1000\n"})

	resp, err := s.GetActiveWorkloadStats(t.Context(), &ateworkerpb.GetActiveWorkloadStatsRequest{})
	if err != nil {
		t.Fatalf("GetActiveWorkloadStats() = %v", err)
	}
	sources := map[string]ateworkerpb.StatsSource{}
	for _, sample := range resp.GetSamples() {
		sources[sample.GetActorUid()] = sample.GetSource()
		if sample.GetSandboxClass() != "kata" {
			t.Errorf("sample of %s has class %q, want kata", sample.GetActorUid(), sample.GetSandboxClass())
		}
	}
	want := map[string]ateworkerpb.StatsSource{
		measured: ateworkerpb.StatsSource_STATS_SOURCE_CGROUP,
		// Nothing to read yet: attributed, and not measured.
		pending: ateworkerpb.StatsSource_STATS_SOURCE_UNSPECIFIED,
	}
	if diff := cmp.Diff(want, sources); diff != "" {
		t.Errorf("sources mismatch (-want +got):\n%s", diff)
	}
}

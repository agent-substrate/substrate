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

package hardware

import (
	"runtime"
	"testing"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

func TestProbeHost(t *testing.T) {
	compat := ProbeHost()
	if got := compat.GetSchemaVersion(); got != SchemaVersionV1 {
		t.Errorf("ProbeHost() schema_version = %q, want %q", got, SchemaVersionV1)
	}
	attrs := compat.GetAttributes()
	if len(attrs) != 1 || attrs[0].GetKey() != AttrArchitecture || attrs[0].GetValue() != runtime.GOARCH {
		t.Errorf("ProbeHost() attributes = %v, want [{%s: %s}]", attrs, AttrArchitecture, runtime.GOARCH)
	}
}

func TestMatches(t *testing.T) {
	runtimeWith := func(class, ver string, attrs ...*ateapipb.AttributeEntry) *ateapipb.SandboxRuntime {
		return &ateapipb.SandboxRuntime{
			SandboxClass: class,
			Version: &ateapipb.VersionedSandboxCompat{
				SchemaVersion: ver,
				Attributes:    attrs,
			},
		}
	}
	amd64GVisor := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
	)
	amd64GVisorReordered := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
	)
	arm64GVisor := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"},
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
	)
	amd64MicroVM := runtimeWith("microvm", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
	)
	amd64GVisorV2 := runtimeWith("gvisor", "v2",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
	)
	amd64GVisorExtraAttr := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: "7"},
		&ateapipb.AttributeEntry{Key: AttrSandboxRuntimeHash, Value: "sha256:abc"},
	)

	tests := []struct {
		name   string
		worker *ateapipb.SandboxRuntime
		snap   *ateapipb.SandboxRuntime
		want   bool
	}{
		{"nil snapshot imposes no constraint", amd64GVisor, nil, true},
		{"exact match", amd64GVisor, amd64GVisor, true},
		{"attribute order ignored", amd64GVisor, amd64GVisorReordered, true},
		{"different attribute value fails", arm64GVisor, amd64GVisor, false},
		{"different sandbox class fails", amd64MicroVM, amd64GVisor, false},
		{"different schema version fails", amd64GVisorV2, amd64GVisor, false},
		{"attribute only the worker has is ignored", amd64GVisorExtraAttr, amd64GVisor, true},
		{"attribute missing on worker fails", amd64GVisor, amd64GVisorExtraAttr, false},
		{"nil worker fails when snapshot is stamped", nil, amd64GVisor, false},
		{"nil version fails", &ateapipb.SandboxRuntime{SandboxClass: "gvisor"}, amd64GVisor, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Matches(tc.worker, tc.snap); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchesCompat(t *testing.T) {
	compat := func(ver string, attrs ...*ateapipb.AttributeEntry) *ateapipb.VersionedSandboxCompat {
		return &ateapipb.VersionedSandboxCompat{SchemaVersion: ver, Attributes: attrs}
	}
	amd64 := &ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"}
	arm64 := &ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"}
	hash := &ateapipb.AttributeEntry{Key: AttrSandboxRuntimeHash, Value: "sha256:abc"}
	cpuBits := func(val string) *ateapipb.AttributeEntry {
		return &ateapipb.AttributeEntry{Key: AttrCPUFeatures, Value: val}
	}

	// Build >64-bit masks so the subset check spans mask words.
	highSnap, err := EncodeCPUFeatures("amd64", []string{"pni", "avx2", "avx512_bf16"})
	if err != nil {
		t.Fatal(err)
	}
	highWorkerSuperset, err := EncodeCPUFeatures("amd64", []string{"pni", "sse2", "avx2", "avx_vnni", "avx512_bf16"})
	if err != nil {
		t.Fatal(err)
	}
	highWorkerMissingOne, err := EncodeCPUFeatures("amd64", []string{"pni", "sse2", "avx2", "avx_vnni"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name         string
		worker, snap *ateapipb.VersionedSandboxCompat
		want         bool
	}{
		{"equal", compat("v1", amd64), compat("v1", amd64), true},
		{"key only the worker has is ignored", compat("v1", amd64, cpuBits("7")), compat("v1", amd64), true},
		{"key only the snapshot has fails", compat("v1", amd64), compat("v1", amd64, cpuBits("7")), false},
		{"key only the snapshot has fails even at equal count", compat("v1", amd64, cpuBits("7")), compat("v1", amd64, hash), false},
		{"value differs", compat("v1", arm64), compat("v1", amd64), false},
		{"cpu_features exact match", compat("v1", amd64, cpuBits(highSnap)), compat("v1", amd64, cpuBits(highSnap)), true},
		{"cpu_features worker is superset of snapshot", compat("v1", amd64, cpuBits(highWorkerSuperset)), compat("v1", amd64, cpuBits(highSnap)), true},
		{"cpu_features worker missing a snapshot feature bit", compat("v1", amd64, cpuBits(highWorkerMissingOne)), compat("v1", amd64, cpuBits(highSnap)), false},
		{"cpu_features empty snapshot set matches any worker", compat("v1", amd64, cpuBits("7")), compat("v1", amd64, cpuBits("0")), true},
		{"cpu_features empty worker set matches nothing", compat("v1", amd64, cpuBits("0")), compat("v1", amd64, cpuBits("7")), false},
		{"cpu_features invalid worker bitmask fails", compat("v1", amd64, cpuBits("not-a-number")), compat("v1", amd64, cpuBits("7")), false},
		{"cpu_features invalid snapshot bitmask fails", compat("v1", amd64, cpuBits("7")), compat("v1", amd64, cpuBits("-1")), false},
		{"cpu_features non-canonical bitmask fails", compat("v1", amd64, cpuBits("07")), compat("v1", amd64, cpuBits("7")), false},
		{"cpu_features uppercase hex fails", compat("v1", amd64, cpuBits("1F")), compat("v1", amd64, cpuBits("7")), false},
		{"schema version differs", compat("v2", amd64), compat("v1", amd64), false},
		{"both schema versions empty", compat("", amd64), compat("", amd64), false},
		{"no attributes on either", compat("v1"), compat("v1"), true},
		// Unlike Matches, a nil snapshot identity here is a stamped snapshot
		// missing its identity, not the absence of a constraint.
		{"nil snapshot identity", compat("v1", amd64), nil, false},
		{"nil worker identity", nil, compat("v1", amd64), false},
		{"both nil", nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := MatchesCompat(tc.worker, tc.snap); got != tc.want {
				t.Errorf("MatchesCompat() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A Requirement is prepared once and asked of many workers, so what it keeps
// of the snapshot must hold across comparisons, and the zero value must
// require nothing.
func TestRequirement(t *testing.T) {
	rt := func(cpu string) *ateapipb.SandboxRuntime {
		return &ateapipb.SandboxRuntime{
			SandboxClass: "gvisor",
			Version: &ateapipb.VersionedSandboxCompat{
				SchemaVersion: SchemaVersionV1,
				Attributes: []*ateapipb.AttributeEntry{
					{Key: AttrArchitecture, Value: "amd64"},
					{Key: AttrCPUFeatures, Value: cpu},
				},
			},
		}
	}
	snap := rt("7")
	req := NewRequirement(snap)
	if req.Runtime() != snap {
		t.Errorf("Runtime() = %v, want the prepared runtime", req.Runtime())
	}
	for _, tc := range []struct {
		name   string
		worker *ateapipb.SandboxRuntime
		want   bool
	}{
		{"worker superset", rt("f"), true},
		{"worker missing a bit", rt("3"), false},
		{"worker mask invalid", rt("zz"), false},
		{"worker superset after misses", rt("7"), true},
	} {
		if got := req.Matches(tc.worker); got != tc.want {
			t.Errorf("%s: Matches() = %v, want %v", tc.name, got, tc.want)
		}
	}
	status := &ateapipb.WorkerStatus{DefaultRuntime: rt("3"), RestorableRuntimes: []*ateapipb.SandboxRuntime{rt("f")}}
	if got := req.MatchingRuntime(status); got != status.RestorableRuntimes[0] {
		t.Errorf("MatchingRuntime() = %v, want the restorable superset", got)
	}

	// A snapshot mask that does not parse is remembered as matching nothing.
	if NewRequirement(rt("zz")).Matches(rt("f")) {
		t.Error("Matches() with an invalid snapshot mask = true, want false")
	}

	var none Requirement
	if none.Runtime() != nil || !none.Matches(nil) || !none.WorkerMatches(nil) {
		t.Error("the zero Requirement must require nothing")
	}
	if got := none.RuntimeFor(status); got != status.DefaultRuntime {
		t.Errorf("zero Requirement RuntimeFor() = %v, want DefaultRuntime", got)
	}
}

func TestMatchingRuntimeAndWorkerMatches(t *testing.T) {
	rt := func(class, arch, hash string) *ateapipb.SandboxRuntime {
		attrs := []*ateapipb.AttributeEntry{{Key: AttrArchitecture, Value: arch}}
		if hash != "" {
			attrs = append(attrs, &ateapipb.AttributeEntry{Key: "gvisor_asset_hash", Value: hash})
		}
		return &ateapipb.SandboxRuntime{
			SandboxClass: class,
			Version: &ateapipb.VersionedSandboxCompat{
				SchemaVersion: SchemaVersionV1,
				Attributes:    attrs,
			},
		}
	}

	defRT := rt("gvisor", "amd64", "v2-hash")
	restorable1 := rt("gvisor", "amd64", "v1-hash")
	restorable2 := rt("gvisor", "amd64", "v0-hash")
	unmatchedRT := rt("gvisor", "arm64", "v2-hash")

	status := &ateapipb.WorkerStatus{
		DefaultRuntime:     defRT,
		RestorableRuntimes: []*ateapipb.SandboxRuntime{restorable1, restorable2},
	}

	t.Run("nil snap matches any worker status and returns DefaultRuntime", func(t *testing.T) {
		if !WorkerMatches(nil, nil) {
			t.Errorf("WorkerMatches(nil, nil) = false, want true")
		}
		if !WorkerMatches(&ateapipb.WorkerStatus{}, nil) {
			t.Errorf("WorkerMatches(empty, nil) = false, want true")
		}
		if got := MatchingRuntime(status, nil); got != defRT {
			t.Errorf("MatchingRuntime(status, nil) = %v, want DefaultRuntime", got)
		}
	})

	t.Run("matches DefaultRuntime", func(t *testing.T) {
		if !WorkerMatches(status, defRT) {
			t.Errorf("WorkerMatches(status, defRT) = false, want true")
		}
		if got := MatchingRuntime(status, defRT); got != defRT {
			t.Errorf("MatchingRuntime(status, defRT) = %v, want defRT", got)
		}
	})

	t.Run("matches RestorableRuntimes entry", func(t *testing.T) {
		if !WorkerMatches(status, restorable2) {
			t.Errorf("WorkerMatches(status, restorable2) = false, want true")
		}
		if got := MatchingRuntime(status, restorable2); got != restorable2 {
			t.Errorf("MatchingRuntime(status, restorable2) = %v, want restorable2", got)
		}
	})

	t.Run("returns nil and false when neither DefaultRuntime nor RestorableRuntimes match", func(t *testing.T) {
		if WorkerMatches(status, unmatchedRT) {
			t.Errorf("WorkerMatches(status, unmatchedRT) = true, want false")
		}
		if got := MatchingRuntime(status, unmatchedRT); got != nil {
			t.Errorf("MatchingRuntime(status, unmatchedRT) = %v, want nil", got)
		}
		if WorkerMatches(nil, defRT) {
			t.Errorf("WorkerMatches(nil, defRT) = true, want false")
		}
	})

	t.Run("RuntimeFor is the match, else the default a cold boot runs on", func(t *testing.T) {
		if got := RuntimeFor(status, restorable2); got != restorable2 {
			t.Errorf("RuntimeFor(status, restorable2) = %v, want restorable2", got)
		}
		if got := RuntimeFor(status, unmatchedRT); got != defRT {
			t.Errorf("RuntimeFor(status, unmatchedRT) = %v, want DefaultRuntime", got)
		}
		if got := RuntimeFor(status, nil); got != defRT {
			t.Errorf("RuntimeFor(status, nil) = %v, want DefaultRuntime", got)
		}
		// A worker that reported no runtimes runs the actor under none.
		if got := RuntimeFor(&ateapipb.WorkerStatus{}, unmatchedRT); got != nil {
			t.Errorf("RuntimeFor(empty, unmatchedRT) = %v, want nil", got)
		}
	})
}

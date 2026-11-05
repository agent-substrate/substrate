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

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
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

func TestRestoreFidelity(t *testing.T) {
	const (
		none    = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE
		volumes = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_VOLUMES
		rootfs  = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS
		memory  = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
	)
	rt := func(class, ver string, attrs ...*ateapi.AttributeEntry) *ateapi.ActorSandboxRuntime {
		return &ateapi.ActorSandboxRuntime{SandboxClass: class, Version: &ateapi.VersionedSandboxCompat{SchemaVersion: ver, Attributes: attrs}}
	}
	gvisor := func(attrs ...*ateapi.AttributeEntry) *ateapi.ActorSandboxRuntime { return rt("gvisor", "v1", attrs...) }
	attr := func(key, value string, rules ...*ateapi.CompatibleValue) *ateapi.AttributeEntry {
		return &ateapi.AttributeEntry{Key: key, Value: value, CompatibleValues: rules}
	}
	rule := func(value string, f ateapi.CompatibleFidelity) *ateapi.CompatibleValue {
		return &ateapi.CompatibleValue{Value: value, Fidelity: f}
	}
	amd64 := attr(AttrArchitecture, "amd64")
	arm64 := attr(AttrArchitecture, "arm64")
	hash := attr(AttrSandboxBundleHash, "sha256:abc")
	cpuBits := func(val string) *ateapi.AttributeEntry { return attr(AttrCPUFeatures, val) }
	const layout = "worker.layout"

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

	memorySnap := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	tests := []struct {
		name         string
		worker, snap *ateapi.ActorSandboxRuntime
		// fidelity is the snapshot's, MEMORY when unset.
		fidelity ateapi.SnapshotFidelity
		want     ateapi.CompatibleFidelity
	}{
		{name: "equal values", worker: gvisor(amd64, hash), snap: gvisor(amd64, hash), want: memory},
		{name: "attribute order ignored", worker: gvisor(hash, amd64), snap: gvisor(amd64, hash), want: memory},
		{name: "no attributes on either", worker: gvisor(), snap: gvisor(), want: memory},
		{name: "different value with no rule falls to ROOTFS", worker: gvisor(arm64), snap: gvisor(amd64), want: rootfs},
		{name: "key only the worker has falls to ROOTFS", worker: gvisor(amd64, hash), snap: gvisor(amd64), want: rootfs},
		{name: "key only the snapshot has falls to ROOTFS", worker: gvisor(amd64), snap: gvisor(amd64, hash), want: rootfs},
		{name: "key only the snapshot has falls to ROOTFS at equal count", worker: gvisor(amd64, cpuBits("7")), snap: gvisor(amd64, hash), want: rootfs},
		{
			name:   "worker rule for the snapshot's value",
			worker: gvisor(amd64, attr(layout, "new", rule("old", volumes))),
			snap:   gvisor(amd64, attr(layout, "old")),
			want:   volumes,
		},
		{
			name:   "worker rule for a snapshot without the key",
			worker: gvisor(amd64, attr(layout, "new", rule("", memory))),
			snap:   gvisor(amd64),
			want:   memory,
		},
		{
			name:   "snapshot rule for the worker's value",
			worker: gvisor(amd64, attr(layout, "old")),
			snap:   gvisor(amd64, attr(layout, "new", rule("old", volumes))),
			want:   volumes,
		},
		{
			name:   "snapshot rule for a worker without the key",
			worker: gvisor(amd64),
			snap:   gvisor(amd64, attr(layout, "new", rule("", rootfs))),
			want:   rootfs,
		},
		{
			name:   "both rules, the lower wins",
			worker: gvisor(amd64, attr(layout, "a", rule("b", memory))),
			snap:   gvisor(amd64, attr(layout, "b", rule("a", volumes))),
			want:   volumes,
		},
		{
			name:   "both rules, the lower wins either way round",
			worker: gvisor(amd64, attr(layout, "a", rule("b", volumes))),
			snap:   gvisor(amd64, attr(layout, "b", rule("a", memory))),
			want:   volumes,
		},
		{
			name:   "a rule for another value does not apply",
			worker: gvisor(amd64, attr(layout, "new", rule("older", memory))),
			snap:   gvisor(amd64, attr(layout, "old")),
			want:   rootfs,
		},
		{
			name:   "a rule applies only to a different value",
			worker: gvisor(amd64, attr(layout, "same", rule("", none))),
			snap:   gvisor(amd64, attr(layout, "same", rule("", none))),
			want:   memory,
		},
		{
			name:   "NONE from the snapshot",
			worker: gvisor(amd64),
			snap:   gvisor(amd64, attr(layout, "per_volume", rule("", none))),
			want:   none,
		},
		{
			name:   "NONE from the worker",
			worker: gvisor(amd64, attr(layout, "new", rule("old", none))),
			snap:   gvisor(amd64, attr(layout, "old")),
			want:   none,
		},
		{
			name:   "the lowest key wins",
			worker: gvisor(arm64, attr(layout, "new", rule("", volumes))),
			snap:   gvisor(amd64),
			want:   volumes,
		},
		{name: "cpu_features exact match", worker: gvisor(amd64, cpuBits(highSnap)), snap: gvisor(amd64, cpuBits(highSnap)), want: memory},
		{name: "cpu_features worker is superset of snapshot", worker: gvisor(amd64, cpuBits(highWorkerSuperset)), snap: gvisor(amd64, cpuBits(highSnap)), want: memory},
		{name: "cpu_features worker missing a snapshot feature bit", worker: gvisor(amd64, cpuBits(highWorkerMissingOne)), snap: gvisor(amd64, cpuBits(highSnap)), want: rootfs},
		{name: "cpu_features empty snapshot set matches any worker", worker: gvisor(amd64, cpuBits("7")), snap: gvisor(amd64, cpuBits("0")), want: memory},
		{name: "cpu_features empty worker set matches nothing", worker: gvisor(amd64, cpuBits("0")), snap: gvisor(amd64, cpuBits("7")), want: rootfs},
		{name: "cpu_features invalid worker bitmask", worker: gvisor(amd64, cpuBits("not-a-number")), snap: gvisor(amd64, cpuBits("7")), want: rootfs},
		{name: "cpu_features invalid snapshot bitmask", worker: gvisor(amd64, cpuBits("7")), snap: gvisor(amd64, cpuBits("-1")), want: rootfs},
		{name: "cpu_features non-canonical bitmask", worker: gvisor(amd64, cpuBits("07")), snap: gvisor(amd64, cpuBits("7")), want: rootfs},
		{name: "cpu_features uppercase hex", worker: gvisor(amd64, cpuBits("1F")), snap: gvisor(amd64, cpuBits("7")), want: rootfs},
		{name: "cpu_features on the snapshot only", worker: gvisor(amd64), snap: gvisor(amd64, cpuBits("7")), want: rootfs},
		{name: "cpu_features on the worker only", worker: gvisor(amd64, cpuBits("7")), snap: gvisor(amd64), want: rootfs},
		{name: "different schema version", worker: rt("gvisor", "v2", amd64), snap: gvisor(amd64), want: rootfs},
		{name: "both schema versions empty", worker: rt("gvisor", "", amd64), snap: rt("gvisor", "", amd64), want: rootfs},
		{name: "worker without a version", worker: &ateapi.ActorSandboxRuntime{SandboxClass: "gvisor"}, snap: gvisor(amd64), want: rootfs},
		{name: "snapshot without a version", worker: gvisor(amd64), snap: &ateapi.ActorSandboxRuntime{SandboxClass: "gvisor"}, want: rootfs},
		{name: "different sandbox class", worker: rt("microvm", "v1", amd64), snap: gvisor(amd64), want: none},
		{name: "nil worker", snap: gvisor(amd64), want: none},
		{name: "ROOTFS snapshot on an equal worker", worker: gvisor(amd64), snap: gvisor(amd64), fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, want: rootfs},
		{name: "ROOTFS snapshot on a different worker", worker: gvisor(arm64), snap: gvisor(amd64), fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, want: rootfs},
		{
			name:     "ROOTFS snapshot capped at VOLUMES",
			worker:   gvisor(amd64),
			snap:     gvisor(amd64, attr(layout, "new", rule("", volumes))),
			fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
			want:     volumes,
		},
		{
			name:     "VOLUMES snapshot refused",
			worker:   gvisor(amd64),
			snap:     gvisor(amd64, attr(layout, "per_volume", rule("", none))),
			fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			want:     none,
		},
		{name: "VOLUMES snapshot on a different worker", worker: gvisor(arm64), snap: gvisor(amd64), fidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, want: volumes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fidelity := tc.fidelity
			if fidelity == ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED {
				fidelity = memorySnap
			}
			if got := NewRequirement(tc.snap, fidelity).RestoreFidelity(tc.worker); got != tc.want {
				t.Errorf("RestoreFidelity() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOwnedKeys(t *testing.T) {
	for _, tc := range []struct {
		key        string
		host, mine bool
	}{
		{key: AttrArchitecture, host: true},
		{key: AttrCPUFeatures, host: true},
		{key: AttrCPUVendor, host: true},
		{key: AttrCPUModel, host: true},
		{key: AttrSandboxBundleHash, host: true},
		{key: "worker.durable_fs_layout", mine: true},
		{key: "worker.", mine: true},
		{key: "workers.x"},
		{key: "other"},
	} {
		if got := IsHostKey(tc.key); got != tc.host {
			t.Errorf("IsHostKey(%q) = %v, want %v", tc.key, got, tc.host)
		}
		if got := IsWorkerKey(tc.key); got != tc.mine {
			t.Errorf("IsWorkerKey(%q) = %v, want %v", tc.key, got, tc.mine)
		}
	}
}

// A Requirement is prepared once and asked of many workers, so what it keeps
// of the snapshot must hold across comparisons, and the zero value must
// require nothing.
func TestRequirement(t *testing.T) {
	rt := func(cpu string) *ateapi.ActorSandboxRuntime {
		return &ateapi.ActorSandboxRuntime{
			SandboxClass: "gvisor",
			Version: &ateapi.VersionedSandboxCompat{
				SchemaVersion: SchemaVersionV1,
				Attributes: []*ateapi.AttributeEntry{
					{Key: AttrArchitecture, Value: "amd64"},
					{Key: AttrCPUFeatures, Value: cpu},
				},
			},
		}
	}
	memory := ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
	snap := rt("7")
	req := NewRequirement(snap, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
	if req.Runtime() != snap {
		t.Errorf("Runtime() = %v, want the prepared runtime", req.Runtime())
	}
	for _, tc := range []struct {
		name   string
		worker *ateapi.ActorSandboxRuntime
		want   bool
	}{
		{"worker superset", rt("f"), true},
		{"worker missing a bit", rt("3"), false},
		{"worker mask invalid", rt("zz"), false},
		{"worker superset after misses", rt("7"), true},
	} {
		if got := req.RestoreFidelity(tc.worker) == memory; got != tc.want {
			t.Errorf("%s: RestoreFidelity() == MEMORY is %v, want %v", tc.name, got, tc.want)
		}
	}
	status := &ateapi.WorkerStatus{SandboxClasses: []*ateapi.WorkerSandboxClass{{
		SandboxClass:       "gvisor",
		DefaultRuntime:     &ateapi.WorkerSandboxRuntime{Version: rt("3").GetVersion()},
		RestorableRuntimes: []*ateapi.WorkerSandboxRuntime{{Version: rt("f").GetVersion()}},
	}}}
	if got := req.MatchingRuntime(status); !proto.Equal(got, rt("f")) {
		t.Errorf("MatchingRuntime() = %v, want the restorable superset", got)
	}

	// A snapshot mask that does not parse is remembered as matching nothing.
	if got := NewRequirement(rt("zz"), ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY).RestoreFidelity(rt("f")); got == memory {
		t.Errorf("RestoreFidelity() with an invalid snapshot mask = %v, want less", got)
	}

	var none Requirement
	if none.Runtime() != nil || !none.WorkerMatches(nil) || !none.WorkerRestores(nil) {
		t.Error("the zero Requirement must require nothing")
	}
	if NewRequirement(nil, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY).Runtime() != nil {
		t.Error("NewRequirement(nil) must require nothing")
	}
	if got := none.RuntimeFor(status); !proto.Equal(got, rt("3")) {
		t.Errorf("zero Requirement RuntimeFor() = %v, want the default", got)
	}
}

func TestMatchingRuntimeAndWorkerMatches(t *testing.T) {
	compat := func(arch, hash string) *ateapi.VersionedSandboxCompat {
		attrs := []*ateapi.AttributeEntry{{Key: AttrArchitecture, Value: arch}}
		if hash != "" {
			attrs = append(attrs, &ateapi.AttributeEntry{Key: AttrSandboxBundleHash, Value: hash})
		}
		return &ateapi.VersionedSandboxCompat{SchemaVersion: SchemaVersionV1, Attributes: attrs}
	}
	rt := func(class string, c *ateapi.VersionedSandboxCompat) *ateapi.ActorSandboxRuntime {
		return &ateapi.ActorSandboxRuntime{SandboxClass: class, Version: c}
	}

	defCompat := compat("amd64", "v2-hash")
	restorable1 := compat("amd64", "v1-hash")
	restorable2 := compat("amd64", "v0-hash")
	defRT := rt("gvisor", defCompat)
	unmatchedRT := rt("gvisor", compat("arm64", "v2-hash"))

	// The package-level forms the cases read as, for a MEMORY snapshot.
	memory := ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	WorkerMatches := func(status *ateapi.WorkerStatus, snap *ateapi.ActorSandboxRuntime) bool {
		return NewRequirement(snap, memory).WorkerMatches(status)
	}
	MatchingRuntime := func(status *ateapi.WorkerStatus, snap *ateapi.ActorSandboxRuntime) *ateapi.ActorSandboxRuntime {
		return NewRequirement(snap, memory).MatchingRuntime(status)
	}
	RuntimeFor := func(status *ateapi.WorkerStatus, snap *ateapi.ActorSandboxRuntime) *ateapi.ActorSandboxRuntime {
		return NewRequirement(snap, memory).RuntimeFor(status)
	}

	status := &ateapi.WorkerStatus{SandboxClasses: []*ateapi.WorkerSandboxClass{{
		SandboxClass:       "gvisor",
		DefaultRuntime:     &ateapi.WorkerSandboxRuntime{Version: defCompat},
		RestorableRuntimes: []*ateapi.WorkerSandboxRuntime{{Version: restorable1}, {Version: restorable2}},
	}}}

	t.Run("nil snap matches any worker status and returns the default", func(t *testing.T) {
		if !WorkerMatches(nil, nil) {
			t.Errorf("WorkerMatches(nil, nil) = false, want true")
		}
		if !WorkerMatches(&ateapi.WorkerStatus{}, nil) {
			t.Errorf("WorkerMatches(empty, nil) = false, want true")
		}
		if got := MatchingRuntime(status, nil); !proto.Equal(got, defRT) {
			t.Errorf("MatchingRuntime(status, nil) = %v, want %v", got, defRT)
		}
	})

	t.Run("matches DefaultRuntime", func(t *testing.T) {
		if !WorkerMatches(status, defRT) {
			t.Errorf("WorkerMatches(status, defRT) = false, want true")
		}
		if got := MatchingRuntime(status, defRT); !proto.Equal(got, defRT) {
			t.Errorf("MatchingRuntime(status, defRT) = %v, want defRT", got)
		}
	})

	t.Run("matches RestorableRuntimes entry", func(t *testing.T) {
		want := rt("gvisor", restorable2)
		if !WorkerMatches(status, want) {
			t.Errorf("WorkerMatches(status, restorable2) = false, want true")
		}
		if got := MatchingRuntime(status, want); !proto.Equal(got, want) {
			t.Errorf("MatchingRuntime(status, restorable2) = %v, want %v", got, want)
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

	t.Run("a class the worker does not run matches nothing, whatever its compat", func(t *testing.T) {
		snap := rt("microvm", defCompat)
		if WorkerMatches(status, snap) {
			t.Errorf("WorkerMatches(status, microvm snap) = true, want false")
		}
		if got := MatchingRuntime(status, snap); got != nil {
			t.Errorf("MatchingRuntime(status, microvm snap) = %v, want nil", got)
		}
	})

	t.Run("RuntimeFor is the match, else the default a cold boot runs on", func(t *testing.T) {
		if got, want := RuntimeFor(status, rt("gvisor", restorable2)), rt("gvisor", restorable2); !proto.Equal(got, want) {
			t.Errorf("RuntimeFor(status, restorable2) = %v, want %v", got, want)
		}
		if got := RuntimeFor(status, unmatchedRT); !proto.Equal(got, defRT) {
			t.Errorf("RuntimeFor(status, unmatchedRT) = %v, want %v", got, defRT)
		}
		if got := RuntimeFor(status, nil); !proto.Equal(got, defRT) {
			t.Errorf("RuntimeFor(status, nil) = %v, want %v", got, defRT)
		}
		// A worker that reported no classes runs the actor under none.
		if got := RuntimeFor(&ateapi.WorkerStatus{}, unmatchedRT); got != nil {
			t.Errorf("RuntimeFor(empty, unmatchedRT) = %v, want nil", got)
		}
	})

	t.Run("ColdBootRuntime before registration", func(t *testing.T) {
		if got := ColdBootRuntime(&ateapi.WorkerStatus{}); got != nil {
			t.Errorf("ColdBootRuntime(empty) = %v, want nil", got)
		}
		if got := ColdBootRuntime(nil); got != nil {
			t.Errorf("ColdBootRuntime(nil) = %v, want nil", got)
		}
		if got := ColdBootRuntime(status); !proto.Equal(got, defRT) {
			t.Errorf("ColdBootRuntime(status) = %v, want %v", got, defRT)
		}
	})
}

func TestWorkerRuntimeFor(t *testing.T) {
	compat := func(hash string) *ateapi.VersionedSandboxCompat {
		return &ateapi.VersionedSandboxCompat{SchemaVersion: SchemaVersionV1, Attributes: []*ateapi.AttributeEntry{
			{Key: AttrArchitecture, Value: "amd64"},
			{Key: AttrSandboxBundleHash, Value: hash},
		}}
	}
	def := &ateapi.WorkerSandboxRuntime{Version: compat("new"), ActorOverhead: &ateapi.RuntimeOverhead{MemoryBytes: 4 << 20}}
	old := &ateapi.WorkerSandboxRuntime{Version: compat("old"), ActorOverhead: &ateapi.RuntimeOverhead{MemoryBytes: 16 << 20}}
	status := &ateapi.WorkerStatus{SandboxClasses: []*ateapi.WorkerSandboxClass{{
		SandboxClass:       "microvm",
		DefaultRuntime:     def,
		RestorableRuntimes: []*ateapi.WorkerSandboxRuntime{old},
	}}}
	stamp := func(c *ateapi.VersionedSandboxCompat) *ateapi.ActorSandboxRuntime {
		return &ateapi.ActorSandboxRuntime{SandboxClass: "microvm", Version: c}
	}
	for _, tc := range []struct {
		name string
		snap *ateapi.ActorSandboxRuntime
		want *ateapi.WorkerSandboxRuntime
	}{
		{name: "cold boot runs the default", want: def},
		{name: "a restore runs the matching runtime", snap: stamp(compat("old")), want: old},
		{name: "a snapshot nothing restores cold boots on the default", snap: stamp(compat("gone")), want: def},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRequirement(tc.snap, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
			got := r.WorkerRuntimeFor(status)
			if got != tc.want {
				t.Errorf("WorkerRuntimeFor() = %v, want %v", got, tc.want)
			}
			// it is the runtime RuntimeFor stamps
			if stamped := r.RuntimeFor(status); !proto.Equal(stamped.GetVersion(), got.GetVersion()) {
				t.Errorf("RuntimeFor() = %v, which is not WorkerRuntimeFor()'s %v", stamped, got)
			}
		})
	}
	if got := NewRequirement(nil, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY).WorkerRuntimeFor(&ateapi.WorkerStatus{}); got != nil {
		t.Errorf("WorkerRuntimeFor() before registration = %v, want nil", got)
	}
}

// WorkerMatches asks for the requirement's least fidelity, which AtLeast
// lowers, and WorkerRestores for any fidelity above NONE.
func TestWorkerMatchesAtLeast(t *testing.T) {
	compat := func(arch string, attrs ...*ateapi.AttributeEntry) *ateapi.VersionedSandboxCompat {
		return &ateapi.VersionedSandboxCompat{SchemaVersion: SchemaVersionV1, Attributes: append([]*ateapi.AttributeEntry{{Key: AttrArchitecture, Value: arch}}, attrs...)}
	}
	layout := func(value string, f ateapi.CompatibleFidelity) *ateapi.AttributeEntry {
		return &ateapi.AttributeEntry{Key: "worker.layout", Value: value, CompatibleValues: []*ateapi.CompatibleValue{{Value: "", Fidelity: f}}}
	}
	status := func(c *ateapi.VersionedSandboxCompat) *ateapi.WorkerStatus {
		return &ateapi.WorkerStatus{SandboxClasses: []*ateapi.WorkerSandboxClass{{SandboxClass: "gvisor", DefaultRuntime: &ateapi.WorkerSandboxRuntime{Version: c}}}}
	}
	stamp := func(c *ateapi.VersionedSandboxCompat) *ateapi.ActorSandboxRuntime {
		return &ateapi.ActorSandboxRuntime{SandboxClass: "gvisor", Version: c}
	}
	const (
		volumes = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_VOLUMES
		rootfs  = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_ROOTFS
		memory  = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_MEMORY
		none    = ateapi.CompatibleFidelity_COMPATIBLE_FIDELITY_NONE
	)
	oldWorker := status(compat("amd64"))
	newWorker := status(compat("amd64", layout("per_volume", memory)))

	for _, tc := range []struct {
		name   string
		worker *ateapi.WorkerStatus
		snap   *ateapi.ActorSandboxRuntime
		// matches is WorkerMatches at MEMORY, ROOTFS and VOLUMES.
		matches  [3]bool
		restores bool
	}{
		{name: "old snapshot on a new worker by its rule", worker: newWorker, snap: stamp(compat("amd64")), matches: [3]bool{true, true, true}, restores: true},
		{name: "new snapshot on a new worker", worker: newWorker, snap: stamp(compat("amd64", layout("per_volume", none))), matches: [3]bool{true, true, true}, restores: true},
		{name: "new snapshot on an old worker", worker: oldWorker, snap: stamp(compat("amd64", layout("per_volume", none))), matches: [3]bool{false, false, false}, restores: false},
		{name: "another architecture restores at ROOTFS", worker: oldWorker, snap: stamp(compat("arm64")), matches: [3]bool{false, true, true}, restores: true},
		{name: "a rule restores at VOLUMES", worker: oldWorker, snap: stamp(compat("amd64", layout("x", volumes))), matches: [3]bool{false, false, true}, restores: true},
		{name: "another class", worker: oldWorker, snap: &ateapi.ActorSandboxRuntime{SandboxClass: "microvm", Version: compat("amd64")}, matches: [3]bool{false, false, false}, restores: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRequirement(tc.snap, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY)
			for i, f := range []ateapi.CompatibleFidelity{memory, rootfs, volumes} {
				if got := r.AtLeast(f).WorkerMatches(tc.worker); got != tc.matches[i] {
					t.Errorf("AtLeast(%v).WorkerMatches() = %v, want %v", f, got, tc.matches[i])
				}
			}
			if got := r.WorkerMatches(tc.worker); got != tc.matches[0] {
				t.Errorf("WorkerMatches() = %v, want it to ask for MEMORY, %v", got, tc.matches[0])
			}
			if got := r.WorkerRestores(tc.worker); got != tc.restores {
				t.Errorf("WorkerRestores() = %v, want %v", got, tc.restores)
			}
		})
	}
}

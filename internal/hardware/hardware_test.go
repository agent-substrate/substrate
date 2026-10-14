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
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

func TestEncodeDecodeCPUFeatures(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch+"/all_features_roundtrip", func(t *testing.T) {
			all := cpuFeatureTables[arch].names
			mask, err := EncodeCPUFeatures(arch, all)
			if err != nil {
				t.Fatalf("EncodeCPUFeatures(%q, all) error: %v", arch, err)
			}
			got, err := DecodeCPUFeatures(arch, mask)
			if err != nil {
				t.Fatalf("DecodeCPUFeatures(%q, %q) error: %v", arch, mask, err)
			}
			if diff := cmp.Diff(all, got); diff != "" {
				t.Errorf("round-trip mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run(arch+"/empty_roundtrip", func(t *testing.T) {
			mask, err := EncodeCPUFeatures(arch, nil)
			if err != nil {
				t.Fatalf("EncodeCPUFeatures(%q, nil) error: %v", arch, err)
			}
			if mask != "0" {
				t.Errorf("EncodeCPUFeatures(%q, nil) = %q, want \"0\"", arch, mask)
			}
			got, err := DecodeCPUFeatures(arch, mask)
			if err != nil {
				t.Fatalf("DecodeCPUFeatures(%q, %q) error: %v", arch, mask, err)
			}
			if len(got) != 0 {
				t.Errorf("DecodeCPUFeatures(%q, \"0\") = %v, want empty", arch, got)
			}
		})
	}

	t.Run("duplicates_and_order_normalized", func(t *testing.T) {
		m1, err := EncodeCPUFeatures("amd64", []string{"avx2", "pni", "avx2", ""})
		if err != nil {
			t.Fatal(err)
		}
		m2, err := EncodeCPUFeatures("amd64", []string{"pni", "avx2"})
		if err != nil {
			t.Fatal(err)
		}
		if m1 != m2 {
			t.Errorf("EncodeCPUFeatures with duplicates/reordering = %q, want %q", m1, m2)
		}
		got, err := DecodeCPUFeatures("amd64", m1)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff([]string{"pni", "avx2"}, got); diff != "" {
			t.Errorf("decoded features mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("errors", func(t *testing.T) {
		if _, err := EncodeCPUFeatures("riscv64", []string{"pni"}); err == nil {
			t.Error("EncodeCPUFeatures with unsupported arch succeeded")
		}
		if _, err := EncodeCPUFeatures("amd64", []string{"not_a_real_feature"}); err == nil {
			t.Error("EncodeCPUFeatures with unknown feature succeeded")
		}
		if _, err := DecodeCPUFeatures("riscv64", "0"); err == nil {
			t.Error("DecodeCPUFeatures with unsupported arch succeeded")
		}
		for _, bad := range []string{"", "0x1f", "1F", "07", "xyz", strings.Repeat("f", featureMaskWords*16+1)} {
			if _, err := DecodeCPUFeatures("amd64", bad); err == nil {
				t.Errorf("DecodeCPUFeatures(%q) succeeded, want a non-canonical value rejected", bad)
			}
		}
		var outOfRange featureMask
		outOfRange.set(len(amd64CPUFeatures))
		if _, err := DecodeCPUFeatures("amd64", outOfRange.String()); err == nil {
			t.Error("DecodeCPUFeatures with a bit beyond the table succeeded")
		}
	})

	t.Run("canonical_hex", func(t *testing.T) {
		// Bits 0, 1, 2 and 64: one word boundary crossed, low word zero-padded.
		var m featureMask
		for _, bit := range []int{0, 1, 2, 64} {
			m.set(bit)
		}
		if got, want := m.String(), "1"+strings.Repeat("0", 15)+"7"; got != want {
			t.Errorf("featureMask.String() = %q, want %q", got, want)
		}
		back, ok := parseFeatureMask(m.String())
		if !ok || back != m {
			t.Errorf("parseFeatureMask(%q) = %v, %v; want the original mask", m.String(), back, ok)
		}
	})
}

// TestCPUFeatureTables checks structural invariants of each architecture's
// feature table and pins anchor bit positions. The generator keeps committed
// order and only appends, so a moved anchor means the generated file was
// edited by hand and stored snapshots no longer decode as they were encoded.
func TestCPUFeatureTables(t *testing.T) {
	for _, tc := range []struct {
		arch, name string
		bit        int
	}{
		{"amd64", "pni", 0},
		{"amd64", "fpu", 31},
		{"amd64", "avx512_bf16", 174},
		{"arm64", "fp", 0},
		{"arm64", "asimdfhm", 23},
	} {
		if got, ok := cpuFeatureTables[tc.arch].index[tc.name]; !ok || got != tc.bit {
			t.Errorf("%s %q at bit %d (present=%v), want %d", tc.arch, tc.name, got, ok, tc.bit)
		}
	}
	for arch, table := range cpuFeatureTables {
		t.Run(arch, func(t *testing.T) {
			if len(table.names) == 0 {
				t.Fatalf("%s table is empty", arch)
			}
			if len(table.names) > featureMaskWords*64 {
				t.Errorf("%s table has %d names, more than a featureMask's %d bits", arch, len(table.names), featureMaskWords*64)
			}
			seen := make(map[string]int, len(table.names))
			for i, name := range table.names {
				if j, dup := seen[name]; dup {
					t.Errorf("%s table lists %q at both %d and %d", arch, name, j, i)
				}
				seen[name] = i
				if name != strings.TrimSpace(name) || name == "" {
					t.Errorf("%s table entry %d is %q, want a trimmed non-empty name", arch, i, name)
				}
			}
		})
	}
}

func TestParseCPUInfo(t *testing.T) {
	tests := []struct {
		name       string
		arch       string
		cpuinfo    string
		wantVendor string
		wantModel  string
		wantErr    bool
	}{
		{
			name: "amd64 Intel",
			arch: "amd64",
			cpuinfo: `processor	: 0
vendor_id	: GenuineIntel
cpu family	: 6
model		: 143
model name	: Intel(R) Xeon(R) Platinum 8481C
`,
			wantVendor: "GenuineIntel",
			wantModel:  "6:143",
		},
		{
			name: "arm64 Neoverse",
			arch: "arm64",
			cpuinfo: `processor	: 0
BogoMIPS	: 2000.00
Features	: fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics
CPU implementer	: 0x41
CPU architecture: 8
CPU variant	: 0x0
CPU part	: 0xd40
CPU revision	: 1
`,
			wantVendor: "0x41",
			wantModel:  "0x0:0xd40:1",
		},
		{
			name:    "missing fields",
			arch:    "amd64",
			cpuinfo: "vendor_id : GenuineIntel\n",
			wantErr: true,
		},
		{
			name:    "unsupported arch",
			arch:    "riscv64",
			cpuinfo: "vendor_id : test\n",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vendor, model, err := parseCPUInfo(tc.arch, strings.NewReader(tc.cpuinfo))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseCPUInfo() error = %v, wantErr %v", err, tc.wantErr)
			}
			if vendor != tc.wantVendor || model != tc.wantModel {
				t.Errorf("parseCPUInfo() = (%q, %q), want (%q, %q)", vendor, model, tc.wantVendor, tc.wantModel)
			}
		})
	}
}

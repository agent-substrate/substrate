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
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	intelCPUInfo = `processor	: 0
vendor_id	: GenuineIntel
cpu family	: 6
model		: 143
model name	: Intel(R) Xeon(R) Platinum 8481C CPU @ 2.70GHz
stepping	: 8
flags		: fpu vme avx2 aes fma avx2

processor	: 1
vendor_id	: GenuineIntel
cpu family	: 6
model		: 143
flags		: fpu vme avx2 aes fma
`

	amdCPUInfo = `processor	: 0
vendor_id	: AuthenticAMD
cpu family	: 25
model		: 17
model name	: AMD EPYC 9B14
flags		: fpu vme sse4_2 avx2 sha_ni
`

	arm64CPUInfo = `processor	: 0
BogoMIPS	: 50.00
Features	: fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics
CPU implementer	: 0x41
CPU architecture: 8
CPU variant	: 0x1
CPU part	: 0xd40
CPU revision	: 1
`
)

func writeCPUInfo(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cpuinfo")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("writing cpuinfo fixture: %v", err)
	}
	return p
}

func entries(kv ...string) []*ateletpb.AttributeEntry {
	var out []*ateletpb.AttributeEntry
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, &ateletpb.AttributeEntry{Key: kv[i], Value: kv[i+1]})
	}
	return out
}

func TestRegistry(t *testing.T) {
	snakeCase := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	seen := make(map[string]bool, len(allAttributes))
	for _, attr := range allAttributes {
		if !snakeCase.MatchString(attr.key) {
			t.Errorf("attribute key %q is not snake_case", attr.key)
		}
		if seen[attr.key] {
			t.Errorf("duplicate attribute key %q in allAttributes", attr.key)
		}
		seen[attr.key] = true
		if attr.derive == nil {
			t.Errorf("attribute %q has nil derive", attr.key)
		}
	}

	for name, tc := range map[string]struct {
		profile  Profile
		wantKeys []string
	}{
		"Base":    {Base, []string{AttrArchitecture}},
		"GVisor":  {GVisor, []string{AttrArchitecture}},
		"MicroVM": {MicroVM, []string{AttrArchitecture, AttrCPUVendor, AttrCPUModel}},
	} {
		gotKeys := tc.profile.Keys()
		if diff := cmp.Diff(tc.wantKeys, gotKeys); diff != "" {
			t.Errorf("%s.Keys() mismatch (-want +got):\n%s", name, diff)
		}
		for _, key := range gotKeys {
			if !seen[key] {
				t.Errorf("profile %s references unregistered attribute %q", name, key)
			}
		}
	}
}

func TestProfileFor(t *testing.T) {
	for class, want := range map[string]Profile{
		"gvisor":  GVisor,
		"microvm": MicroVM,
		"":        Base,
		"unknown": Base,
	} {
		if got := ProfileFor(class); !cmp.Equal(want.Keys(), got.Keys()) {
			t.Errorf("ProfileFor(%q) keys = %v, want %v", class, got.Keys(), want.Keys())
		}
	}
}

func TestProbeHost(t *testing.T) {
	for _, p := range []Profile{GVisor, MicroVM} {
		got := ProbeHost(p)
		if got.GetSchemaVersion() != SchemaVersionV1 {
			t.Errorf("ProbeHost() schema_version = %q, want %q", got.GetSchemaVersion(), SchemaVersionV1)
		}
		// Only architecture is knowable on every test host; the rest depends
		// on /proc/cpuinfo and is covered by fixtures below.
		if len(got.GetAttributes()) == 0 || got.GetAttributes()[0].GetKey() != AttrArchitecture || got.GetAttributes()[0].GetValue() != runtime.GOARCH {
			t.Errorf("ProbeHost() attributes = %v, want %s=%s first", got.GetAttributes(), AttrArchitecture, runtime.GOARCH)
		}
		for _, a := range got.GetAttributes() {
			if !slices.Contains(p.Keys(), a.GetKey()) {
				t.Errorf("ProbeHost() reported %q outside profile %v", a.GetKey(), p.Keys())
			}
		}
	}
}

func TestCompat(t *testing.T) {
	tests := []struct {
		name    string
		profile Profile
		arch    string
		cpuinfo string
		want    []*ateletpb.AttributeEntry
	}{
		{
			name:    "gvisor reports architecture only",
			profile: GVisor,
			arch:    "amd64",
			cpuinfo: intelCPUInfo,
			want:    entries(AttrArchitecture, "amd64"),
		},
		{
			name:    "missing cpuinfo falls back to architecture only",
			profile: MicroVM,
			arch:    "amd64",
			want:    entries(AttrArchitecture, "amd64"),
		},
		{
			name:    "x86_64 Intel vendor and family/model from the first core",
			profile: MicroVM,
			arch:    "amd64",
			cpuinfo: intelCPUInfo,
			want:    entries(AttrArchitecture, "amd64", AttrCPUVendor, "GenuineIntel", AttrCPUModel, "6/143"),
		},
		{
			name:    "x86_64 AMD vendor and family/model",
			profile: MicroVM,
			arch:    "amd64",
			cpuinfo: amdCPUInfo,
			want:    entries(AttrArchitecture, "amd64", AttrCPUVendor, "AuthenticAMD", AttrCPUModel, "25/17"),
		},
		{
			name:    "x86_64 model without cpu family omits cpu_model",
			profile: MicroVM,
			arch:    "amd64",
			cpuinfo: "vendor_id : GenuineIntel\nmodel : 143\n",
			want:    entries(AttrArchitecture, "amd64", AttrCPUVendor, "GenuineIntel"),
		},
		{
			name:    "arm64 implementer and variant/part",
			profile: MicroVM,
			arch:    "arm64",
			cpuinfo: arm64CPUInfo,
			want:    entries(AttrArchitecture, "arm64", AttrCPUVendor, "0x41", AttrCPUModel, "0x1/0xd40"),
		},
		{
			name:    "arm64 part without variant still reports part",
			profile: MicroVM,
			arch:    "arm64",
			cpuinfo: "CPU implementer : 0x41\nCPU part : 0xd40\n",
			want:    entries(AttrArchitecture, "arm64", AttrCPUVendor, "0x41", AttrCPUModel, "0xd40"),
		},
		{
			name:    "unknown architecture reports vendor but no model",
			profile: MicroVM,
			arch:    "riscv64",
			cpuinfo: "vendor_id : X\ncpu family : 1\nmodel : 2\n",
			want:    entries(AttrArchitecture, "riscv64", AttrCPUVendor, "X"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing")
			if tc.cpuinfo != "" {
				path = writeCPUInfo(t, tc.cpuinfo)
			}
			got := compat(readHost(path, tc.arch), tc.profile)
			want := &ateletpb.VersionedSandboxCompat{SchemaVersion: SchemaVersionV1, Attributes: tc.want}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("compat() mismatch (-want +got):\n%s", diff)
			}
		})
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
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorReordered := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
	)
	arm64GVisor := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "arm64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64MicroVM := runtimeWith("microvm", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorV2 := runtimeWith("gvisor", "v2",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
	)
	amd64GVisorExtraAttr := runtimeWith("gvisor", "v1",
		&ateapipb.AttributeEntry{Key: AttrArchitecture, Value: "amd64"},
		&ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"},
		&ateapipb.AttributeEntry{Key: "gvisor_asset_hash", Value: "abc"},
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
		{"attribute the snapshot predates is not checked", amd64GVisorExtraAttr, amd64GVisor, true},
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
	cpu := &ateapipb.AttributeEntry{Key: "cpu_features", Value: "GenuineIntel"}

	tests := []struct {
		name         string
		worker, snap *ateapipb.VersionedSandboxCompat
		want         bool
	}{
		{"equal", compat("v1", amd64), compat("v1", amd64), true},
		{"key only the worker has is ignored", compat("v1", amd64, cpu), compat("v1", amd64), true},
		{"key only the snapshot has fails", compat("v1", amd64), compat("v1", amd64, cpu), false},
		{"value differs", compat("v1", arm64), compat("v1", amd64), false},
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

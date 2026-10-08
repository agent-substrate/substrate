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
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
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

	wantProfiles := map[string]struct {
		profile  Profile
		wantKeys []string
	}{
		"GVisor":  {GVisor, []string{AttrArchitecture, AttrCPUFeatures}},
		"MicroVM": {MicroVM, []string{AttrArchitecture, AttrCPUVendor, AttrCPUModel}},
	}
	for name, tc := range wantProfiles {
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

func TestProbeGVisor(t *testing.T) {
	if got := Probe(GVisor).GetAttributes()[AttrArchitecture]; got != runtime.GOARCH {
		t.Errorf("Probe(GVisor) architecture = %q, want %q", got, runtime.GOARCH)
	}

	t.Run("missing cpuinfo falls back to architecture only", func(t *testing.T) {
		hw := identity(readHost(filepath.Join(t.TempDir(), "missing"), "amd64"), GVisor)
		want := map[string]string{AttrArchitecture: "amd64"}
		if diff := cmp.Diff(want, hw.GetAttributes()); diff != "" {
			t.Errorf("attributes mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("x86_64 flags are canonicalized and hashed with FNV-1a", func(t *testing.T) {
		// Golden 64-bit FNV-1a digest of canonical sorted flags "aes avx2 fma fpu vme".
		// Changing this format invalidates warm-restore matching for existing snapshots.
		const wantIntelHash = "0ac853df914e2ca0"

		hw := identity(readHost(writeCPUInfo(t, intelCPUInfo), "amd64"), GVisor)
		reordered := identity(readHost(writeCPUInfo(t, "flags : fma aes vme fpu avx2\n"), "amd64"), GVisor)
		different := identity(readHost(writeCPUInfo(t, amdCPUInfo), "amd64"), GVisor)

		gotHash := hw.GetAttributes()[AttrCPUFeatures]
		if gotHash != wantIntelHash {
			t.Fatalf("cpu_features hash = %q, want golden digest %q", gotHash, wantIntelHash)
		}
		if gotHash != reordered.GetAttributes()[AttrCPUFeatures] {
			t.Errorf("reordered/deduplicated flags hash = %q, want %q", reordered.GetAttributes()[AttrCPUFeatures], gotHash)
		}
		if gotHash == different.GetAttributes()[AttrCPUFeatures] {
			t.Errorf("distinct feature sets produced identical hash %q", gotHash)
		}
		if _, hasVendor := hw.GetAttributes()[AttrCPUVendor]; hasVendor {
			t.Errorf("gVisor hardware attributes unexpectedly included %q: %v", AttrCPUVendor, hw.GetAttributes())
		}
	})

	t.Run("arm64 Features are hashed with FNV-1a", func(t *testing.T) {
		hw := identity(readHost(writeCPUInfo(t, arm64CPUInfo), "arm64"), GVisor)
		if got := hw.GetAttributes()[AttrCPUFeatures]; len(got) != 16 {
			t.Errorf("arm64 cpu_features = %q, want 16-char hex FNV-1a", got)
		}
	})
}

func TestProbeMicroVM(t *testing.T) {
	if got := Probe(MicroVM).GetAttributes()[AttrArchitecture]; got != runtime.GOARCH {
		t.Errorf("Probe(MicroVM) architecture = %q, want %q", got, runtime.GOARCH)
	}

	tests := []struct {
		name    string
		arch    string
		cpuinfo string
		want    map[string]string
	}{
		{
			name: "missing cpuinfo falls back to architecture only",
			arch: "amd64",
			want: map[string]string{
				AttrArchitecture: "amd64",
			},
		},
		{
			name:    "x86_64 Intel vendor and family/model",
			arch:    "amd64",
			cpuinfo: intelCPUInfo,
			want: map[string]string{
				AttrArchitecture: "amd64",
				AttrCPUVendor:    "GenuineIntel",
				AttrCPUModel:     "6/143",
			},
		},
		{
			name:    "x86_64 AMD vendor and family/model",
			arch:    "amd64",
			cpuinfo: amdCPUInfo,
			want: map[string]string{
				AttrArchitecture: "amd64",
				AttrCPUVendor:    "AuthenticAMD",
				AttrCPUModel:     "25/17",
			},
		},
		{
			name:    "x86_64 model without cpu family omits ambiguous cpu_model",
			arch:    "amd64",
			cpuinfo: "vendor_id : GenuineIntel\nmodel : 143\n",
			want: map[string]string{
				AttrArchitecture: "amd64",
				AttrCPUVendor:    "GenuineIntel",
			},
		},
		{
			name:    "arm64 implementer and variant/part",
			arch:    "arm64",
			cpuinfo: arm64CPUInfo,
			want: map[string]string{
				AttrArchitecture: "arm64",
				AttrCPUVendor:    "0x41",
				AttrCPUModel:     "0x1/0xd40",
			},
		},
		{
			name:    "arm64 part without variant still reports part",
			arch:    "arm64",
			cpuinfo: "CPU implementer : 0x41\nCPU part : 0xd40\n",
			want: map[string]string{
				AttrArchitecture: "arm64",
				AttrCPUVendor:    "0x41",
				AttrCPUModel:     "0xd40",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing")
			if tc.cpuinfo != "" {
				path = writeCPUInfo(t, tc.cpuinfo)
			}
			hw := identity(readHost(path, tc.arch), MicroVM)
			if diff := cmp.Diff(tc.want, hw.GetAttributes()); diff != "" {
				t.Errorf("attributes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMatches(t *testing.T) {
	amd64Worker := &ateapipb.HardwareIdentity{Attributes: map[string]string{
		AttrArchitecture: "amd64",
		AttrCPUFeatures:  "0123456789abcdef",
	}}
	diffFeaturesWorker := &ateapipb.HardwareIdentity{Attributes: map[string]string{
		AttrArchitecture: "amd64",
		AttrCPUFeatures:  "fedcba9876543210",
	}}
	arm64Worker := &ateapipb.HardwareIdentity{Attributes: map[string]string{
		AttrArchitecture: "arm64",
		AttrCPUFeatures:  "0123456789abcdef",
	}}

	tests := []struct {
		name   string
		worker *ateapipb.HardwareIdentity
		snap   *ateapipb.HardwareIdentity
		want   bool
	}{
		{"nil snapshot imposes no constraint", amd64Worker, nil, true},
		{"empty snapshot imposes no constraint", amd64Worker, &ateapipb.HardwareIdentity{}, true},
		{"same architecture and cpu_features matches", amd64Worker, amd64Worker, true},
		{"different cpu_features fails", diffFeaturesWorker, amd64Worker, false},
		{"different architecture fails", arm64Worker, amd64Worker, false},
		{"nil worker fails when snapshot is stamped", nil, amd64Worker, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Matches(tc.worker, tc.snap); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}

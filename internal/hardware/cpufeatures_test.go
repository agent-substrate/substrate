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
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestEncodeDecodeCPUFeatures(t *testing.T) {
	for _, arch := range []string{"amd64", "arm64"} {
		t.Run(arch+"/all_features_roundtrip", func(t *testing.T) {
			table := cpuFeatureTables[arch]
			all := make([]string, 0, len(table.bits))
			for _, bit := range table.bits {
				all = append(all, table.names[bit])
			}
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
		// Bit 16 is CPUID.1:ECX bit 16, which the hardware reserves, so it
		// sits inside the table's range with no name; and one past the
		// highest named bit has none either. Decoding must name the bit.
		amd64 := cpuFeatureTables["amd64"]
		lastBit := amd64.bits[len(amd64.bits)-1]
		for _, bit := range []int{16, lastBit + 1} {
			if _, named := amd64.names[bit]; named {
				t.Fatalf("test assumes amd64 bit %d has no name, but it is %q", bit, amd64.names[bit])
			}
			var m featureMask
			m.set(0)
			m.set(bit)
			_, err := DecodeCPUFeatures("amd64", m.String())
			if err == nil || !strings.Contains(err.Error(), strconv.Itoa(bit)) {
				t.Errorf("DecodeCPUFeatures with unnamed bit %d set = %v, want an error naming the bit", bit, err)
			}
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
// feature table and pins anchor bits to the values of gVisor's Feature
// constants (32*block + CPUID bit on amd64). A moved anchor means
// cpufeatures_table.go no longer follows the hardware's numbering and stored
// snapshots would decode differently than they were encoded.
func TestCPUFeatureTables(t *testing.T) {
	for _, tc := range []struct {
		arch, name string
		bit        int
	}{
		{"amd64", "pni", 0},
		{"amd64", "pcid", 17},           // after the reserved bit 16
		{"amd64", "osxsave", 27},        // listed after bit 31 in gVisor's map
		{"amd64", "fpu", 32},            // block 1, bit 0
		{"amd64", "avx2", 2*32 + 5},     // block 2
		{"amd64", "syscall", 6*32 + 11}, // sparse block 6
		{"amd64", "avx512_bf16", 8*32 + 5},
		{"arm64", "fp", 0},
		{"arm64", "asimdfhm", 23},
	} {
		if got, ok := cpuFeatureTables[tc.arch].index[tc.name]; !ok || got != tc.bit {
			t.Errorf("%s %q at bit %d (present=%v), want %d", tc.arch, tc.name, got, ok, tc.bit)
		}
	}
	for arch, table := range cpuFeatureTables {
		t.Run(arch, func(t *testing.T) {
			if len(table.bits) == 0 {
				t.Fatalf("%s table is empty", arch)
			}
			if !slices.IsSorted(table.bits) || len(table.bits) != len(table.names) {
				t.Errorf("%s table bits %v are not the sorted keys of its %d names", arch, table.bits, len(table.names))
			}
			var defined featureMask
			seen := make(map[string]int, len(table.names))
			for bit, name := range table.names {
				defined.set(bit) // newFeatureTable panics at init on a bit outside the mask
				if other, dup := seen[name]; dup {
					t.Errorf("%s table lists %q at both %d and %d", arch, name, other, bit)
				}
				seen[name] = bit
				if name != strings.TrimSpace(name) || name == "" {
					t.Errorf("%s table bit %d is %q, want a trimmed non-empty name", arch, bit, name)
				}
				if got := table.index[name]; got != bit {
					t.Errorf("%s index[%q] = %d, want %d", arch, name, got, bit)
				}
			}
			if defined != table.defined {
				t.Errorf("%s table's defined mask %v does not match its names", arch, table.defined)
			}
			for name, bit := range table.index {
				if _, ok := table.names[bit]; !ok {
					t.Errorf("%s index maps %q to bit %d, which has no name", arch, name, bit)
				}
			}
		})
	}
}

// A table entry the mask cannot hold is a panic at package init rather than a
// dropped name.
func TestNewFeatureTableRejectsBitOutsideMask(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("newFeatureTable with a bit outside the mask did not panic")
		}
	}()
	newFeatureTable(map[int]string{featureMaskWords * 64: "beyond"})
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

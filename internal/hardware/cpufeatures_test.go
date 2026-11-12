// Copyright 2026 The Agent Substrate Authors
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
	"strings"
	"testing"
)

// TestEncodeCPUFeatureMask pins the cpu_features value of real `runsc
// cpu-features --bitmask` output, which stored snapshots must keep matching,
// and checks the edges of the block format.
func TestEncodeCPUFeatureMask(t *testing.T) {
	// runsc 20261102.0 on an Intel host: block 0 is CPUID.1:ECX (pni is bit
	// 0), block 1 CPUID.1:EDX (fpu is bit 0, mask bit 32), and so on, with
	// block 8 empty. Its names form, through the tables this package used
	// to map them with, encoded to the same value.
	const (
		blocks = "0xfefa3203,0x1f8bfbff,0x001c2ffb,0x00000000,0x00000001,0x00000121,0x2c100800,0xac000400,0x00000000"
		want   = "ac0004002c100800000001210000000100000000001c2ffb1f8bfbfffefa3203"
	)
	if got, err := EncodeCPUFeatureMask(blocks); err != nil || got != want {
		t.Errorf("EncodeCPUFeatureMask(%q) = (%q, %v), want (%q, nil)", blocks, got, err, want)
	}

	for _, tc := range []struct{ blocks, want string }{
		{"0x00000000", "0"},
		{"0x00000003\n", "3"},
		{"0x00000003,0x00000000,0x00000000", "3"}, // trailing zero blocks
		{"0x00000001,0x00000000,0x00000020", "20" + strings.Repeat("0", 15) + "1"}, // bits 0 and 69: a word boundary crossed
		{strings.Repeat("0x00000000,", featureMaskWords*2-1) + "0x80000000", "8" + strings.Repeat("0", featureMaskWords*16-1)},
	} {
		if got, err := EncodeCPUFeatureMask(tc.blocks); err != nil || got != tc.want {
			t.Errorf("EncodeCPUFeatureMask(%q) = (%q, %v), want (%q, nil)", tc.blocks, got, err, tc.want)
		}
	}
	for _, bad := range []string{
		"",
		"3",
		"0x",
		"0xg",
		"0x100000000",
		"0x00000001,",
		strings.Repeat("0x00000000,", featureMaskWords*2) + "0x00000001",
	} {
		if got, err := EncodeCPUFeatureMask(bad); err == nil {
			t.Errorf("EncodeCPUFeatureMask(%q) = %q, want an error", bad, got)
		}
	}
}

// TestParseFeatureMask checks that the canonical encoding round-trips and
// that nothing else parses: a worker or snapshot value in another spelling
// of the same set would compare unequal as a proto field.
func TestParseFeatureMask(t *testing.T) {
	// Bits 0, 1, 2 and 64: one word boundary crossed, low word zero-padded.
	m := featureMask{7, 1}
	if got, want := m.String(), "1"+strings.Repeat("0", 15)+"7"; got != want {
		t.Errorf("featureMask.String() = %q, want %q", got, want)
	}
	for _, s := range []string{m.String(), "0", "f", featureMask{^uint64(0)}.String()} {
		back, ok := parseFeatureMask(s)
		if !ok || back.String() != s {
			t.Errorf("parseFeatureMask(%q) = (%v, %v), want the mask that encodes to it", s, back, ok)
		}
	}
	for _, bad := range []string{"", "0x1f", "1F", "07", "xyz", strings.Repeat("f", featureMaskWords*16+1)} {
		if _, ok := parseFeatureMask(bad); ok {
			t.Errorf("parseFeatureMask(%q) succeeded, want a non-canonical value rejected", bad)
		}
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

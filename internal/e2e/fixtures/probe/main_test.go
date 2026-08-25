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
	"slices"
	"testing"
)

// The capabilities e2e assertions are only as trustworthy as this decoder and
// its name table, and that suite needs a cluster to run. These cases pin both
// against masks whose meaning is known independently.
func TestDecodeCapMask(t *testing.T) {
	tests := []struct {
		name string
		mask string
		want []string
	}{{
		name: "no capabilities",
		mask: "0000000000000000",
		want: []string{},
	}, {
		// The container-runtime default set (docker, containerd/CRI). Decoding
		// this to exactly its 14 well-known members is what validates the bit
		// positions in capabilityNames.
		name: "container-runtime default set",
		mask: "00000000a80425fb",
		want: []string{
			"CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL",
			"SETGID", "SETUID", "SETPCAP", "NET_BIND_SERVICE", "NET_RAW",
			"SYS_CHROOT", "MKNOD", "AUDIT_WRITE", "SETFCAP",
		},
	}, {
		// atelet's default set: KILL (5), NET_BIND_SERVICE (10), AUDIT_WRITE (29).
		name: "atelet default set",
		mask: "0000000020000420",
		want: []string{"KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"},
	}, {
		name: "single capability",
		mask: "0000000000000400",
		want: []string{"NET_BIND_SERVICE"},
	}, {
		// A bit past the end of the table must still be reported, so a kernel
		// newer than this table yields a readable diff, not a silent omission.
		name: "unknown capability bit",
		mask: "0000020000000000",
		want: []string{"CAP_41"},
	}, {
		name: "leading and trailing whitespace is tolerated",
		mask: "  0000000000000020\n",
		want: []string{"KILL"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCapMask(tt.mask)
			if err != nil {
				t.Fatalf("decodeCapMask(%q) failed: %v", tt.mask, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("decodeCapMask(%q) =\n  %v\nwant:\n  %v", tt.mask, got, tt.want)
			}
		})
	}
}

func TestDecodeCapMaskInvalid(t *testing.T) {
	if _, err := decodeCapMask("nothex"); err == nil {
		t.Error("decodeCapMask(\"nothex\") succeeded, want an error")
	}
}

// capabilityNames is indexed by capability value, so a wrong length means the
// table has drifted from <linux/capability.h> and every decoded name past the
// gap would be wrong.
func TestCapabilityNamesTable(t *testing.T) {
	const wantLen = 41 // CAP_CHOWN (0) .. CAP_CHECKPOINT_RESTORE (40)
	if len(capabilityNames) != wantLen {
		t.Errorf("len(capabilityNames) = %d, want %d", len(capabilityNames), wantLen)
	}
	for i, name := range capabilityNames {
		if name == "" {
			t.Errorf("capabilityNames[%d] is empty", i)
		}
	}
}

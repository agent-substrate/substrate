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

package imagecache

import (
	"slices"
	"strings"
	"testing"
)

func TestMountPointsIn(t *testing.T) {
	mountinfo := strings.Join([]string{
		`22 26 0:20 / /sys rw,nosuid,nodev shared:7 - sysfs sysfs rw`,
		`40 21 8:1 / /var/lib/ateom-gvisor/actors/a1/bundles/task/rootfs rw shared:1 - overlay overlay rw,lowerdir=/x`,
		`41 21 8:1 / /var/lib/ateom-gvisor/actors/a1/bundles/pause/rootfs rw - overlay overlay rw`,
		`42 21 8:1 / /var/lib/ateom-gvisor/actors/a2/bundles/task/rootfs rw - overlay overlay rw`,
		// Path with an escaped space; mountinfo octal-escapes it.
		`43 21 8:1 / /var/lib/ateom-gvisor/actors/a1/bundles/odd\040name/rootfs rw - overlay overlay rw`,
		// Prefix-sibling directory that must NOT match a1's subtree.
		`44 21 8:1 / /var/lib/ateom-gvisor/actors/a1-sibling/bundles/x/rootfs rw - overlay overlay rw`,
		`malformed line`,
		``,
	}, "\n")

	got, err := mountPointsIn(strings.NewReader(mountinfo), "/var/lib/ateom-gvisor/actors/a1/bundles")
	if err != nil {
		t.Fatalf("mountPointsIn: %v", err)
	}
	want := []string{
		"/var/lib/ateom-gvisor/actors/a1/bundles/task/rootfs",
		"/var/lib/ateom-gvisor/actors/a1/bundles/pause/rootfs",
		"/var/lib/ateom-gvisor/actors/a1/bundles/odd name/rootfs",
	}
	if !slices.Equal(got, want) {
		t.Errorf("mount points = %v, want %v", got, want)
	}
}

func TestMountPointsIn_ExactDirMatch(t *testing.T) {
	got, err := mountPointsIn(strings.NewReader(`40 21 8:1 / /mnt/target rw - ext4 /dev/sda1 rw`), "/mnt/target")
	if err != nil {
		t.Fatalf("mountPointsIn: %v", err)
	}
	if !slices.Equal(got, []string{"/mnt/target"}) {
		t.Errorf("mount points = %v, want the exact dir itself", got)
	}
}

func TestUnescapeMountPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{`/plain/path`, `/plain/path`},
		{`/with\040space`, `/with space`},
		{`/tab\011and\012newline`, "/tab\tand\nnewline"},
		{`/back\134slash`, `/back\slash`},
	}
	for _, tc := range tests {
		if got := unescapeMountPath(tc.in); got != tc.want {
			t.Errorf("unescapeMountPath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

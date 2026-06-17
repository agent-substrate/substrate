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

package kata

import (
	"strings"
	"testing"
)

// stockConfig mirrors the asset-path lines of kata's clh configuration.toml
// (the rest of the file is elided; RenderConfig only touches these lines).
const stockConfig = `[hypervisor.clh]
path = "/usr/local/bin/cloud-hypervisor"
kernel = "/opt/kata/share/kata-containers/vmlinux.container"
image = "/opt/kata/share/kata-containers/kata-containers.img"
valid_hypervisor_paths = ["/opt/kata/bin/cloud-hypervisor","/usr/local/bin/cloud-hypervisor"]
shared_fs = "virtio-fs"
virtio_fs_daemon = "/usr/local/bin/virtiofsd-patched"
valid_virtio_fs_daemon_paths = ["/usr/local/bin/virtiofsd-patched","/opt/kata/libexec/virtiofsd"]
`

func TestRenderConfig(t *testing.T) {
	a := ConfigAssets{
		Kernel:     "/var/lib/ateom-gvisor/static-files/vmlinux-abc",
		Image:      "/var/lib/ateom-gvisor/static-files/rootfs-def",
		Hypervisor: "/var/lib/ateom-gvisor/static-files/cloud-hypervisor-123",
		Virtiofsd:  "/var/lib/ateom-gvisor/static-files/virtiofsd-456",
	}
	out, err := RenderConfig([]byte(stockConfig), a)
	if err != nil {
		t.Fatalf("RenderConfig: %v", err)
	}
	got := string(out)

	wantLines := []string{
		`kernel = "` + a.Kernel + `"`,
		`image = "` + a.Image + `"`,
		`path = "` + a.Hypervisor + `"`,
		`valid_hypervisor_paths = ["` + a.Hypervisor + `"]`,
		`virtio_fs_daemon = "` + a.Virtiofsd + `"`,
		`valid_virtio_fs_daemon_paths = ["` + a.Virtiofsd + `"]`,
	}
	for _, w := range wantLines {
		if !strings.Contains(got, w) {
			t.Errorf("rendered config missing line %q\n--- got ---\n%s", w, got)
		}
	}
	// Untouched settings must survive.
	if !strings.Contains(got, `shared_fs = "virtio-fs"`) {
		t.Error("RenderConfig dropped shared_fs")
	}
	// No stale /opt/kata or default cloud-hypervisor paths remain.
	for _, stale := range []string{"/opt/kata/share", "/opt/kata/bin/cloud-hypervisor", "/opt/kata/libexec/virtiofsd"} {
		if strings.Contains(got, stale) {
			t.Errorf("rendered config still references stale path %q", stale)
		}
	}
}

func TestRenderConfigMissingField(t *testing.T) {
	// Base lacking virtio_fs_daemon -> error (fail loudly).
	base := `kernel = "/k"
image = "/i"
path = "/p"
valid_hypervisor_paths = ["/p"]
valid_virtio_fs_daemon_paths = ["/v"]
`
	_, err := RenderConfig([]byte(base), ConfigAssets{Kernel: "/k2", Image: "/i2", Hypervisor: "/p2", Virtiofsd: "/v2"})
	if err == nil {
		t.Fatal("expected error for missing virtio_fs_daemon field")
	}
}

func TestRenderConfigMissingAsset(t *testing.T) {
	_, err := RenderConfig([]byte(stockConfig), ConfigAssets{Kernel: "/k"}) // others empty
	if err == nil {
		t.Fatal("expected error for missing asset paths")
	}
}

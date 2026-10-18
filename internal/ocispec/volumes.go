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

package ocispec

import (
	"path/filepath"

	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// SubPathDir holds pinned per-mount sources, outside the sandbox's rootfs.
const SubPathDir = "subpaths"

type volumeBind struct {
	mount    *ateworkerpb.VolumeMount
	source   string
	readOnly bool
}

func volumeBinds(o Options) []volumeBind {
	var out []volumeBind
	add := func(mounts []*ateworkerpb.VolumeMount, root string, readOnly bool) {
		for _, m := range mounts {
			out = append(out, volumeBind{m, filepath.Join(root, m.GetName()), readOnly || m.GetReadOnly()})
		}
	}
	d, c := o.ActorDirs, o.Container
	add(c.GetDurableDirVolumeMounts(), d.GetDurableDirVolumeMountsDir(), false)
	add(c.GetExternalVolumeMounts(), d.GetExternalVolumeMountsDir(), false)
	add(c.GetSystemInfoVolumeMounts(), d.GetSystemInfoVolumeRootsDir(), true)
	add(c.GetImageVolumeMounts(), imagecache.ImageVolumeMountPath(filepath.Join(d.GetOciBundleDir(), c.GetName()), ""), true)
	return out
}

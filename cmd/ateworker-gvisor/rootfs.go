//go:build linux

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
	"path/filepath"

	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/imagecache"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// setupAppRootfs composes an application container's bundle rootfs, the base
// layer under the sandbox's restored rootfs writes, and writes its /etc files.
func setupAppRootfs(actorDirs *ateworkerpb.ActorDirs, containerName string) error {
	bundle := ociBundlePath(actorDirs, containerName)
	if err := imagecache.SetupBundleRootfs(bundle); err != nil {
		return err
	}
	return actornet.WriteRootfsEtcFiles(filepath.Join(bundle, "rootfs"), ocispec.Hostname)
}

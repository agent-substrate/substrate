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
	"context"
	"fmt"
	"os"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// hasCsiVolumes reports whether any container mounts a CSI volume.
func hasCsiVolumes(containers []*ateompb.Container) bool {
	for _, c := range containers {
		if len(c.GetCsiVolumeMounts()) > 0 {
			return true
		}
	}
	return false
}

// stageCsiVolumes bind-mounts the actor's host CSI volumes directory
// into the sandbox's shared virtio-fs tree at SharedDir(actorUID)/csi.
func (s *AteomService) stageCsiVolumes(ctx context.Context, actorUID string) error {
	src := ateompath.VolumesDir(actorUID)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("while checking CSI volumes dir %q: %w", src, err)
	}
	if err := kata.BindIntoShare(ctx, src, actorUID, ocispec.ShareCSI); err != nil {
		return fmt.Errorf("while binding CSI volumes into the shared tree: %w", err)
	}
	return nil
}

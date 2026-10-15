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

import "github.com/agent-substrate/substrate/internal/proto/ateworkerpb"

// The fidelity ladder, lowest to highest, each level including the ones
// below it: VOLUMES holds the durable volumes, ROOTFS adds each container's
// rootfs upper, MEMORY adds the guest. These two predicates are the only
// place the runtime spells out which levels carry which layer.

// includesRootfs reports whether snapshots at this fidelity carry the
// containers' rootfs uppers, so a checkpoint tars them and a restore puts
// them back.
func includesRootfs(f ateworkerpb.SnapshotFidelity) bool {
	return f == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS || f == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
}

// includesMemory reports whether snapshots at this fidelity carry the guest
// itself, so a restore resumes it (and its CPU counters) rather than
// cold-booting a fresh one.
func includesMemory(f ateworkerpb.SnapshotFidelity) bool {
	return f == ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
}

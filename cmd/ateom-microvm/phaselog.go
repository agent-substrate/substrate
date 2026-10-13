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

import "github.com/agent-substrate/substrate/internal/ateattr"

// The phase names, suffixed onto the ateomphaselog duration keys. Kept out of
// ateattr on purpose: that package's SnapshotPhase* values are the
// ate.snapshot.phase metric enum, and these are not values of it. "total" is
// shared so the two layers' records agree on the denominator.
//
// Checkpoint: snapshot, durable_dir and rootfs_upper run concurrently on the
// paused guest, so the paused window costs their max, not their sum; prep,
// pause and teardown are sequential around them. Restore: every phase is
// sequential and the phases partition the total.
const (
	phasePause       = "pause"
	phaseSnapshot    = "snapshot"
	phaseDurableDir  = "durable_dir"
	phaseRootfsUpper = "rootfs_upper"
	phaseTeardown    = "teardown"

	phasePrep        = "prep"
	phaseBundles     = "bundles"
	phaseUpperJoin   = "upper_join"
	phaseLowers      = "lowers"
	phaseTap         = "tap"
	phaseVMMLaunch   = "vmm_launch"
	phaseVMRestore   = "vm_restore"
	phaseResume      = "resume"
	phaseWakeupProbe = "wakeup_probe"

	phaseTotal = ateattr.SnapshotPhaseTotal
)

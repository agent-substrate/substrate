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
	"time"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/ateworker/phaselog"
)

// The phase names, suffixed onto the phaselog duration keys. Kept out of
// ateattr on purpose: that package's SnapshotPhase* values are the
// ate.snapshot.phase metric enum, and these are not values of it. "total" is
// shared so the two layers' records agree on the denominator.
//
// Every phase of both operations is sequential, so the phases partition the
// total up to the few microseconds between them. The app_* phases are summed
// over the application containers, which are restored one after another; the
// container count rides along on the record so a reader can divide.
const (
	phasePrep           = "prep"
	phaseEgressPrepare  = "egress_prepare"
	phaseNetSetup       = "net_setup"
	phaseSandboxCreate  = "sandbox_create"
	phaseSandboxRestore = "sandbox_restore"
	phaseAppRootfs      = "app_rootfs"
	phaseAppCreate      = "app_create"
	phaseAppRestore     = "app_restore"
	phaseWakeupProbe    = "wakeup_probe"
	phaseActivate       = "activate"

	phaseCheckpoint   = "checkpoint"
	phaseFSCheckpoint = "fs_checkpoint"
	phaseTeardown     = "teardown"

	phaseTotal = ateattr.SnapshotPhaseTotal
)

// containerCountKey carries the number of application containers on the
// restore record, since the app_* phases are sums over them.
const containerCountKey = "ate.actor.container.count"

// restoreTiming is the elapsed time of each RestoreWorkload phase. A phase
// left at zero never ran. Under VOLUMES and ROOTFS fidelity, sandboxRestore and
// appRestore time the cold start that stands in for the restore. activate is
// the total time spent activating the tunnel: egress is activated before the
// containers restore and ingress after the wakeup probe, and both are counted.
type restoreTiming struct {
	prep, egressPrepare, netSetup    time.Duration
	sandboxCreate, sandboxRestore    time.Duration
	appRootfs, appCreate, appRestore time.Duration
	wakeupProbe, activate, total     time.Duration
}

func (t restoreTiming) phases() []phaselog.Phase {
	return []phaselog.Phase{
		{Name: phasePrep, D: t.prep},
		{Name: phaseEgressPrepare, D: t.egressPrepare},
		{Name: phaseNetSetup, D: t.netSetup},
		{Name: phaseSandboxCreate, D: t.sandboxCreate},
		{Name: phaseSandboxRestore, D: t.sandboxRestore},
		{Name: phaseAppRootfs, D: t.appRootfs},
		{Name: phaseAppCreate, D: t.appCreate},
		{Name: phaseAppRestore, D: t.appRestore},
		{Name: phaseWakeupProbe, D: t.wakeupProbe},
		{Name: phaseActivate, D: t.activate},
		{Name: phaseTotal, D: t.total},
	}
}

// checkpointTiming is the elapsed time of each CheckpointWorkload phase.
// MEMORY fidelity runs runsc checkpoint (recorded under checkpoint). VOLUMES
// and ROOTFS both run runsc fscheckpoint (recorded under fs_checkpoint);
// ate.snapshot.fidelity tells them apart.
type checkpointTiming struct {
	prep, checkpoint, fsCheckpoint, teardown, total time.Duration
}

func (t checkpointTiming) phases() []phaselog.Phase {
	return []phaselog.Phase{
		{Name: phasePrep, D: t.prep},
		{Name: phaseCheckpoint, D: t.checkpoint},
		{Name: phaseFSCheckpoint, D: t.fsCheckpoint},
		{Name: phaseTeardown, D: t.teardown},
		{Name: phaseTotal, D: t.total},
	}
}

// lap returns the time since *last and advances it, so consecutive calls
// measure consecutive phases without a timestamp per boundary.
func lap(last *time.Time) time.Duration {
	now := time.Now()
	d := now.Sub(*last)
	*last = now
	return d
}

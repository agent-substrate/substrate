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
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The keys the per-phase durations are logged under. They are named like
// instruments but deliberately are not ones: the phases are implementation
// details of this binary, so they stay a developer-facing log record rather
// than becoming metric API. They decompose the ateom_restore /
// ateom_checkpoint phases of atelet's ate.actor.*.duration histograms, whose
// names they extend, and are joined per actor by benchmarking tooling.
const (
	restoreDurationKey    = "ateom.actor.restore.duration"
	checkpointDurationKey = "ateom.actor.checkpoint.duration"
)

// The phase names, suffixed onto the duration keys. Kept out of ateattr on
// purpose: that package's SnapshotPhase* values are the ate.snapshot.phase
// metric enum, and these are not values of it. "total" is shared so the two
// layers' records agree on the denominator.
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

// phase is one timed step of a snapshot operation. A zero duration means the
// phase never ran (a Data-scope checkpoint captures no guest) and is skipped
// rather than logged as instant.
type phase struct {
	name string
	d    time.Duration
}

// scopeLogValue maps the ateom wire enum onto the shared scope label values,
// the same way ateattr.SnapshotScopeValue does for the atelet enum. An
// unrecognized scope reports as unknown rather than stringified, so no wire
// value can widen the value set readers key on.
func scopeLogValue(scope ateompb.SnapshotScope) string {
	switch scope {
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL:
		return ateattr.SnapshotScopeFull
	case ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		return ateattr.SnapshotScopeData
	default:
		return ateattr.SnapshotScopeUnknown
	}
}

// snapshotPhaseAttrs renders one joinable per-actor record for a checkpoint
// or restore, mirroring atelet's snapshotLogAttrs: full actor identity, the
// scope, and one float-seconds attr per non-zero phase under
// durationKey.<phase>. One record carries every phase of the operation, so a
// reader never joins two half-records that disagree about the same actor.
//
// err is the operation's outcome. A failed operation still records the phases
// it completed, marked with error.type (the gRPC code, context errors as
// DeadlineExceeded / Canceled) so a reader can leave it out of a latency
// distribution. Absence means success.
func snapshotPhaseAttrs(a resources.ActorAttribution, scope ateompb.SnapshotScope, durationKey string, err error, phases []phase) []slog.Attr {
	attrs := ateattr.ActorLogAttrs(a)
	attrs = append(attrs, slog.String(string(ateattr.SnapshotScopeKey), scopeLogValue(scope)))
	if err != nil {
		code := status.Code(err)
		if code == codes.Unknown {
			code = status.FromContextError(err).Code()
		}
		attrs = append(attrs, slog.String(string(ateattr.ErrorTypeKey), code.String()))
	}
	for _, p := range phases {
		if p.d == 0 {
			continue
		}
		// Seconds and not slog.Duration's nanoseconds: the values sit under
		// the atelet histograms' unit (s), so readers compare them without a
		// per-key unit table.
		attrs = append(attrs, slog.Float64(durationKey+"."+p.name, p.d.Seconds()))
	}
	return attrs
}

// logSnapshotPhases emits the snapshotPhaseAttrs record under msg.
func logSnapshotPhases(ctx context.Context, msg string, a resources.ActorAttribution, scope ateompb.SnapshotScope, durationKey string, err error, phases []phase) {
	slog.LogAttrs(ctx, slog.LevelInfo, msg, snapshotPhaseAttrs(a, scope, durationKey, err, phases)...)
}

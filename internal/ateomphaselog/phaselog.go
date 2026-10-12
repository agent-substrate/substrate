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

// Package ateomphaselog renders the per-actor "timing breakdown" log record an
// ateom emits for each checkpoint and restore. Every ateom binary times its
// own phases, which are implementation details of that runtime; this package
// holds the shape they share so the records of all ateoms carry the same
// identity, fidelity and key layout, and atelet's own record joins to any of them.
package ateomphaselog

import (
	"context"
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/status"
)

// The keys the per-phase durations are logged under. They are named like
// instruments but deliberately are not ones: the phases are implementation
// details of each ateom, so they stay a developer-facing log record rather
// than becoming metric API. They decompose the ateom_restore /
// ateom_checkpoint phases of atelet's ate.actor.*.duration histograms, whose
// names they extend, and are joined per actor by benchmarking tooling.
const (
	RestoreDurationKey    = "ateom.actor.restore.duration"
	CheckpointDurationKey = "ateom.actor.checkpoint.duration"
)

// Phase is one timed step of a snapshot operation. A zero duration means the
// phase never ran (a VOLUMES checkpoint captures no guest) and is skipped
// rather than logged as instant. Name is suffixed onto the duration key; each
// ateom defines its own names, with ateattr.SnapshotPhaseTotal shared so the
// two layers' records agree on the denominator.
type Phase struct {
	Name string
	D    time.Duration
}

// FidelityLogValue maps the ateom wire enum onto the shared fidelity label
// values, the same way ateattr.SnapshotFidelityValue does for the atelet
// enum. An unrecognized fidelity reports as unknown rather than stringified,
// so no wire value can widen the value set readers key on.
func FidelityLogValue(fidelity ateompb.SnapshotFidelity) string {
	switch fidelity {
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateattr.SnapshotFidelityVolumes
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateattr.SnapshotFidelityRootfs
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateattr.SnapshotFidelityMemory
	default:
		return ateattr.SnapshotFidelityUnknown
	}
}

// SnapshotPhaseAttrs renders one joinable per-actor record for a checkpoint
// or restore, mirroring atelet's snapshotLogAttrs: full actor identity, the
// scope, and one float-seconds attr per non-zero phase under
// durationKey.<phase>. One record carries every phase of the operation, so a
// reader never joins two half-records that disagree about the same actor.
//
// err is the operation's outcome. A failed operation still records the phases
// it completed, marked with error.type (the gRPC code, context errors as
// DeadlineExceeded / Canceled) so a reader can leave it out of a latency
// distribution. Absence means success.
func SnapshotPhaseAttrs(a resources.ActorAttribution, fidelity ateompb.SnapshotFidelity, durationKey string, err error, phases []Phase) []slog.Attr {
	attrs := ateattr.ActorLogAttrs(a)
	attrs = append(attrs, slog.String(string(ateattr.SnapshotFidelityKey), FidelityLogValue(fidelity)))
	if err != nil {
		s, ok := apierror.FromError(err)
		code := s.Code()
		if !ok {
			code = status.Code(err)
		}
		attrs = append(attrs, slog.String(string(ateattr.ErrorTypeKey), code.String()))
	}
	for _, p := range phases {
		if p.D == 0 {
			continue
		}
		// Seconds and not slog.Duration's nanoseconds: the values sit under
		// the atelet histograms' unit (s), so readers compare them without a
		// per-key unit table.
		attrs = append(attrs, slog.Float64(durationKey+"."+p.Name, p.D.Seconds()))
	}
	return attrs
}

// LogSnapshotPhases emits the SnapshotPhaseAttrs record under msg.
func LogSnapshotPhases(ctx context.Context, msg string, a resources.ActorAttribution, fidelity ateompb.SnapshotFidelity, durationKey string, err error, phases []Phase) {
	slog.LogAttrs(ctx, slog.LevelInfo, msg, SnapshotPhaseAttrs(a, fidelity, durationKey, err, phases)...)
}

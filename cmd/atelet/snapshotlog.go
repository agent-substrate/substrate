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
	"errors"
	"log/slog"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

// errRestoreUnwound stands in for a restore that left through a panic, where the
// named error is still nil. Carries no reason, so it reports as UNKNOWN.
var errRestoreUnwound = errors.New("restore did not run to completion")

// snapshotLogAttrs renders what recordPhases measures as a per-actor record. The
// histograms cannot be one: actor identity is barred from metric labels
// (docs/metrics/substrate.yaml, cardinality_rules.no-actor-identity), so the
// phase breakdown reaches a backend keyed by template only.
//
// durationKey is the name of the instrument these phases also feed, which is why
// the values are seconds and not the nanoseconds slog.Duration writes: that
// instrument declares unit s. Identity stays out of snapshotOp so no edit here
// can route it into a datapoint.
func snapshotLogAttrs(a resources.ActorAttribution, op snapshotOp, durationKey string, err error, phases []phase) []slog.Attr {
	attrs := ateattr.ActorLogAttrs(a)

	// Borrow the metric's dimensions rather than rebuild them: op.attrs already
	// drops what is unknown and bounds the sandbox class. The template pair is
	// identity here and a dimension there, and a key emitted twice leaves
	// consumers to disagree about which copy wins.
	seen := make(map[string]struct{}, len(attrs))
	for _, attr := range attrs {
		seen[attr.Key] = struct{}{}
	}
	for _, kv := range op.attrs() {
		if _, dup := seen[string(kv.Key)]; dup {
			continue
		}
		attrs = append(attrs, slog.String(string(kv.Key), kv.Value.String()))
	}

	// Absence means success, as on the instruments. There is no ate.snapshot.phase:
	// on a datapoint it names the one step timed, and this record carries them all.
	if err != nil {
		attrs = append(attrs, slog.String(string(ateattr.FailureReasonKey), ateattr.FailureReason(err)))
	}

	for _, p := range phases {
		if p.d == 0 {
			continue
		}
		attrs = append(attrs, slog.Float64(durationKey+"."+p.name, p.d.Seconds()))
	}
	return attrs
}

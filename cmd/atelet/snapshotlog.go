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
	"log/slog"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// snapshotLogAttrs renders what recordPhases measures as a per-actor record. The
// histograms cannot be one: actor identity is barred from metric labels
// (docs/metrics/substrate.yaml, cardinality_rules.no-actor-identity), so the
// phase breakdown reaches a backend keyed by template only.
//
// durationKey is the name of the instrument these phases also feed, which is why
// the values are seconds and not the nanoseconds slog.Duration writes: that
// instrument declares unit s. Identity stays out of snapshotOp so no edit here
// can route it into a datapoint.
//
// err is the operation's outcome. The record is written on the way out of a
// failed operation too, so that its completed phases are not lost; error.type
// (the gRPC code, as on the ateapi instruments) marks it so a reader does not
// average a timed-out download in with the successful ones. Absence means
// success, as on the instruments.
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

	if err != nil {
		// Only an error that came back over gRPC carries a status; atelet's
		// own failures are plain errors, and the ones worth telling apart are
		// the context ones (a timed-out download, a cancelled restore).
		code := status.Code(err)
		if code == codes.Unknown {
			code = status.FromContextError(err).Code()
		}
		attrs = append(attrs, slog.String(string(ateattr.ErrorTypeKey), code.String()))
	}

	// There is no ate.snapshot.phase key: on a datapoint it names the one step
	// timed, and this record carries them all.
	for _, p := range phases {
		if p.d == 0 {
			continue
		}
		attrs = append(attrs, slog.Float64(durationKey+"."+p.name, p.d.Seconds()))
	}
	return attrs
}

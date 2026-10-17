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

package snapshottracker

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

// Report settings. A report runs on the worker that ran the upload or detach,
// so these bound how long a control plane that is down or busy holds that
// worker: about reportAttempts*reportAttemptTimeout plus the retry delays.
const (
	// reportAttempts is how many times a worker tries to report a state.
	reportAttempts = 3
	// reportAttemptTimeout bounds each attempt.
	reportAttemptTimeout = 3 * time.Second
	// reportRetryDelay is how long a worker waits before its second attempt.
	// Each further attempt waits twice as long as the one before.
	reportRetryDelay = 500 * time.Millisecond
)

// report tells the control plane the actor's snapshot state after a
// background operation of kind cause succeeded. It reads the state afresh for
// each attempt, so a report carries what atelet holds when it is sent, and it
// never holds the actor's lock while it waits on the control plane.
//
// It is best-effort: it gives up after reportAttempts, on an error the control
// plane says not to retry, or once the tracker's context ends. A report that is
// dropped loses nothing for good, since every later Checkpoint,
// PromoteSnapshot, or detach returns or reports the actor's whole state.
func (t *Tracker) report(actorUID string, actor resources.ActorRef, cause opKind) {
	if t.reporter == nil {
		return
	}
	if actor == (resources.ActorRef{}) {
		// The control plane names actors by atespace and name, which the
		// snapshot's commit records alongside it.
		slog.WarnContext(t.ctx, "Not reporting snapshot state: the snapshot names no actor",
			slog.String("actorUID", actorUID), slog.String("cause", cause.String()))
		return
	}
	delay := reportRetryDelay
	for attempt := 1; ; attempt++ {
		st := t.State(actorUID)
		start := t.clock.Now()
		ctx, cancel := context.WithTimeout(t.ctx, reportAttemptTimeout)
		err := t.reporter.ReportActorSnapshotState(ctx, actor, actorUID, st)
		cancel()
		retry := retryableReportErr(err) && attempt < reportAttempts && t.ctx.Err() == nil
		logReport(t.ctx, actorUID, actor, cause, attempt, t.clock.Since(start), st, err, retry)
		if !retry {
			return
		}
		select {
		case <-t.ctx.Done():
			return
		case <-t.clock.After(delay):
		}
		delay *= 2
	}
}

// retryableReportErr reports whether a failed report is worth repeating: the
// control plane was busy with the actor or could not be reached in time.
// ReportActorSnapshotState asks not to retry NOT_FOUND, FAILED_PRECONDITION,
// or INVALID_ARGUMENT.
func retryableReportErr(err error) bool {
	switch status.Code(err) {
	case codes.Aborted, codes.Unavailable, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// reportOutcome labels how a report attempt ended, for logs.
func reportOutcome(err error, retry bool) string {
	switch {
	case err == nil:
		return "ok"
	case retry:
		return "retry"
	case status.Code(err) == codes.NotFound:
		return "not_found"
	case retryableReportErr(err):
		return "gave_up"
	default:
		return "rejected"
	}
}

// logReport logs one report attempt: at info level when it succeeded, will be
// retried, or found the actor gone from this node, and as a warning when the
// state is dropped otherwise.
func logReport(ctx context.Context, actorUID string, actor resources.ActorRef, cause opKind, attempt int, elapsed time.Duration, st *ateletpb.ActorSnapshotState, err error, retry bool) {
	outcome := reportOutcome(err, retry)
	attrs := []any{
		slog.String("actorUID", actorUID),
		slog.Any("actor", actor),
		slog.String("cause", cause.String()),
		slog.Int("attempt", attempt),
		slog.Duration("elapsed", elapsed),
		slog.String("code", status.Code(err).String()),
		slog.String("outcome", outcome),
		slog.Int("latest_local_generation", int(st.GetLatestLocal().GetSnapshot().GetGeneration())),
		slog.Int("uploading_generation", int(st.GetUploading().GetSnapshot().GetGeneration())),
		slog.Int("latest_durable_generation", int(st.GetLatestDurable().GetSnapshot().GetGeneration())),
	}
	switch outcome {
	case "ok", "retry", "not_found":
		if err != nil {
			attrs = append(attrs, slog.Any("err", err))
		}
		slog.InfoContext(ctx, "Reported snapshot state", attrs...)
	default:
		slog.WarnContext(ctx, "Failed to report snapshot state", append(attrs, slog.Any("err", err))...)
	}
}

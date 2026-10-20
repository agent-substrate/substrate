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
	"math/rand/v2"
	"time"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
)

// Report settings. DefaultReportWorkers and DefaultReportGiveUpAfter are
// overridden by atelet's --snapshot-report-workers and
// --snapshot-report-give-up-after.
const (
	// DefaultReportWorkers is how many reports run at once on a node by
	// default.
	DefaultReportWorkers = 1
	// reportTimeout bounds one attempt to report an actor's state.
	reportTimeout = 10 * time.Second
	// reportRetryDelay is how long atelet holds an actor's report back after
	// an attempt failed, and the base delay for the report queue after a
	// control-plane failure. Each further consecutive failure doubles it, up
	// to maxReportRetryDelay, until DefaultReportGiveUpAfter (or
	// WithReportGiveUpAfter) has passed since the report became due.
	reportRetryDelay    = 5 * time.Second
	maxReportRetryDelay = 5 * time.Minute
	// maxReportQueueRetryDelay caps how long the report queue pauses after
	// consecutive control-plane failures, so reports resume soon after
	// ate-api-server recovers.
	maxReportQueueRetryDelay = 30 * time.Second
	DefaultReportGiveUpAfter = 10 * time.Minute
	// reportStartupJitter spreads the reports atelet recovers from disk at
	// startup, so a fleet-wide restart does not hit the control plane at
	// once.
	reportStartupJitter = 30 * time.Second
)

// Reporter tells the control plane what atelet holds of an actor's snapshots.
type Reporter interface {
	// ReportActorSnapshotState reports state, the actor's snapshot state on
	// this node. Each report carries the whole state, so repeating one is
	// safe.
	ReportActorSnapshotState(ctx context.Context, actorUID string, actor resources.ActorRef, state *ateletpb.ActorSnapshotState) error
}

// reportQueue holds the queue and settings for reporting snapshot state
// changes to the control plane.
type reportQueue struct {
	// reporter tells the control plane what atelet holds of an actor's
	// snapshots. Nil without WithReporter, or when workers is 0.
	reporter Reporter
	// workers is how many report workers drain queue: DefaultReportWorkers
	// unless WithReportWorkers sets it.
	workers int
	// giveUpAfter is how long atelet keeps retrying a failed report since it
	// became due before giving up on it: DefaultReportGiveUpAfter unless
	// WithReportGiveUpAfter sets it.
	giveUpAfter time.Duration
	// queue holds the UIDs of actors whose snapshot state has a report due.
	queue workqueue.TypedDelayingInterface[string]
	// retries says how long an actor's report is held back after a failure:
	// longer with each consecutive failure for the actor.
	retries workqueue.TypedRateLimiter[string]
	// queueRetries says how long the whole report queue pauses after a
	// control-plane failure, so actors do not all hit an overloaded ateapi at
	// once.
	queueRetries workqueue.TypedRateLimiter[string]
	// startupJitter is the window over which saved reports are spread at
	// startup: reportStartupJitter unless tests override it.
	startupJitter time.Duration
}

func newReportQueue(clk clock.WithTicker) reportQueue {
	return reportQueue{
		workers:       DefaultReportWorkers,
		giveUpAfter:   DefaultReportGiveUpAfter,
		queue:         workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Clock: clk}),
		retries:       workqueue.NewTypedItemExponentialFailureRateLimiter[string](reportRetryDelay, maxReportRetryDelay),
		queueRetries:  workqueue.NewTypedItemExponentialFailureRateLimiter[string](reportRetryDelay, maxReportQueueRetryDelay),
		startupJitter: reportStartupJitter,
	}
}

// markReportDueLocked records that a background upload or detach just made a
// change to report, starting a fresh giveUpAfter window and retry backoff.
func (t *Tracker) markReportDueLocked(a *actorSnapshots) {
	a.ReportDueSince = t.clock.Now()
	t.reporting.retries.Forget(a.actorUID)
}

// cancelReportLocked clears any pending report of the actor's state and its
// retry backoff. The caller saves the actor's state.
func (t *Tracker) cancelReportLocked(a *actorSnapshots) {
	a.ReportDueSince = time.Time{}
	a.reportDeferred = false
	t.reporting.retries.Forget(a.actorUID)
}

// queueReportLocked queues the actor's report, if one is due. While a Run,
// Restore, Checkpoint, PromoteSnapshot, or Terminate holds the actor, it
// defers the report until the hold ends.
func (t *Tracker) queueReportLocked(a *actorSnapshots) {
	if t.reporting.reporter == nil || !a.reportDue() {
		return
	}
	if a.hold != holdNone {
		a.reportDeferred = true
		return
	}
	a.reportDeferred = false
	t.reporting.queue.Add(a.actorUID)
}

// enqueueSavedReportLocked queues a saved actor's report at startup after a
// random delay up to startupJitter, so a fleet-wide atelet restart does not
// hit the control plane at once.
func (t *Tracker) enqueueSavedReportLocked(a *actorSnapshots) {
	if t.reporting.reporter == nil || !a.reportDue() {
		return
	}
	var delay time.Duration
	if t.reporting.startupJitter > 0 {
		delay = time.Nanosecond + rand.N(t.reporting.startupJitter)
	}
	t.reporting.queue.AddAfter(a.actorUID, delay)
}

// runReporter sends the reports of the actors it takes from the report queue,
// until the queue shuts down.
func (t *Tracker) runReporter() {
	for {
		actorUID, shutdown := t.reporting.queue.Get()
		if shutdown {
			return
		}
		delay := t.report(actorUID)
		t.reporting.queue.Done(actorUID)
		if !t.waitReportQueue(delay) {
			return
		}
	}
}

// waitReportQueue pauses the report worker for delay, or returns false if the
// tracker shuts down first.
func (t *Tracker) waitReportQueue(delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	timer := t.clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C():
		return true
	case <-t.ctx.Done():
		return false
	}
}

// report sends the actor's state to the control plane if a report is due, and
// returns how long the report queue should pause before the next report. A
// report the control plane acknowledges or refuses is no longer due, unless a
// background change was made while it ran. Any other failure is retried after
// a backoff, until a commit or detach cancels it or giveUpAfter has passed
// since the report became due; a failure other than Aborted also backs off
// the whole report queue.
func (t *Tracker) report(actorUID string) time.Duration {
	actor, sent, dueSince, ok := t.beginReport(actorUID)
	if !ok {
		return 0
	}
	ctx, cancel := context.WithTimeout(t.ctx, reportTimeout)
	err := t.reporting.reporter.ReportActorSnapshotState(ctx, actorUID, actor, sent)
	cancel()
	attrs := []any{
		slog.String("actorUID", actorUID),
		slog.Any("actor", actor),
		slog.String("latestLocal", sent.GetLatestLocal().GetSnapshot().GetSnapshotUuid()),
		slog.String("latestDurable", sent.GetLatestDurable().GetSnapshot().GetSnapshotUuid()),
	}
	switch {
	case err == nil:
		slog.Info("Reported the actor's snapshot state", attrs...)
	case reportRetryable(err):
		if t.ctx.Err() != nil {
			// atelet is shutting down: leave the report due for the next
			// atelet to send.
			slog.Info("Failed to report the actor's snapshot state", append(attrs, slog.Any("err", err))...)
			return 0
		}
		var queueDelay time.Duration
		if status.Code(err) == codes.Aborted {
			// The control plane answered; only this actor conflicted.
			t.reporting.queueRetries.Forget("")
		} else {
			queueDelay = jitteredReportDelay(t.reporting.queueRetries.When(""))
		}
		t.retryReport(actorUID, dueSince, attrs, err)
		return queueDelay
	case status.Code(err) == codes.NotFound || status.Code(err) == codes.Unimplemented:
		// NotFound is expected once the actor is deleted or a suspend has
		// released it from this node; Unimplemented is expected during a
		// rolling upgrade while ate-api-server is older than atelet.
		slog.Info("The control plane refused the actor's snapshot state", append(attrs, slog.Any("err", err))...)
	default:
		slog.Warn("The control plane refused the actor's snapshot state", append(attrs, slog.Any("err", err))...)
	}
	t.reporting.queueRetries.Forget("")
	t.reporting.retries.Forget(actorUID)
	t.endReport(actorUID, sent)
	return 0
}

// jitteredReportDelay returns a random delay in [d/2, d), so nodes that hit
// the same control-plane failure do not retry in lockstep.
func jitteredReportDelay(d time.Duration) time.Duration {
	if half := d / 2; half > 0 {
		return half + rand.N(d-half)
	}
	return d
}

// beginReport returns the actor, the state to report for it, and when the
// report became due, or false when no report is due or a Run, Restore,
// Checkpoint, PromoteSnapshot, or Terminate of the actor is in progress.
func (t *Tracker) beginReport(actorUID string) (resources.ActorRef, *ateletpb.ActorSnapshotState, time.Time, bool) {
	if !t.locks.Lock(t.ctx, actorUID) {
		// atelet is shutting down.
		return resources.ActorRef{}, nil, time.Time{}, false
	}
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !a.reportDue() {
		t.reporting.retries.Forget(actorUID)
		return resources.ActorRef{}, nil, time.Time{}, false
	}
	if a.hold != holdNone {
		// A Run, Restore, Checkpoint, PromoteSnapshot, or Terminate of the
		// actor is in progress, and queues the report when it ends (see
		// holdLocked).
		a.reportDeferred = true
		return resources.ActorRef{}, nil, time.Time{}, false
	}
	a.reportDeferred = false
	return a.latestDurableActor(), t.stateLocked(a), a.ReportDueSince, true
}

// endReport records that the control plane acknowledged or refused a report
// of sent, so the report is no longer due. If a background change modified
// the actor's state while sent was in flight, it queues the actor to report
// again instead.
func (t *Tracker) endReport(actorUID string, sent *ateletpb.ActorSnapshotState) {
	t.lock(actorUID)
	defer t.unlock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !a.reportDue() {
		return
	}
	if !sameReport(sent, t.stateLocked(a)) {
		t.queueReportLocked(a)
		return
	}
	t.clearReportDueLocked(a)
}

// retryReport schedules another attempt to send the report that became due at
// dueSince after err, unless a commit or detach canceled it, a newer
// background change replaced it, or giveUpAfter has passed.
func (t *Tracker) retryReport(actorUID string, dueSince time.Time, attrs []any, err error) {
	t.lock(actorUID)
	a := t.findLocked(actorUID)
	if a == nil || !a.reportDue() {
		t.reporting.retries.Forget(actorUID)
		t.unlock(actorUID)
		return
	}
	if !a.ReportDueSince.Equal(dueSince) {
		t.queueReportLocked(a)
		t.unlock(actorUID)
		return
	}
	now := t.clock.Now()
	if !now.Before(dueSince.Add(t.reporting.giveUpAfter)) {
		t.clearReportDueLocked(a)
		t.unlock(actorUID)
		slog.Warn("Giving up on reporting the actor's snapshot state", append(attrs, slog.Duration("after", now.Sub(dueSince)), slog.Any("err", err))...)
		return
	}
	delay := t.reporting.retries.When(actorUID)
	t.unlock(actorUID)

	level := slog.LevelWarn
	if status.Code(err) == codes.Aborted {
		// Another control-plane operation is changing the actor.
		level = slog.LevelInfo
	}
	slog.Log(t.ctx, level, "Failed to report the actor's snapshot state", append(attrs, slog.Duration("retryAfter", delay), slog.Any("err", err))...)
	t.reporting.queue.AddAfter(actorUID, delay)
}

// clearReportDueLocked clears ReportDueSince, resets the report backoff, and
// saves the actor's state.
func (t *Tracker) clearReportDueLocked(a *actorSnapshots) {
	t.addLocked(a)
	t.cancelReportLocked(a)
	t.saveLocked(a)
	t.requeueLocked(a)
}

// sameReport reports whether a and b name the same LatestLocal and
// LatestDurable. It ignores uploading and upload_due_time, whose changes do
// not make a report due.
func sameReport(a, b *ateletpb.ActorSnapshotState) bool {
	return proto.Equal(a.GetLatestLocal(), b.GetLatestLocal()) &&
		proto.Equal(a.GetLatestDurable(), b.GetLatestDurable())
}

// reportRetryable reports whether a report that failed with err is worth
// sending again. The control plane refuses a report with NotFound once the
// actor is gone or no longer on this node, with FailedPrecondition or
// InvalidArgument when it conflicts with the actor's record, and with
// Unimplemented when it does not serve reports: sending it again only repeats
// the refusal. Anything else, such as Aborted while another operation changes
// the actor, or Unavailable, can pass on a later attempt.
func reportRetryable(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument, codes.Unimplemented:
		return false
	default:
		return true
	}
}

// Copyright 2026 The Agent Substrate Authors
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
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/clock"
)

// Background upload, detach, and report settings, matching atelet's defaults.
// A failed detach is retried after detachRetryDelay each time, without
// atelet's exponential backoff.
const (
	defaultUploadDelay  = 5 * time.Minute
	defaultDetachDelay  = 30 * time.Minute
	detachRetryDelay    = time.Minute
	reportTimeout       = 10 * time.Second
	reportRetryDelay    = 5 * time.Second
	maxReportRetryDelay = 5 * time.Minute
	reportGiveUpAfter   = 10 * time.Minute
	uploadWorkers       = 2
	detachWorkers       = 2
	reportWorkers       = 1
)

// actorSnapshots records each actor's latest local and durable snapshot, as
// atelet's snapshot tracker does, so Checkpoint and PromoteSnapshot report the
// state atelet would. ate-api-server keeps an actor on its node while this
// state names a local copy.
//
// With a snapshotReporter on the herder, it also runs deferred uploads after
// defaultUploadDelay and detaches suspended actors after defaultDetachDelay,
// reporting each change through WorkerService.ReportActorSnapshotState. Like
// atelet, it runs at most uploadWorkers uploads, detachWorkers detaches, and
// reportWorkers report attempts at once.
//
// It is kept in memory only. The zero value is ready to use.
type actorSnapshots struct {
	mu     sync.Mutex
	actors map[string]*actorSnapshot // by actor UID

	wgMu   sync.RWMutex
	closed atomic.Bool
	wg     sync.WaitGroup

	// uploads, detaches, and reports are semaphores, made on first use.
	semOnce                    sync.Once
	uploads, detaches, reports chan struct{}
}

type actorSnapshot struct {
	actor *ateapi.ObjectRef
	state ateletpb.ActorSnapshotState

	// seq increments on each local commit, start, or drop so a background
	// detach stops if the actor changed since the detach was scheduled.
	seq uint64
	// uploadTimer fires when a deferred upload of LatestLocal is due. It is
	// started on the first deferred checkpoint since LatestDurable and kept
	// across later deferred checkpoints so an actor that suspends and resumes
	// repeatedly still uploads within defaultUploadDelay.
	uploadTimer clock.Timer
	// detachTimer fires when a background detach of the suspended actor is
	// due. Run and Restore stop it; each Checkpoint resets it.
	detachTimer clock.Timer
	// op is the in-flight background upload or detach, if any.
	op *backgroundOp
	// cancelReport cancels an in-flight ReportActorSnapshotState retry loop
	// when a newer commit supersedes the reported state.
	cancelReport context.CancelFunc
}

// backgroundOp is a background upload or detach of an actor's snapshot.
type backgroundOp struct {
	detach bool
	cancel context.CancelFunc
}

func (a *actorSnapshot) uploading() bool {
	return a.op != nil && !a.op.detach
}

func (a *actorSnapshot) stopUploadTimer() {
	if a.uploadTimer != nil {
		a.uploadTimer.Stop()
		a.uploadTimer = nil
	}
}

func (a *actorSnapshot) stopDetachTimer() {
	if a.detachTimer != nil {
		a.detachTimer.Stop()
		a.detachTimer = nil
	}
}

func (a *actorSnapshot) stopOp() {
	if a.op != nil {
		a.op.cancel()
		a.op = nil
	}
}

// endOp clears op if it is still the actor's in-flight op, and reports
// whether it was.
func (a *actorSnapshot) endOp(op *backgroundOp) bool {
	if a.op != op {
		return false
	}
	a.stopOp()
	return true
}

func (a *actorSnapshot) stopReport() {
	if a.cancelReport != nil {
		a.cancelReport()
		a.cancelReport = nil
	}
}

func (a *actorSnapshot) stopAll() {
	a.stopUploadTimer()
	a.stopDetachTimer()
	a.stopOp()
	a.stopReport()
}

// commitLocal records snap as the actor's latest local snapshot, schedules its
// background detach and (when not yet durable) its deferred upload, and
// returns the actor's state.
func (h *herder) commitLocal(actorUID string, actor *ateapi.ObjectRef, snap *ateletpb.Snapshot, now time.Time) *ateletpb.ActorSnapshotState {
	s := &h.snapshots
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.findOrAddLocked(actorUID)
	if actor.GetName() != "" {
		a.actor = proto.CloneOf(actor)
	}
	a.seq++
	a.stopDetachTimer()
	a.stopReport()
	a.state.LatestLocal = &ateletpb.SnapshotReport{
		Snapshot:        proto.CloneOf(snap),
		LocalCommitTime: timestamppb.New(now),
	}
	if !s.closed.Load() && h.reporter != nil && actorUID != "" && snap.GetObject().GetSnapshotUri() != "" {
		if !a.uploading() && a.uploadTimer == nil && a.state.GetLatestDurable().GetSnapshot().GetGeneration() < snap.GetGeneration() {
			a.uploadTimer = h.afterFunc(defaultUploadDelay, func() { h.backgroundUpload(actorUID) })
		}
		seq := a.seq
		a.detachTimer = h.afterFunc(defaultDetachDelay, func() { h.backgroundDetach(actorUID, seq) })
	}
	return proto.CloneOf(&a.state)
}

// commitDurable records snap as the actor's latest durable snapshot and
// returns the actor's state. It drops an older local snapshot, which snap
// supersedes, and stops a background upload. A pending background detach of
// the local snapshot it keeps stays scheduled, as in atelet.
func (s *actorSnapshots) commitDurable(actorUID string, actor *ateapi.ObjectRef, snap *ateletpb.Snapshot, now time.Time) *ateletpb.ActorSnapshotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.findOrAddLocked(actorUID)
	if actor.GetName() != "" {
		a.actor = proto.CloneOf(actor)
	}
	if a.uploading() {
		a.stopOp()
	}
	a.stopReport()
	localCommit := timestamppb.New(now)
	if local := a.state.GetLatestLocal(); local.GetSnapshot().GetSnapshotUuid() == snap.GetSnapshotUuid() && local.GetLocalCommitTime() != nil {
		localCommit = proto.CloneOf(local.GetLocalCommitTime())
	}
	a.state.LatestDurable = &ateletpb.SnapshotReport{
		Snapshot:            proto.CloneOf(snap),
		LocalCommitTime:     localCommit,
		DurableCompleteTime: timestamppb.New(now),
	}
	if a.state.GetLatestLocal().GetSnapshot().GetGeneration() < snap.GetGeneration() {
		a.seq++
		a.state.LatestLocal = nil
		a.stopDetachTimer()
	}
	if a.state.GetLatestLocal() == nil || a.state.GetLatestDurable().GetSnapshot().GetGeneration() >= a.state.GetLatestLocal().GetSnapshot().GetGeneration() {
		a.stopUploadTimer()
	}
	return proto.CloneOf(&a.state)
}

// markStarted records that Run or Restore started the actor on this node,
// unscheduling any background detach until the next Checkpoint.
func (s *actorSnapshots) markStarted(actorUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actors[actorUID]; ok {
		a.seq++
		a.stopDetachTimer()
	}
}

// detachLocal clears the actor's latest local snapshot, which DetachActor made
// durable, stops its other background work, and reports the detach in the
// background. Like atelet, it keeps LatestDurable until ate-api-server
// acknowledges the report, so a repeated DetachActor still returns it.
func (h *herder) detachLocal(actorUID string) {
	s := &h.snapshots
	s.mu.Lock()
	a, ok := s.actors[actorUID]
	if !ok {
		s.mu.Unlock()
		return
	}
	a.seq++
	a.stopAll()
	a.state.LatestLocal = nil
	if s.closed.Load() || h.reporter == nil {
		s.mu.Unlock()
		return
	}
	seq, actor, state := a.seq, proto.CloneOf(a.actor), proto.CloneOf(&a.state)
	reportCtx := h.beginReportLocked(a)
	s.mu.Unlock()
	s.goBackground(func() { h.reportDetach(reportCtx, actorUID, seq, actor, state) })
}

// get returns the actor's state, empty when nothing is recorded for it.
func (s *actorSnapshots) get(actorUID string) *ateletpb.ActorSnapshotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actors[actorUID]; ok {
		return proto.CloneOf(&a.state)
	}
	return &ateletpb.ActorSnapshotState{}
}

// forget drops what is recorded for the actor and stops its background work.
func (s *actorSnapshots) forget(actorUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actors[actorUID]; ok {
		a.stopAll()
		delete(s.actors, actorUID)
	}
}

// close stops all timers and in-flight background work and waits for running
// background goroutines to finish.
func (s *actorSnapshots) close() {
	s.mu.Lock()
	for _, a := range s.actors {
		a.stopAll()
	}
	s.actors = nil
	s.mu.Unlock()

	s.wgMu.Lock()
	s.closed.Store(true)
	s.wgMu.Unlock()
	s.wg.Wait()
}

func (s *actorSnapshots) findOrAddLocked(actorUID string) *actorSnapshot {
	if s.actors == nil {
		s.actors = map[string]*actorSnapshot{}
	}
	a, ok := s.actors[actorUID]
	if !ok {
		a = &actorSnapshot{}
		s.actors[actorUID] = a
	}
	return a
}

func (h *herder) afterFunc(d time.Duration, fn func()) clock.Timer {
	s := &h.snapshots
	return h.clock().AfterFunc(d, func() { s.goBackground(fn) })
}

// goBackground runs fn in a goroutine that close waits for, unless close has
// begun.
func (s *actorSnapshots) goBackground(fn func()) {
	s.wgMu.RLock()
	defer s.wgMu.RUnlock()
	if !s.closed.Load() {
		s.wg.Go(fn)
	}
}

// acquire takes a slot from *sem, or returns false if ctx ends first. The
// caller calls release when done.
func (s *actorSnapshots) acquire(ctx context.Context, sem *chan struct{}) (release func(), ok bool) {
	s.semOnce.Do(func() {
		s.uploads = make(chan struct{}, uploadWorkers)
		s.detaches = make(chan struct{}, detachWorkers)
		s.reports = make(chan struct{}, reportWorkers)
	})
	select {
	case *sem <- struct{}{}:
		return func() { <-*sem }, true
	case <-ctx.Done():
		return nil, false
	}
}

func (h *herder) beginOpLocked(a *actorSnapshot, detach bool) (context.Context, *backgroundOp) {
	a.stopOp()
	ctx, cancel := context.WithCancel(h.baseContext())
	a.op = &backgroundOp{detach: detach, cancel: cancel}
	return ctx, a.op
}

func (h *herder) beginReportLocked(a *actorSnapshot) context.Context {
	a.stopReport()
	ctx, cancel := context.WithCancel(h.baseContext())
	a.cancelReport = cancel
	return ctx
}

// backgroundUpload uploads the actor's latest local snapshot and reports the
// updated state to ate-api-server.
func (h *herder) backgroundUpload(actorUID string) {
	s := &h.snapshots
	release, ok := s.acquire(h.baseContext(), &s.uploads)
	if !ok {
		return
	}
	release = sync.OnceFunc(release)
	defer release()

	s.mu.Lock()
	a := s.actors[actorUID]
	if a == nil {
		s.mu.Unlock()
		return
	}
	a.uploadTimer = nil
	local := a.state.GetLatestLocal()
	snap := local.GetSnapshot()
	uri := snap.GetObject().GetSnapshotUri()
	// A detach in flight uploads the snapshot itself.
	if snap == nil || uri == "" || a.op != nil || a.state.GetLatestDurable().GetSnapshot().GetGeneration() >= snap.GetGeneration() {
		s.mu.Unlock()
		return
	}
	snap = proto.CloneOf(snap)
	localCommit := proto.CloneOf(local.GetLocalCommitTime())
	opCtx, op := h.beginOpLocked(a, false)
	s.mu.Unlock()

	err := h.writePlaceholder(opCtx, time.Now(), 0, uri)

	s.mu.Lock()
	a = s.actors[actorUID]
	if a == nil || opCtx.Err() != nil || !a.endOp(op) {
		s.mu.Unlock()
		return
	}
	if err != nil {
		slog.WarnContext(opCtx, "Background snapshot upload failed", slog.String("actor_uid", actorUID), slog.Any("err", err))
		if cur := a.state.GetLatestLocal().GetSnapshot(); cur != nil && cur.GetObject().GetSnapshotUri() != "" &&
			a.state.GetLatestDurable().GetSnapshot().GetGeneration() < cur.GetGeneration() && a.uploadTimer == nil && !s.closed.Load() {
			a.uploadTimer = h.afterFunc(defaultUploadDelay, func() { h.backgroundUpload(actorUID) })
		}
		s.mu.Unlock()
		return
	}
	if a.state.GetLatestDurable().GetSnapshot().GetGeneration() >= snap.GetGeneration() {
		s.mu.Unlock()
		return
	}
	now := h.now()
	a.state.LatestDurable = &ateletpb.SnapshotReport{
		Snapshot:            snap,
		LocalCommitTime:     localCommit,
		DurableCompleteTime: timestamppb.New(now),
	}
	if cur := a.state.GetLatestLocal().GetSnapshot(); cur != nil && cur.GetGeneration() > snap.GetGeneration() &&
		cur.GetObject().GetSnapshotUri() != "" && a.uploadTimer == nil && !s.closed.Load() {
		a.uploadTimer = h.afterFunc(defaultUploadDelay, func() { h.backgroundUpload(actorUID) })
	}
	actorRef := proto.CloneOf(a.actor)
	state := proto.CloneOf(&a.state)
	reportCtx := h.beginReportLocked(a)
	s.mu.Unlock()
	release()

	h.reportSnapshotState(reportCtx, actorUID, actorRef, state)
}

// backgroundDetach uploads the actor's latest local snapshot if not yet
// durable, clears LatestLocal, and reports the detach to ate-api-server. Once
// ate-api-server acknowledges the report, or refuses it with NotFound, it
// forgets the actor, as atelet does. A failed upload is retried after
// detachRetryDelay.
func (h *herder) backgroundDetach(actorUID string, seq uint64) {
	s := &h.snapshots
	release, ok := s.acquire(h.baseContext(), &s.detaches)
	if !ok {
		return
	}
	release = sync.OnceFunc(release)
	defer release()

	s.mu.Lock()
	a := s.actors[actorUID]
	if a == nil || a.seq != seq {
		s.mu.Unlock()
		return
	}
	a.detachTimer = nil
	a.stopUploadTimer()
	local := a.state.GetLatestLocal()
	snap := local.GetSnapshot()
	uri := snap.GetObject().GetSnapshotUri()
	if snap == nil || uri == "" {
		s.mu.Unlock()
		return
	}
	needUpload := a.state.GetLatestDurable().GetSnapshot().GetGeneration() < snap.GetGeneration()
	snap = proto.CloneOf(snap)
	localCommit := proto.CloneOf(local.GetLocalCommitTime())
	opCtx, op := h.beginOpLocked(a, true)
	s.mu.Unlock()

	var err error
	if needUpload {
		err = h.writePlaceholder(opCtx, time.Now(), 0, uri)
	}

	s.mu.Lock()
	a = s.actors[actorUID]
	if a == nil || opCtx.Err() != nil || !a.endOp(op) || a.seq != seq {
		s.mu.Unlock()
		return
	}
	if err != nil {
		slog.WarnContext(opCtx, "Background snapshot detach failed", slog.String("actor_uid", actorUID), slog.Any("err", err))
		if !s.closed.Load() {
			a.detachTimer = h.afterFunc(detachRetryDelay, func() { h.backgroundDetach(actorUID, seq) })
		}
		s.mu.Unlock()
		return
	}
	if needUpload && a.state.GetLatestDurable().GetSnapshot().GetGeneration() < snap.GetGeneration() {
		a.state.LatestDurable = &ateletpb.SnapshotReport{
			Snapshot:            snap,
			LocalCommitTime:     localCommit,
			DurableCompleteTime: timestamppb.New(h.now()),
		}
	}
	a.state.LatestLocal = nil
	actorRef := proto.CloneOf(a.actor)
	state := proto.CloneOf(&a.state)
	reportCtx := h.beginReportLocked(a)
	s.mu.Unlock()
	release()

	h.reportDetach(reportCtx, actorUID, seq, actorRef, state)
}

// reportDetach reports the actor's detach. Once ate-api-server acknowledges
// it, or refuses it with NotFound, it forgets the actor, as atelet does. It
// keeps the actor if a Run, Restore, or commit changed its seq meanwhile.
func (h *herder) reportDetach(ctx context.Context, actorUID string, seq uint64, actor *ateapi.ObjectRef, state *ateletpb.ActorSnapshotState) {
	acked := h.reportSnapshotState(ctx, actorUID, actor, state)
	s := &h.snapshots
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.actors[actorUID]; a != nil && a.seq == seq && ctx.Err() == nil {
		a.stopAll()
		if acked {
			delete(s.actors, actorUID)
		}
	}
}

// reportSnapshotState sends state through WorkerService.ReportActorSnapshotState,
// retrying retryable errors such as Aborted while a workflow holds the actor.
// It reports whether ate-api-server acknowledged the state or refused it with
// NotFound because the actor is gone or no longer on this node.
func (h *herder) reportSnapshotState(ctx context.Context, actorUID string, actor *ateapi.ObjectRef, state *ateletpb.ActorSnapshotState) bool {
	if h.reporter == nil || actor.GetName() == "" || actorUID == "" {
		return false
	}
	req := &ateapi.ReportActorSnapshotStateRequest{
		Actor:    actor,
		ActorUid: actorUID,
		State:    toAteAPIActorSnapshotState(state),
	}
	clk := h.clock()
	deadline := clk.Now().Add(reportGiveUpAfter)
	backoff := reportRetryDelay
	for {
		release, ok := h.snapshots.acquire(ctx, &h.snapshots.reports)
		if !ok {
			return false
		}
		callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
		_, err := h.reporter.ReportActorSnapshotState(callCtx, req)
		cancel()
		release()
		if err == nil || status.Code(err) == codes.NotFound {
			return true
		}
		if ctx.Err() != nil || !reportRetryable(err) || !clk.Now().Before(deadline) {
			if ctx.Err() == nil && status.Code(err) != codes.Unimplemented {
				slog.WarnContext(ctx, "Failed to report the actor's snapshot state",
					slog.String("actor_uid", actorUID), slog.Any("actor", actor), slog.Any("err", err))
			}
			return false
		}
		t := clk.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C():
		}
		backoff = min(backoff*2, maxReportRetryDelay)
	}
}

func reportRetryable(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument, codes.Unimplemented:
		return false
	default:
		return true
	}
}

func toAteAPIActorSnapshotState(in *ateletpb.ActorSnapshotState) *ateapi.ActorSnapshotState {
	if in == nil {
		return nil
	}
	return &ateapi.ActorSnapshotState{
		LatestLocal:   toAteAPISnapshotReport(in.GetLatestLocal()),
		Uploading:     toAteAPISnapshotReport(in.GetUploading()),
		LatestDurable: toAteAPISnapshotReport(in.GetLatestDurable()),
		UploadDueTime: in.GetUploadDueTime(),
	}
}

func toAteAPISnapshotReport(in *ateletpb.SnapshotReport) *ateapi.SnapshotReport {
	snap := in.GetSnapshot()
	if snap == nil {
		return nil
	}
	out := &ateapi.SnapshotReport{
		Uuid:                snap.GetSnapshotUuid(),
		Generation:          snap.GetGeneration(),
		Fidelity:            toAteAPIFidelity(snap.GetFidelity()),
		LocalCommitTime:     in.GetLocalCommitTime(),
		DurableCompleteTime: in.GetDurableCompleteTime(),
	}
	if obj := snap.GetObject(); obj != nil {
		out.Object = &ateapi.ObjectSnapshot{SnapshotUri: obj.GetSnapshotUri()}
	}
	return out
}

func toAteAPIFidelity(in ateletpb.SnapshotFidelity) ateapi.SnapshotFidelity {
	switch in {
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES:
		return ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS
	case ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY
	default:
		return ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED
	}
}

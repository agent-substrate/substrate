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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/clock"
)

// placeholderObject is the one object written under each snapshot URI.
const placeholderObject = "fake-dataplane-placeholder"

// objectWriter is the part of pkg/objectstorage.ObjectStorage the herder
// uses.
type objectWriter interface {
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

// actorCertMinter is the part of ateapi.WorkerServiceClient the herder uses to
// mint actor certificates.
type actorCertMinter interface {
	MintWorkerActorCertificate(ctx context.Context, in *ateapi.MintWorkerActorCertificateRequest, opts ...grpc.CallOption) (*ateapi.MintWorkerActorCertificateResponse, error)
}

// snapshotReporter is the part of ateapi.WorkerServiceClient the herder uses
// to report background snapshot uploads and detaches.
type snapshotReporter interface {
	ReportActorSnapshotState(ctx context.Context, in *ateapi.ReportActorSnapshotStateRequest, opts ...grpc.CallOption) (*ateapi.ReportActorSnapshotStateResponse, error)
}

// delays is how long each Atelet call takes before it succeeds: the
// data plane's share of it, not counting an actor certificate mint.
type delays struct {
	run, restore, checkpoint, promoteSnapshot, detachActor, terminate time.Duration
}

// herder answers every Atelet call with success after its delay, without
// running or saving any workload. Actors it "runs" do not exist.
//
// It writes a placeholder object under each snapshot URI it uploads to:
// ate-api-server copies an actor's snapshot when it creates a tag, golden tags
// included, and refuses to copy an empty one.
//
// It calls ate-api-server back to mint actor certificates on Run and Restore
// when an egress gateway is named, and to report background snapshot uploads
// and detaches.
type herder struct {
	ateletpb.UnsafeAteletServer

	ctx     context.Context
	clk     clock.WithDelayedExecution
	delays  delays
	storage objectWriter
	// minter mints actor certificates under this pod's atelet identity; nil
	// leaves the mint out. csr is sent with every mint.
	minter actorCertMinter
	csr    []byte
	// reporter reports background uploads and detaches under this pod's atelet
	// identity; nil leaves background work out.
	reporter snapshotReporter
	// epochs is where Run and Restore read the epoch they report; the relay
	// records it when the Worker registers. Workers are named by their pod UID.
	epochs *workerEpochs
	// snapshots is the state Checkpoint, PromoteSnapshot, and DetachActor
	// report, and schedules background uploads and detaches.
	snapshots actorSnapshots
}

func (h *herder) baseContext() context.Context {
	if h.ctx != nil {
		return h.ctx
	}
	return context.Background()
}

func (h *herder) clock() clock.WithDelayedExecution {
	if h.clk != nil {
		return h.clk
	}
	return clock.RealClock{}
}

func (h *herder) now() time.Time {
	return h.clock().Now()
}

// newActorCSR returns a CSR for a fresh key. ate-api-server takes only the
// public key from it, so one serves every mint; worker makes a key per
// activation, but that cost is the data plane's, not ate-api-server's.
func newActorCSR() ([]byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("while generating the actor key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, fmt.Errorf("while creating the actor CSR: %w", err)
	}
	return csr, nil
}

// mintActorCertificate mints the actor's certificate when the call names an
// egress gateway, as worker does before it starts the workload. Its time is
// ate-api-server's, so it adds to the call's delay rather than running inside
// it; a failure fails the call, as tunneled egress fails closed in worker.
func (h *herder) mintActorCertificate(ctx context.Context, workerPodUID, atespace, name, uid string, gateway *ateletpb.EgressGateway) error {
	if h.minter == nil || gateway == nil {
		return nil
	}
	if _, err := h.minter.MintWorkerActorCertificate(ctx, &ateapi.MintWorkerActorCertificateRequest{
		Actor:                     &ateapi.ObjectRef{Atespace: atespace, Name: name},
		ActorUid:                  uid,
		CertificateSigningRequest: h.csr,
		// Workers are global-scoped and named by their pod UID.
		Worker: &ateapi.ObjectRef{Name: workerPodUID},
	}); err != nil {
		return fmt.Errorf("while minting the certificate of actor %s/%s: %w", atespace, name, err)
	}
	return nil
}

// wait sleeps until start+d, or returns the context's error if the caller
// gives up first.
func wait(ctx context.Context, start time.Time, d time.Duration) error {
	remaining := d - time.Since(start)
	if remaining <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(remaining)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// writePlaceholder writes the placeholder object under snapshotURI. It runs
// inside the call's delay; a write slower than a nonzero delay is logged, since
// the call then takes longer than configured. With no delay, every write
// outlasts it, so there is nothing to report.
func (h *herder) writePlaceholder(ctx context.Context, start time.Time, d time.Duration, snapshotURI string) error {
	uri, err := resources.ParseSnapshotURI(snapshotURI)
	if err != nil {
		return fmt.Errorf("while parsing snapshot URI %q: %w", snapshotURI, err)
	}
	bucket, prefix, err := objectstore.BucketPrefix(uri.Prefix())
	if err != nil {
		return err
	}
	if err := h.storage.PutObject(ctx, bucket, prefix+placeholderObject, strings.NewReader("")); err != nil {
		return fmt.Errorf("while writing the placeholder under %s: %w", snapshotURI, err)
	}
	if elapsed := time.Since(start); d > 0 && elapsed > d {
		slog.WarnContext(ctx, "Placeholder write outlasted the configured delay",
			slog.String("snapshot_uri", snapshotURI), slog.Duration("elapsed", elapsed), slog.Duration("delay", d))
	}
	return nil
}

func (h *herder) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := h.mintActorCertificate(ctx, req.GetWorkerPodUid(), req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetEgressGateway()); err != nil {
		return nil, err
	}
	if err := wait(ctx, time.Now(), h.delays.run); err != nil {
		return nil, err
	}
	h.snapshots.markStarted(req.GetActorUid())
	return &ateletpb.RunResponse{WorkerEpoch: h.epochs.get(req.GetWorkerPodUid())}, nil
}

func (h *herder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := h.mintActorCertificate(ctx, req.GetWorkerPodUid(), req.GetAtespace(), req.GetActorName(), req.GetActorUid(), req.GetEgressGateway()); err != nil {
		return nil, err
	}
	if err := wait(ctx, time.Now(), h.delays.restore); err != nil {
		return nil, err
	}
	h.snapshots.markStarted(req.GetActorUid())
	return &ateletpb.RestoreResponse{WorkerEpoch: h.epochs.get(req.GetWorkerPodUid())}, nil
}

func (h *herder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	start := time.Now()
	// An immediate upload puts the snapshot in object storage before
	// Checkpoint returns; a deferred upload runs in the background after
	// defaultUploadDelay.
	if req.GetUploadConfig().GetMode() == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		if err := h.writePlaceholder(ctx, start, h.delays.checkpoint, req.GetSnapshot().GetObject().GetSnapshotUri()); err != nil {
			return nil, err
		}
	}
	if err := wait(ctx, start, h.delays.checkpoint); err != nil {
		return nil, err
	}
	// Like atelet, keep the snapshot on the node in either mode, after
	// recording an immediate upload as durable.
	actorUID, snap, now := req.GetActorUid(), req.GetSnapshot(), h.now()
	actor := &ateapi.ObjectRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	if req.GetUploadConfig().GetMode() == ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		h.snapshots.commitDurable(actorUID, actor, snap, now)
	}
	return &ateletpb.CheckpointResponse{State: h.commitLocal(actorUID, actor, snap, now)}, nil
}

func (h *herder) PromoteSnapshot(ctx context.Context, req *ateletpb.PromoteSnapshotRequest) (*ateletpb.PromoteSnapshotResponse, error) {
	start := time.Now()
	if err := h.writePlaceholder(ctx, start, h.delays.promoteSnapshot, req.GetSnapshot().GetObject().GetSnapshotUri()); err != nil {
		return nil, err
	}
	if err := wait(ctx, start, h.delays.promoteSnapshot); err != nil {
		return nil, err
	}
	// Like atelet, report the snapshot durable and still kept on the node.
	actorUID, snap, now := req.GetActorUid(), req.GetSnapshot(), h.now()
	actor := &ateapi.ObjectRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
	state := h.snapshots.commitDurable(actorUID, actor, snap, now)
	if state.GetLatestLocal() == nil {
		state = h.commitLocal(actorUID, actor, snap, now)
	}
	return &ateletpb.PromoteSnapshotResponse{State: state}, nil
}

// DetachActor makes the actor's latest local snapshot durable, as atelet does
// by uploading it, then forgets the actor and reports its latest snapshot
// durable with none kept locally. ate-api-server records the detach from this
// response.
func (h *herder) DetachActor(ctx context.Context, req *ateletpb.DetachActorRequest) (*ateletpb.DetachActorResponse, error) {
	start := time.Now()
	actorUID := req.GetActorUid()
	state := h.snapshots.get(actorUID)
	local := state.GetLatestLocal().GetSnapshot()
	if local == nil {
		return &ateletpb.DetachActorResponse{State: &ateletpb.ActorSnapshotState{}}, nil
	}
	if state.GetLatestDurable().GetSnapshot().GetGeneration() < local.GetGeneration() {
		uri := local.GetObject().GetSnapshotUri()
		if uri == "" {
			return nil, status.Errorf(codes.FailedPrecondition, "actor %s has no snapshot.object to upload its latest local snapshot to", actorUID)
		}
		if err := h.writePlaceholder(ctx, start, h.delays.detachActor, uri); err != nil {
			return nil, err
		}
		actor := &ateapi.ObjectRef{Atespace: req.GetAtespace(), Name: req.GetActorName()}
		state = h.snapshots.commitDurable(actorUID, actor, local, h.now())
	}
	// The detach is done once the snapshot is durable: a caller that gives up
	// during the delay finds nothing left to detach when it calls again.
	h.snapshots.forget(actorUID)
	if err := wait(ctx, start, h.delays.detachActor); err != nil {
		return nil, err
	}
	return &ateletpb.DetachActorResponse{State: &ateletpb.ActorSnapshotState{LatestDurable: state.GetLatestDurable()}}, nil
}

func (h *herder) Terminate(ctx context.Context, req *ateletpb.TerminateRequest) (*ateletpb.TerminateResponse, error) {
	if err := wait(ctx, time.Now(), h.delays.terminate); err != nil {
		return nil, err
	}
	// Like atelet, drop the actor's snapshot state with the actor.
	h.snapshots.forget(req.GetActorUid())
	return &ateletpb.TerminateResponse{}, nil
}

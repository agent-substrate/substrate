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
	"crypto/tls"
	"fmt"
	"log/slog"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atelet/internal/apivalidation"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type workerSupportServer struct {
	ateletpb.UnimplementedWorkerSupportServer
	workers ateapi.WorkerServiceClient
	control ateapi.ControlClient
}

func (b *workerSupportServer) MintActorCertificate(ctx context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// the control plane mints only for the Worker that the actor is assigned
	// to, so a worker gets certificates for the actor it hosts and no other.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Reject malformed requests here rather than forwarding them for the
	// control plane to reject after a round trip. After authentication, so an
	// unauthenticated caller learns nothing but Unauthenticated.
	if errs := apivalidation.ValidateMintActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}

	resp, err := b.workers.MintWorkerActorCertificate(ctx, &ateapi.MintWorkerActorCertificateRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker: &ateapi.ObjectRef{Name: workerIdentity.PodUID},
		Actor: &ateapi.ObjectRef{
			Atespace: req.GetActorAtespace(),
			Name:     req.GetActorName(),
		},
		ActorUid:                  req.GetActorUid(),
		CertificateSigningRequest: req.GetCertificateSigningRequest(),
	})
	if err != nil {
		return nil, fmt.Errorf("mint actor certificate: %w", err)
	}
	return &ateletpb.MintActorCertificateResponse{ActorCertificates: resp.GetActorCertificates()}, nil
}

func authenticatedWorkerIdentity(ctx context.Context) (*substratex509.PodIdentity, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing peer credentials")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing peer certificate")
	}
	identity, err := substratex509.PodIdentityFromCertificate(tlsInfo.State.PeerCertificates[0])
	if err != nil || identity == nil {
		return nil, status.Error(codes.PermissionDenied, "invalid worker identity")
	}
	return identity, nil
}

// verifyClientOnSameNode returns a TLS callback that accepts only worker Pods
// scheduled on the atelet's node incarnation.
func verifyClientOnSameNode(node *substratex509.PodIdentity) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("worker certificate is required")
		}
		identity, err := substratex509.PodIdentityFromCertificate(state.PeerCertificates[0])
		if err != nil {
			return fmt.Errorf("parse worker Pod identity: %w", err)
		}
		if identity == nil || identity.NodeName != node.NodeName || identity.NodeUID != node.NodeUID {
			return fmt.Errorf("worker is not on node %q (%s)", node.NodeName, node.NodeUID)
		}
		return nil
	}
}

// AcquireWorkerEpoch hands the calling worker process a new epoch from the
// control plane's WorkerService.AcquireWorkerEpoch.
//
// Each of the registration calls below returns the control plane's error
// unwrapped, so the caller can tell a refusal from a failure to retry: a Worker
// record the syncer has not created yet is the ordinary reason for a first
// attempt to fail.
func (s *workerSupportServer) AcquireWorkerEpoch(ctx context.Context, _ *ateletpb.AcquireWorkerEpochRequest) (*ateletpb.AcquireWorkerEpochResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can acquire an epoch for itself and no one else.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := s.workers.AcquireWorkerEpoch(ctx, &ateapi.AcquireWorkerEpochRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker: &ateapi.ObjectRef{Name: workerIdentity.PodUID},
	})
	if err != nil {
		return nil, err
	}
	return &ateletpb.AcquireWorkerEpochResponse{Epoch: resp.GetEpoch()}, nil
}

// RegisterWorker registers the calling worker process with the control plane:
// it activates the epoch the process acquired and records its pod IPs and
// capacity, in one WorkerService.RegisterWorker call.
func (s *workerSupportServer) RegisterWorker(ctx context.Context, req *ateletpb.RegisterWorkerRequest) (*ateletpb.RegisterWorkerResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can register itself and no one else.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Reject malformed requests here rather than forwarding them for the
	// control plane to reject after a round trip. After authentication, so an
	// unauthenticated caller learns nothing but Unauthenticated.
	if errs := apivalidation.ValidateRegisterWorkerRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	if _, err := s.workers.RegisterWorker(ctx, &ateapi.RegisterWorkerRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker:   &ateapi.ObjectRef{Name: workerIdentity.PodUID},
		Epoch:    req.GetEpoch(),
		Ips:      req.GetIps(),
		Capacity: toWorkerResources(req.GetCapacity()),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Registered worker",
		slog.String("pod_uid", workerIdentity.PodUID),
		slog.Int64("epoch", req.GetEpoch()),
		slog.Any("ips", req.GetIps()),
		slog.Any("capacity", req.GetCapacity()))
	return &ateletpb.RegisterWorkerResponse{}, nil
}

// SetWorkerSandboxRuntimes records the sandbox runtimes of the calling worker
// process through WorkerService.SetWorkerSandboxRuntimes.
func (s *workerSupportServer) SetWorkerSandboxRuntimes(ctx context.Context, req *ateletpb.SetWorkerSandboxRuntimesRequest) (*ateletpb.SetWorkerSandboxRuntimesResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can report its own runtimes and no one else's.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateSetWorkerSandboxRuntimesRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	if _, err := s.workers.SetWorkerSandboxRuntimes(ctx, &ateapi.SetWorkerSandboxRuntimesRequest{
		Worker:         &ateapi.ObjectRef{Name: workerIdentity.PodUID},
		Epoch:          req.GetEpoch(),
		SandboxClasses: toAteAPISandboxClasses(req.GetSandboxClasses()),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Recorded worker sandbox runtimes",
		slog.String("pod_uid", workerIdentity.PodUID),
		slog.Int64("epoch", req.GetEpoch()),
		slog.Any("sandbox_classes", req.GetSandboxClasses()))
	return &ateletpb.SetWorkerSandboxRuntimesResponse{}, nil
}

// toWorkerResources converts atelet's WorkerResources to the control plane's,
// which it mirrors field for field.
func toWorkerResources(in *ateletpb.WorkerResources) *ateapi.WorkerResources {
	if in == nil {
		return nil
	}
	return &ateapi.WorkerResources{
		CpuMillicores: in.GetCpuMillicores(),
		MemoryBytes:   in.GetMemoryBytes(),
		Actors:        in.GetActors(),
	}
}

// toAteAPISandboxClasses converts atelet's WorkerSandboxClass list to the
// control plane's, which it mirrors field for field.
func toAteAPISandboxClasses(in []*ateletpb.WorkerSandboxClass) []*ateapi.WorkerSandboxClass {
	if in == nil {
		return nil
	}
	out := make([]*ateapi.WorkerSandboxClass, 0, len(in))
	for _, c := range in {
		out = append(out, &ateapi.WorkerSandboxClass{
			SandboxClass:       c.GetSandboxClass(),
			DefaultRuntime:     toAteAPISandboxRuntime(c.GetDefaultRuntime()),
			RestorableRuntimes: toAteAPISandboxRuntimes(c.GetRestorableRuntimes()),
		})
	}
	return out
}

// toAteAPISandboxRuntime converts atelet's WorkerSandboxRuntime to the control
// plane's, which it mirrors field for field.
func toAteAPISandboxRuntime(in *ateletpb.WorkerSandboxRuntime) *ateapi.WorkerSandboxRuntime {
	if in == nil {
		return nil
	}
	return &ateapi.WorkerSandboxRuntime{Version: toAteAPICompat(in.GetVersion())}
}

func toAteAPISandboxRuntimes(in []*ateletpb.WorkerSandboxRuntime) []*ateapi.WorkerSandboxRuntime {
	if in == nil {
		return nil
	}
	out := make([]*ateapi.WorkerSandboxRuntime, 0, len(in))
	for _, rt := range in {
		out = append(out, toAteAPISandboxRuntime(rt))
	}
	return out
}

// toAteAPICompat converts atelet's VersionedSandboxCompat to the control
// plane's, which it mirrors field for field.
func toAteAPICompat(in *ateletpb.VersionedSandboxCompat) *ateapi.VersionedSandboxCompat {
	if in == nil {
		return nil
	}
	out := &ateapi.VersionedSandboxCompat{SchemaVersion: in.GetSchemaVersion()}
	for _, a := range in.GetAttributes() {
		out.Attributes = append(out.Attributes, &ateapi.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()})
	}
	return out
}

// RequestActorSuspend forwards a worker's request to suspend an actor it hosts
// to the control plane, which owns their lifecycle. The worker observes; the
// control plane decides.
//
// The control plane's error is returned unwrapped: a refusal is a real answer
// here -- the actor is no longer assigned to this worker, or the suspend lost a
// race to a resume, suspend, or delete -- and the worker needs to tell those
// from a transport failure it should retry.
func (s *workerSupportServer) RequestActorSuspend(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) (*ateletpb.RequestActorSuspendResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request: a
	// worker can speak for the actors it hosts and no others. Which those are
	// is the control plane's to know, so it is checked there against the
	// worker this names.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Reject malformed requests here rather than forwarding them for the
	// control plane to reject after a round trip. After authentication, so an
	// unauthenticated caller learns nothing but Unauthenticated.
	if errs := apivalidation.ValidateRequestActorSuspendRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	if _, err := s.workers.RequestActorSuspend(ctx, &ateapi.RequestActorSuspendRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker: &ateapi.ObjectRef{Name: workerIdentity.PodUID},
		Actor: &ateapi.ObjectRef{
			Atespace: req.GetActorAtespace(),
			Name:     req.GetActorName(),
		},
		ActorUid: req.GetActorUid(),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Forwarded an actor suspend request",
		slog.String("pod_uid", workerIdentity.PodUID),
		slog.String("actor_atespace", req.GetActorAtespace()),
		slog.String("actor_name", req.GetActorName()),
		slog.String("actor_uid", req.GetActorUid()))
	return &ateletpb.RequestActorSuspendResponse{}, nil
}

// GetActorEgressPolicy reads the egress policy of an actor.
func (s *workerSupportServer) GetActorEgressPolicy(
	ctx context.Context,
	req *ateletpb.GetActorEgressPolicyRequest,
) (*ateletpb.GetActorEgressPolicyResponse, error) {
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Validate request.
	if errs := apivalidation.ValidateGetActorEgressPolicyRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}

	// TODO(bowei): add validation for which Actor requested this.

	policy, err := s.control.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{
		Actor: &ateapi.ObjectRef{
			Atespace: req.GetActorAtespace(),
			Name:     req.GetActorName(),
		},
		Name: resources.SingletonName,
	})

	// Note: we do not check that the EgressPolicy is directly attached to the
	// Actor: Policies are applied by (user-given) name as opposed to linked
	// explicitly to the internal Actor UUID.

	// NotFound is treated as empty policy (block all).
	if status.Code(err) == codes.NotFound {
		return &ateletpb.GetActorEgressPolicyResponse{}, nil
	}

	if err != nil {
		return nil, err
	}

	slog.DebugContext(ctx, "Fetched an actor egress policy",
		slog.String("pod_uid", workerIdentity.PodUID),
		slog.String("actor_atespace", req.GetActorAtespace()),
		slog.String("actor_name", req.GetActorName()))

	return &ateletpb.GetActorEgressPolicyResponse{EgressPolicy: toAteletEgressPolicy(policy)}, nil
}

// ReportActorCrashed forwards a worker's report that the sandbox of an actor
// it hosts died on its own. Authorized like RequestActorSuspend.
func (s *workerSupportServer) ReportActorCrashed(ctx context.Context, req *ateletpb.ReportActorCrashedRequest) (*ateletpb.ReportActorCrashedResponse, error) {
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	if errs := apivalidation.ValidateReportActorCrashedRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}
	if _, err := s.workers.ReportActorCrashed(ctx, &ateapi.ReportActorCrashedRequest{
		Worker: &ateapi.ObjectRef{Name: workerIdentity.PodUID},
		Actor: &ateapi.ObjectRef{
			Atespace: req.GetActorAtespace(),
			Name:     req.GetActorName(),
		},
		ActorUid: req.GetActorUid(),
		Message:  req.GetMessage(),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Forwarded an actor crash report",
		slog.String("pod_uid", workerIdentity.PodUID),
		slog.String("actor_atespace", req.GetActorAtespace()),
		slog.String("actor_name", req.GetActorName()),
		slog.String("actor_uid", req.GetActorUid()))
	return &ateletpb.ReportActorCrashedResponse{}, nil
}

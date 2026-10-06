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

	"github.com/agent-substrate/substrate/cmd/atelet/internal/apivalidation"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ateomSupportServer struct {
	ateletpb.UnimplementedAteomSupportServer
	workers ateapipb.WorkerServiceClient
}

func (b *ateomSupportServer) MintActorCertificate(ctx context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	// Check which ateom is calling.
	_, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Reject malformed requests here rather than forwarding them for the
	// control plane to reject after a round trip. After authentication, so an
	// unauthenticated caller learns nothing but Unauthenticated.
	if errs := apivalidation.ValidateMintActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToGRPCStatusError(errs)
	}

	// TODO(identity): Check that we believe that this ateom is running the
	// requested actor?  ate-api-server will further check that we (the atelet)
	// are allowed to request a certificate for the actor.

	resp, err := b.workers.MintAteomActorCertificate(ctx, &ateapipb.MintAteomActorCertificateRequest{
		Actor: &ateapipb.ObjectRef{
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

// RegisterWorker registers the calling worker with the control plane: what the
// worker says its capacity and hardware identity are, in one
// WorkerService.RegisterWorker call so capacity and hardware land atomically.
//
// It returns the control plane's error unwrapped so the caller retries: a
// worker reports once, so an accepted call is the only thing that puts
// capacity and hardware on the Worker, and a Worker record the syncer has not
// created yet is the ordinary reason for a first attempt to fail.
func (s *ateomSupportServer) RegisterWorker(ctx context.Context, req *ateletpb.RegisterWorkerRequest) (*ateletpb.RegisterWorkerResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can report its own capacity and hardware and no one else's.
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
	if _, err := s.workers.RegisterWorker(ctx, &ateapipb.RegisterWorkerRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker:   &ateapipb.ObjectRef{Name: workerIdentity.PodUID},
		Capacity: toWorkerResources(req.GetCapacity()),
		Hardware: toHardwareIdentity(req.GetHardware()),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Registered worker capacity and hardware",
		slog.String("pod_uid", workerIdentity.PodUID), slog.Any("capacity", req.GetCapacity()), slog.Any("hardware", req.GetHardware()))
	return &ateletpb.RegisterWorkerResponse{}, nil
}

// toWorkerResources converts atelet's WorkerResources to the control plane's,
// which it mirrors field for field.
func toWorkerResources(in *ateletpb.WorkerResources) *ateapipb.WorkerResources {
	if in == nil {
		return nil
	}
	out := &ateapipb.WorkerResources{Actors: in.GetActors()}
	if r := in.GetResources(); r != nil {
		out.Resources = &ateapipb.Resources{}
		for _, l := range r.GetLimits() {
			out.Resources.Limits = append(out.Resources.Limits, &ateapipb.Limits{Name: l.GetName(), Quantity: l.GetQuantity()})
		}
	}
	return out
}

// toHardwareIdentity converts atelet's HardwareIdentity to the control plane's,
// which it mirrors field for field.
func toHardwareIdentity(in *ateletpb.HardwareIdentity) *ateapipb.HardwareIdentity {
	if in == nil {
		return nil
	}
	out := &ateapipb.HardwareIdentity{}
	if attrs := in.GetAttributes(); attrs != nil {
		out.Attributes = make(map[string]string, len(attrs))
		for k, v := range attrs {
			out.Attributes[k] = v
		}
	}
	return out
}

// RequestActorSuspend forwards a worker's request to suspend an actor it hosts
// to the control plane, which owns their lifecycle. The worker observes; the
// control plane decides.
//
// The control plane's error is returned unwrapped: a refusal is a real answer
// here -- the actor is no longer assigned to this worker, or the suspend lost a
// race to a resume, pause, or delete -- and the worker needs to tell those from
// a transport failure it should retry.
func (s *ateomSupportServer) RequestActorSuspend(ctx context.Context, req *ateletpb.RequestActorSuspendRequest) (*ateletpb.RequestActorSuspendResponse, error) {
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
	if _, err := s.workers.RequestActorSuspend(ctx, &ateapipb.RequestActorSuspendRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker: &ateapipb.ObjectRef{Name: workerIdentity.PodUID},
		Actor: &ateapipb.ObjectRef{
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

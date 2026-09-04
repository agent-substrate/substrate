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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ateomSupportServer struct {
	ateletpb.UnimplementedAteomSupportServer
	controlClient ateapipb.ControlClient
	workers       ateapipb.WorkerServiceClient
}

func (b *ateomSupportServer) MintActorCertificate(ctx context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	// Check which ateom is calling.
	_, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}

	// TODO(identity): Check that we believe that this ateom is running the
	// requested actor?  ate-api-server will further check that we (the atelet)
	// are allowed to request a certificate for the actor.

	resp, err := b.controlClient.MintActorCertificate(ctx, &ateapipb.MintActorCertificateRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: req.GetActorAtespace(),
			Name:     req.GetActorName(),
		},
		ActorUid:                  req.GetActorUid(),
		CertificateSigningRequest: req.GetCertificateSigningRequest(),
		Purpose:                   ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL,
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

// SetWorkerCapacity records what the calling worker says it has.
//
// It returns the control plane's error unwrapped so the caller retries: a
// worker reports once, so an accepted call is the only thing that puts
// capacity on the Worker, and a Worker record the syncer has not created yet
// is the ordinary reason for a first attempt to fail.
func (s *ateomSupportServer) SetWorkerCapacity(ctx context.Context, req *ateletpb.SetWorkerCapacityRequest) (*ateletpb.SetWorkerCapacityResponse, error) {
	// Identity comes only from the mTLS certificate, never from the request:
	// a worker can report its own capacity and no one else's.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	// Forwarded as reported: the worker speaks the vocabulary the control plane
	// records, so there is nothing to translate.
	if _, err := s.workers.SetWorkerCapacity(ctx, &ateapipb.SetWorkerCapacityRequest{
		// Workers are global-scoped and named by their pod UID.
		Worker:   &ateapipb.ObjectRef{Name: workerIdentity.PodUID},
		Capacity: req.GetCapacity(),
	}); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx, "Recorded worker capacity",
		slog.String("pod_uid", workerIdentity.PodUID), slog.Any("capacity", req.GetCapacity()))
	return &ateletpb.SetWorkerCapacityResponse{}, nil
}

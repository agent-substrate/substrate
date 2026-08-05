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

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type credentialBroker struct {
	ateletpb.UnimplementedCredentialBrokerServer
	// control resolves the authenticated worker Pod to its current assignment.
	control ateapipb.ControlClient
	// actorIdentityClient revalidates that assignment and signs the actor certificate.
	actorIdentityClient ateapipb.ActorIdentityClient
}

func (b *credentialBroker) MintActorCertificate(ctx context.Context, req *ateletpb.MintActorCertificateRequest) (*ateletpb.MintActorCertificateResponse, error) {
	// TODO: Before release, require the egress PEP to reject actor certificates
	// whose ActorIdentity purpose is not atunnel.
	// The request deliberately carries no actor identity. The authenticated Pod
	// identity is the only input used to select a worker and its current assignment.
	workerIdentity, err := authenticatedWorkerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	assigned, err := b.control.GetWorker(ctx, &ateapipb.GetWorkerRequest{
		WorkerNamespace: workerIdentity.Namespace,
		WorkerPod:       workerIdentity.PodName,
	})
	if err != nil {
		return nil, fmt.Errorf("get worker: %w", err)
	}
	if assigned.GetWorkerPodUid() != workerIdentity.PodUID {
		return nil, status.Error(codes.PermissionDenied, "worker Pod UID does not match")
	}
	actorRef := assigned.GetAssignment().GetActor()
	if actorRef.GetAtespace() == "" || actorRef.GetName() == "" {
		return nil, status.Error(codes.PermissionDenied, "worker has no actor assignment")
	}
	actor, err := b.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: actorRef})
	if err != nil {
		return nil, fmt.Errorf("get assigned actor: %w", err)
	}
	// MintCert revalidates this assignment in ateapi. That second check closes
	// the race where the worker is reassigned after GetWorker returns.
	resp, err := b.actorIdentityClient.MintCert(ctx, &ateapipb.MintCertRequest{
		Atespace:                  actor.GetMetadata().GetAtespace(),
		ActorName:                 actor.GetMetadata().GetName(),
		ActorUid:                  actor.GetMetadata().GetUid(),
		CertificateSigningRequest: req.GetCertificateSigningRequest(),
		WorkerPodUid:              workerIdentity.PodUID,
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

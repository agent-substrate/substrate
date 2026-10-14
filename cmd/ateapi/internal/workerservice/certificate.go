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

package workerservice

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// MintWorkerActorCertificate mints a Substrate-issued SPIFFE certificate that asserts
// a worker acting on behalf of a particular actor.
func (s *Server) MintWorkerActorCertificate(ctx context.Context, req *ateapipb.MintWorkerActorCertificateRequest) (*ateapipb.MintWorkerActorCertificateResponse, error) {
	if errs := apivalidation.ValidateMintWorkerActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	// TODO(identity): This check should be handled by OpenFGA.
	caller, err := ateletauth.Authenticate(ctx, s.ateletSPIFFEID)
	if err != nil {
		return nil, err
	}

	// TODO(authz): The egress gateway may also need certificates for actors
	// (via a cluster-level grant?).

	dbActor, err := s.store.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}
	// Only the worker assigned the actor may mint, through its node's atelet.
	// atunnel runs in that worker: it mints while the actor resumes and
	// renews until the worker is released. Unlike MintWorkerActorJwt, a paused
	// actor's node gets nothing, since no worker runs the actor. The node and
	// worker checks come first so a caller that does not run the actor gets
	// NotFound whatever UID it sends.
	if err := s.checkActorOnNode(ctx, dbActor, caller.NodeName); err != nil {
		return nil, err
	}
	// The node check alone would let any worker on the node mint for its
	// neighbors. HostingNode has checked that the store binds the worker the
	// status names to the actor; a paused actor's status names none.
	if assigned := dbActor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); assigned != req.GetWorker().GetName() {
		slog.WarnContext(ctx, "Refusing a worker-for-actor certificate for an actor another worker runs",
			slog.String("actor", resources.ActorRefFromActor(dbActor).String()),
			slog.String("assigned_worker", assigned),
			slog.String("caller_worker", req.GetWorker().GetName()),
			slog.String("caller_node", caller.NodeName))
		return nil, apierror.NotFound("actor not found")
	}
	if dbActor.GetMetadata().GetUid() != req.GetActorUid() {
		return nil, apierror.Aborted("conflict; actor has been deleted and recreated")
	}

	// Parse the CSR.
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		return nil, apierror.InvalidArgument("Failed to parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, apierror.InvalidArgument("Failed to verify CSR signature: %v", err)
	}

	template := &x509.Certificate{
		URIs: []*url.URL{
			{
				Scheme: "spiffe",
				// TODO(identity): Must be configurable per-install, so that each install can set it to a unique value.
				Host: "substrate-actor.local",
				Path: path.Join("worker-for-actor", dbActor.GetMetadata().GetAtespace(), dbActor.GetMetadata().GetName()),
			},
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	err = substratex509.AddActorIdentityToCertificate(
		&substratex509.ActorIdentity{
			Atespace:  dbActor.GetMetadata().GetAtespace(),
			ActorName: dbActor.GetMetadata().GetName(),
			ActorUid:  dbActor.GetMetadata().GetUid(),
		},
		template,
	)
	if err != nil {
		return nil, fmt.Errorf("while adding Substrate extension: %w", err)
	}

	// Sign and return the actor cert.
	chain, err := s.actorIDCAPool.CreateCertificate(template, csr.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("while signing certificate: %w", err)
	}

	return &ateapipb.MintWorkerActorCertificateResponse{
		ActorCertificates: chain,
	}, nil
}

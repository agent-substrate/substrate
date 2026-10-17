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

	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/ateletauth"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/substratex509"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
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
	// Only the assigned worker may mint, via its node's atelet, checked before the
	// UID so other callers get NotFound. Unlike MintWorkerActorJwt, a paused actor
	// gets NotFound too: it has no worker to bind the certificate to.
	if err := s.checkActorOnNode(ctx, dbActor, caller.NodeName, "certificate"); err != nil {
		return nil, err
	}
	// The status must name the requesting worker. When it names one,
	// checkActorOnNode has confirmed in the store's worker assignments that
	// this worker holds the actor. When it names none (a paused actor, or a
	// resume that has bound a worker but not yet recorded it), checkActorOnNode
	// read assigned_node alone, and no worker runs the actor yet.
	if assigned := dbActor.GetStatus().GetWorkerAssignment().GetWorker().GetName(); assigned == "" || assigned != req.GetWorker().GetName() {
		slog.WarnContext(ctx, "Refusing a worker-for-actor certificate for an actor not assigned to the calling worker",
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

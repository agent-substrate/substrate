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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MintAteomActorCertificate mints a Substrate-issued SPIFFE certificate that asserts
// an ateom acting on behalf of a particular actor.
func (s *Server) MintAteomActorCertificate(ctx context.Context, req *ateapipb.MintAteomActorCertificateRequest) (*ateapipb.MintAteomActorCertificateResponse, error) {
	if errs := validateMintAteomActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}

	// TODO(identity): This check should be handled by OpenFGA.
	if _, err := ateletauth.Authenticate(ctx); err != nil {
		return nil, err
	}

	// TODO(authz): Authorization layer needs to check whether the caller has
	// the mintActorCertificate permission/relation with this actor.    This
	// could be an atelet (via the relationship of the atelet running the
	// actor), or the egress gateway (via a cluster-level grant?)

	// Verify that this actor exists in the store. It doesn't need to be
	// running, since we may need to issue certificates during actor boot / resume.
	dbActor, err := s.store.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}
	if dbActor.GetMetadata().GetUid() != req.GetActorUid() {
		return nil, status.Error(codes.Aborted, "conflict; actor has been deleted and recreated")
	}

	// Parse the CSR
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		return nil, fmt.Errorf("while parsing CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		slog.ErrorContext(ctx, "Failed to verify CSR signature", slog.Any("err", err))
		return nil, status.Errorf(codes.InvalidArgument, "Failed to verify CSR signature")
	}

	template := &x509.Certificate{
		URIs: []*url.URL{
			{
				Scheme: "spiffe",
				// TODO(identity): Must be configurable per-install, so that each install can set it to a unique value.
				Host: "substrate-actor.local",
				Path: path.Join("ateom", "actor", dbActor.GetMetadata().GetAtespace(), dbActor.GetMetadata().GetName()),
			},
		},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// Sign and return the actor cert.
	chain, err := s.actorIDCAPool.CreateCertificate(template, csr.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("while signing certificate: %w", err)
	}

	return &ateapipb.MintAteomActorCertificateResponse{
		ActorCertificates: chain,
	}, nil
}

func validateMintAteomActorCertificateRequest(ctx context.Context, req *ateapipb.MintAteomActorCertificateRequest) field.ErrorList {
	op := operation.Operation{Type: operation.Create}
	return controlapi.Validate_MintAteomActorCertificateRequest(ctx, op, nil, req, nil)
}

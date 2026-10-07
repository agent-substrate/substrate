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

package controlapi

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/admission"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/actoridjwt"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *RPCService) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (created *ateapipb.Actor, err error) {
	// First scrub any fields that users are not allowed to set, then fill the
	// defaults so validation sees the final resource state.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForCreate(inActor.Metadata)
		inActor.Status = nil
		defaults.Apply(inActor)
	}

	// Validate the request, including the object within it.
	if errs := apivalidation.ValidateCreateActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	start := time.Now()
	// Recorded only after validation, so every operation uniformly measures a
	// validated request; malformed ones stay visible in rpc.server.call.duration.
	defer func() {
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationCreate, start, err,
			ateattr.TemplateNameKey.String(inActor.GetActorTemplate().GetName()),
			ateattr.TemplateAtespaceKey.String(inActor.GetActorTemplate().GetAtespace()),
		)
	}()

	setSpanActorRefAttributes(ctx, resources.ActorRefFromActor(inActor))

	// Handle the creation, including validation of the final stored object.
	stored, err := s.admission.CreateActor(ctx, inActor)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		switch {
		case errors.Is(err, store.ErrAlreadyExists):
			return nil, apierror.AlreadyExists("Actor %s already exists", inActor.GetMetadata().GetName())
		case errors.Is(err, store.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("Atespace %s not found", inActor.GetMetadata().GetAtespace())
		case errors.Is(err, admission.ErrInvalid):
			return nil, apierror.InvalidArgument("%w", err)
		case errors.Is(err, admission.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("%w", err)
		default:
			return nil, fmt.Errorf("while recording actor: %w", err)
		}
	}

	// Without this an actor that is created and never resumed has no record at
	// all, at any retention.
	logActorStateChanged(ctx, stored, ateattr.OperationCreate)
	setSpanActorAttributes(ctx, stored)

	return stored, nil
}

func (s *RPCService) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if errs := apivalidation.ValidateGetActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	actor, err := s.admission.GetActor(ctx, actorRef)
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("Actor %s not found", actorRef)
	} else if err != nil {
		return nil, fmt.Errorf("while getting actor from DB: %w", err)
	}
	return actor, nil
}

func (s *RPCService) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	if errs := apivalidation.ValidateListActorsRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	page, err := s.admission.ListActors(ctx, req.GetAtespace(), store.ListOptions{PageSize: effectivePageSize(req.GetPageSize()), PageToken: req.GetPageToken()})
	if err != nil {
		return nil, mapListError(fmt.Errorf("while listing actors in db: %w", err))
	}
	return &ateapipb.ListActorsResponse{
		Actors:        page.Items,
		NextPageToken: page.NextPageToken,
	}, nil
}

func (s *RPCService) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	// First scrub any fields that users are not allowed to set.
	inActor := req.Actor
	if inActor != nil { // otherwise validation will flag it
		scrubResourceMetadataForUpdate(inActor.Metadata)
		inActor.Status = nil
	}

	// Validate the request.
	if errs := apivalidation.ValidateUpdateActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}

	actorRef := resources.ActorRefFromActor(inActor)
	setSpanActorRefAttributes(ctx, actorRef)

	storedActor, err := s.admission.UpdateActorSpec(ctx, inActor)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		switch {
		case errors.Is(err, store.ErrVersionConflict), errors.Is(err, store.ErrUIDConflict):
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		case errors.Is(err, store.ErrNotFound):
			return nil, apierror.NotFound("actor %s not found", actorRef)
		case errors.Is(err, store.ErrPreconditionRequired):
			return nil, apierror.InvalidArgument("while updating actor %s: %v", actorRef, err)
		case errors.Is(err, admission.ErrInvalid):
			return nil, apierror.InvalidArgument("%w", err)
		case errors.Is(err, admission.ErrFailedPrecondition):
			return nil, apierror.FailedPrecondition("%w", err)
		default:
			return nil, fmt.Errorf("while updating actor: %w", err)
		}
	}

	setSpanActorAttributes(ctx, storedActor)

	return storedActor, nil
}

func (s *RPCService) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (deleted *ateapipb.Actor, err error) {
	if errs := apivalidation.ValidateDeleteActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	start := time.Now()
	// Template dims only once the record resolved: the request names only the
	// actor, so failures before the load carry none. No pool pair: delete only
	// runs from SUSPENDED or CRASHED, which already released the worker.
	defer func() {
		var attrs []attribute.KeyValue
		if deleted != nil {
			attrs = append(attrs,
				ateattr.TemplateNameKey.String(deleted.GetActorTemplate().GetName()),
				ateattr.TemplateAtespaceKey.String(deleted.GetActorTemplate().GetAtespace()),
			)
		}
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationDelete, start, err, attrs...)
	}()
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	deleted, err = s.actorWorkflow.DeleteActor(ctx, actorRef, req.GetAnyState(), toDeletePreconditions(req.GetOptions()))
	if err != nil {
		return nil, err
	}

	return deleted, nil
}

func (s *RPCService) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	if errs := apivalidation.ValidatePauseActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.PauseActor(ctx, actorRef)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.PauseActorResponse{Actor: actor}, nil
}

func (s *RPCService) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if errs := apivalidation.ValidateResumeActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, resumed, err := s.actorWorkflow.ResumeActor(ctx, actorRef)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}

	setSpanActorAttributes(ctx, actor)
	return &ateapipb.ResumeActorResponse{Actor: actor, Resumed: resumed}, nil
}

func (s *RPCService) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if errs := apivalidation.ValidateSuspendActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.SuspendActor(ctx, actorRef)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return &ateapipb.SuspendActorResponse{Actor: actor}, nil
}

func (s *RPCService) RevertActor(ctx context.Context, req *ateapipb.RevertActorRequest) (*ateapipb.RevertActorResponse, error) {
	if errs := apivalidation.ValidateRevertActorRequest(ctx, req); len(errs) > 0 {
		return nil, resources.ToAPIError(errs)
	}
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.actorWorkflow.RevertActor(ctx, actorRef)
	if err != nil {
		// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Actor %s not found", actorRef)
		}
		return nil, err
	}
	setSpanActorAttributes(ctx, actor)
	return &ateapipb.RevertActorResponse{Actor: actor}, nil
}

func (s *RPCService) MintActorJWT(ctx context.Context, req *ateapipb.MintActorJWTRequest) (*ateapipb.MintActorJWTResponse, error) {
	if errs := apivalidation.ValidateMintActorJWTRequest(ctx, req); len(errs) > 0 {
		return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
	}

	// TODO(authz): Authorization layer needs to check whether the caller has
	// the mintActorJWT permission/relation with this actor.  This could be an
	// atelet (via the relationship of the atelet running the actor), or the
	// egress gateway (via a cluster-level grant?)

	// Verify that this actor exists in the store.  It doesn't need to be
	// running, since we may need to issue JWTs during actor boot / resume.
	dbActor, err := s.admission.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}

	// We only issue tokens with audience bindings.
	if len(req.GetAudience()) == 0 {
		return nil, fmt.Errorf("at least one audience must be requested")
	}

	// JWT timestamps have one-second resolution; truncating keeps expires_at
	// equal to the exp claim.
	now := time.Now().Truncate(time.Second)
	expiresAt := now.Add(time.Duration(req.GetExpirationSeconds()) * time.Second)
	actorClaims := &actoridjwt.Claims{
		Issuer:     s.actorJWTIssuer,
		Subject:    fmt.Sprintf("actor/%s/%s", dbActor.GetMetadata().GetAtespace(), dbActor.GetMetadata().GetName()),
		Audiences:  req.GetAudience(),
		Expiration: expiresAt,
		NotBefore:  now.Add(-5 * time.Minute),
		IssuedAt:   now,
		JTI:        rand.Text(),

		Substrate: actoridjwt.SubstrateClaims{
			Atespace:  dbActor.GetMetadata().GetAtespace(),
			ActorName: dbActor.GetMetadata().GetName(),
			ActorUID:  dbActor.GetMetadata().GetUid(),
		},
	}

	actorJWT, err := s.actorIDJWTPool.SignJWT(actorClaims)
	if err != nil {
		return nil, fmt.Errorf("while signing actor JWT: %w", err)
	}

	return &ateapipb.MintActorJWTResponse{
		ActorJwt:  actorJWT,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (s *RPCService) MintActorCertificate(ctx context.Context, req *ateapipb.MintActorCertificateRequest) (*ateapipb.MintActorCertificateResponse, error) {
	if errs := apivalidation.ValidateMintActorCertificateRequest(ctx, req); len(errs) > 0 {
		return nil, apierror.InvalidArgument("%v", errs.ToAggregate())
	}

	// TODO(authz): Authorization layer needs to check whether the caller has
	// the mintActorCertificate permission/relation with this actor.    This
	// could be an atelet (via the relationship of the atelet running the
	// actor), or the egress gateway (via a cluster-level grant?)

	// Check that the caller authenticated with a client certificate --- we
	// should not allow bootstrapping a proof-of-possession credential from a
	// bearer credential.  Note, we don't care that it was a certificate issued
	// by Substrate, or something else.
	//
	// TODO(authz): Perhaps this can be handled with an OpenFGA condition.
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, apierror.Unauthenticated("no peer transport information found")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, apierror.Unauthenticated("unexpected peer transport credentials")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, apierror.Unauthenticated("could not verify peer certificate")
	}

	// Verify that this actor exists in the store.  It doesn't need to be
	// running, since we may need to issue certificates during actor boot / resume.
	dbActor, err := s.admission.GetActor(ctx, resources.ActorRefFromObjectRef(req.GetActor()))
	// TODO: Centralize admission/store error-to-apierror mapping once store errors carry descriptive messages.
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.NotFound("actor not found")
	} else if err != nil {
		return nil, fmt.Errorf("while retrieving actor: %w", err)
	}
	if dbActor.GetMetadata().GetUid() != req.GetActorUid() {
		return nil, apierror.Aborted("conflict; actor has been deleted and recreated")
	}

	// Parse the CSR
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		return nil, fmt.Errorf("while parsing CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		slog.ErrorContext(ctx, "Failed to verify CSR signature", slog.Any("err", err))
		return nil, apierror.InvalidArgument("Failed to verify CSR signature")
	}

	template := &x509.Certificate{
		URIs: []*url.URL{resources.ActorSPIFFEID(resources.ActorRef{
			Atespace: dbActor.GetMetadata().GetAtespace(),
			Name:     dbActor.GetMetadata().GetName(),
		})},
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

	return &ateapipb.MintActorCertificateResponse{
		ActorCertificates: chain,
	}, nil
}

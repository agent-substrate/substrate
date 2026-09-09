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

package actoridentity

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/ateletauth"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/controlapi"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/actoridjwt"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Server implements ateapipb.ActorIdentityServer
type Server struct {
	ateapipb.UnimplementedActorIdentityServer

	// TODO(identity): Issuer is probably logically a property of the JWT
	// signing pool.
	actorIdentityJWTIssuer string

	actorIDJWTPool localjwtauthority.Pool
	actorIDCAPool  localca.Pool

	// store is the actor database. MintCert consults it to confirm the caller
	// is entitled to the actor it is asking for a credential for.
	store   store.Interface
	workers *workercache.Cache
}

var _ ateapipb.ActorIdentityServer = (*Server)(nil)

func New(actorIdentityJWTIssuer string, actorIDJWTPool localjwtauthority.Pool, actorIDCAPool localca.Pool, store store.Interface, workers *workercache.Cache) *Server {
	return &Server{
		actorIdentityJWTIssuer: actorIdentityJWTIssuer,
		actorIDJWTPool:         actorIDJWTPool,
		actorIDCAPool:          actorIDCAPool,
		store:                  store,
		workers:                workers,
	}
}

const actorCertificateLifetime = time.Hour

func (s *Server) MintJWT(ctx context.Context, req *ateapipb.MintJWTRequest) (*ateapipb.MintJWTResponse, error) {
	caller, ok := principal.FromContext(ctx)
	if !ok || caller.Kind != principal.KindJWT {
		return nil, status.Errorf(codes.Unauthenticated, "JWT authentication is required")
	}
	if caller.Issuer != s.actorIdentityJWTIssuer {
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor JWTs")
	}

	if errs := validateMintJWTRequest(ctx, req); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}

	// TODO: Cross-check the verified caller and requested actor against the actor database.

	// We only issue tokens with audience bindings.
	if len(req.GetAudience()) == 0 {
		return nil, fmt.Errorf("at least one audience must be requested")
	}

	actorClaims := &actoridjwt.Claims{
		// TODO: This is currently API but it has to be a globally unique, oidc-compliant and accsible DNS name
		Issuer: "https://api.ate-system.svc",
		// TODO: this format is very likely going to change.
		Subject:    fmt.Sprintf("atespaces:%s:actors:%s", req.GetAtespace(), req.GetActorName()),
		Audiences:  req.GetAudience(),
		Expiration: time.Now().Add(15 * time.Minute),
		NotBefore:  time.Now().Add(-5 * time.Minute),
		IssuedAt:   time.Now(),
		JTI:        rand.Text(),

		Substrate: actoridjwt.SubstrateClaims{
			Atespace:  req.GetAtespace(),
			ActorName: req.GetActorName(),
			ActorUID:  req.GetActorUid(),
		},
	}

	actorJWT, err := s.actorIDJWTPool.SignJWT(actorClaims)
	if err != nil {
		return nil, fmt.Errorf("while signing actor JWT: %w", err)
	}

	return &ateapipb.MintJWTResponse{
		ActorJwt: actorJWT,
	}, nil
}

func (s *Server) MintCert(ctx context.Context, req *ateapipb.MintCertRequest) (*ateapipb.MintCertResponse, error) {
	caller, err := ateletauth.Authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if errs := validateMintCertRequest(ctx, req); len(errs) > 0 {
		return nil, status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
	}
	// Validation bounds purpose to the enum's range; which purposes this
	// server actually supports is a policy decision that stays here.
	if req.GetPurpose() != ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL {
		return nil, status.Error(codes.InvalidArgument, "unsupported actor certificate purpose")
	}
	actor, actorRef, err := s.authorizeActor(ctx, caller, req)
	if err != nil {
		return nil, err
	}
	atespace, actorName := actorRef.Atespace, actorRef.Name

	// expected_actor_uid picked which actor to mint for (see authorizeActor);
	// re-checking it here fails closed if the request crossed an assignment
	// change.
	actorUID := actor.GetMetadata().GetUid()
	if actorUID == "" {
		slog.ErrorContext(ctx, "MintCert: actor has no UID", slog.Any("actor", actorRef))
		return nil, status.Errorf(codes.Internal, "actor has no UID")
	}
	if req.GetExpectedActorUid() != actorUID {
		slog.WarnContext(ctx, "MintCert refused: expected actor UID does not match the placed actor",
			slog.Any("actor", actorRef), slog.String("expectedActorUID", req.GetExpectedActorUid()))
		return nil, status.Error(codes.FailedPrecondition, "worker assignment changed while minting actor certificate")
	}

	// Parse the CSR
	csr, err := x509.ParseCertificateRequest(req.GetCertificateSigningRequest())
	if err != nil {
		slog.ErrorContext(ctx, "Failed to parse CSR", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to parse CSR")
	}
	if err := csr.CheckSignature(); err != nil {
		slog.ErrorContext(ctx, "Failed to verify CSR signature", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to verify CSR signature")
	}

	spiffeURI := &url.URL{
		Scheme: "spiffe",
		Host:   "substrate-actor.local",
		Path:   path.Join("atespace", atespace, "actor", actorName),
	}
	template := &x509.Certificate{
		URIs:                  []*url.URL{spiffeURI},
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(actorCertificateLifetime),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		Issuer: pkix.Name{
			CommonName: "api.ate-system.svc.cluster.local",
		},
	}

	if err := substratex509.AddActorIdentityToCertificate(&substratex509.ActorIdentity{
		Atespace:  atespace,
		ActorName: actorName,
		ActorUid:  actorUID,
		Purpose:   substratex509.ActorIdentityPurposeAtunnel,
	}, template); err != nil {
		slog.ErrorContext(ctx, "Failed to add ActorIdentity extension", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to build certificate")
	}

	// Sign and return the actor cert.
	chain, err := s.actorIDCAPool.CreateCertificate(template, csr.PublicKey)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to sign certificate", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to sign certificate")
	}

	return &ateapipb.MintCertResponse{
		ActorCertificates: chain,
	}, nil
}

func validateMintJWTRequest(ctx context.Context, req *ateapipb.MintJWTRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return controlapi.Validate_MintJWTRequest(ctx, op, nil, req, nil)
}

func validateMintCertRequest(ctx context.Context, req *ateapipb.MintCertRequest) field.ErrorList {
	// Call the generated validation.
	op := operation.Operation{Type: operation.Create}
	return controlapi.Validate_MintCertRequest(ctx, op, nil, req, nil)
}

// authorizeActor resolves the actor the request names among those the worker is
// hosting and verifies the two still point at one another. Requester-supplied
// identity never participates in the decision.
//
// The worker is resolved from cache first (hot path), but cache misses and
// denials fall back to the authoritative store to handle watch-delivery lag
// right after ResumeActor.
func (s *Server) authorizeActor(ctx context.Context, caller *ateletauth.Caller, req *ateapipb.MintCertRequest) (*ateapipb.Actor, resources.ActorRef, error) {
	reason := "worker not found"
	worker, err := s.workers.Worker(req.GetWorker().GetName())
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		slog.ErrorContext(ctx, "ActorIdentity: failed to read worker", slog.Any("err", err))
		return nil, resources.ActorRef{}, status.Error(codes.Internal, "failed to look up worker")
	}
	if err == nil {
		actor, actorRef, mismatchReason, err := s.authorizeWithWorker(ctx, worker, caller, req)
		if err == nil {
			return actor, actorRef, nil
		}
		if !errors.Is(err, errAssignmentMismatch) {
			return nil, resources.ActorRef{}, err // e.g. actor lookup failed
		}
		reason = mismatchReason
	}

	// Read-through: re-check the authoritative worker from the store on a
	// cache miss or assignment mismatch. Only fresh data may authorize, and
	// only fresh data may deny.
	fresh, ferr := s.store.GetWorker(ctx, req.GetWorker().GetName())
	if ferr != nil {
		if !errors.Is(ferr, store.ErrNotFound) {
			slog.ErrorContext(ctx, "ActorIdentity: read-through worker lookup failed", slog.Any("err", ferr))
		}
		return nil, resources.ActorRef{}, s.denyMint(ctx, caller, req, reason) // the cached verdict stands
	}

	actor, actorRef, retryReason, retryErr := s.authorizeWithWorker(ctx, fresh, caller, req)
	if retryErr != nil {
		if errors.Is(retryErr, errAssignmentMismatch) {
			return nil, resources.ActorRef{}, s.denyMint(ctx, caller, req, retryReason)
		}
		return nil, resources.ActorRef{}, retryErr
	}

	slog.InfoContext(ctx, "ActorIdentity: authorized via store read-through; worker cache was stale",
		slog.String("worker", req.GetWorker().GetName()))
	return actor, actorRef, nil
}

// denyMint logs the internal reason and returns a uniform PermissionDenied.
// Denials are deliberately indistinguishable from each other: a caller that
// is not entitled to a worker should not learn its assignment.
func (s *Server) denyMint(ctx context.Context, caller *ateletauth.Caller, req *ateapipb.MintCertRequest, reason string, args ...any) error {
	slog.WarnContext(ctx, "ActorIdentity denied: "+reason,
		append([]any{slog.String("worker", req.GetWorker().GetName()), slog.String("callerPod", caller.PodName), slog.String("callerNode", caller.NodeName)}, args...)...)
	return status.Error(codes.PermissionDenied, "caller is not permitted to mint credentials for this actor")
}

var errAssignmentMismatch = errors.New("assignment mismatch")

// authorizeWithWorker returns errAssignmentMismatch and a reason string if the authorization failed
// due to an assignment mismatch, indicating the caller may want to refetch the worker and retry.
func (s *Server) authorizeWithWorker(ctx context.Context, worker *ateapipb.Worker, caller *ateletauth.Caller, req *ateapipb.MintCertRequest) (*ateapipb.Actor, resources.ActorRef, string, error) {
	if worker.GetNodeName() != caller.NodeName {
		return nil, resources.ActorRef{}, "worker is hosted on a different node", errAssignmentMismatch
	}

	assigned, err := s.assignmentToMintFor(ctx, worker.GetMetadata().GetName(), req.GetExpectedActorUid())
	if errors.Is(err, store.ErrNotFound) {
		return nil, resources.ActorRef{}, "worker is not hosting the requested actor", errAssignmentMismatch
	}
	if err != nil {
		slog.ErrorContext(ctx, "ActorIdentity: failed to read worker assignment", slog.Any("err", err))
		return nil, resources.ActorRef{}, "", status.Error(codes.Internal, "failed to look up worker assignment")
	}
	actorRef := resources.ActorRefFromObjectRef(assigned.GetActor())
	if actorRef == (resources.ActorRef{}) {
		return nil, resources.ActorRef{}, "worker assignment names no actor", errAssignmentMismatch
	}

	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, resources.ActorRef{}, "assigned actor not found", errAssignmentMismatch
		}
		slog.ErrorContext(ctx, "ActorIdentity: failed to read actor", slog.Any("actor", actorRef), slog.Any("err", err))
		return nil, resources.ActorRef{}, "", status.Error(codes.Internal, "failed to look up actor")
	}

	// Refuse credential minting if the actor is being deleted. Under force deletion,
	// an actor enters ACTOR_STATE_DELETING while its worker assignment is still active.
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
		slog.WarnContext(ctx, "ActorIdentity refused: actor is being deleted", slog.Any("actor", actorRef))
		return nil, resources.ActorRef{}, "", status.Error(codes.FailedPrecondition, "actor is being deleted")
	}

	assignment := actor.GetStatus().GetWorkerAssignment()
	if assignment == nil {
		slog.ErrorContext(ctx, "ActorIdentity: running actor has no worker assignment", slog.Any("actor", actorRef))
		return nil, resources.ActorRef{}, "", status.Error(codes.FailedPrecondition, "actor has no worker assigned")
	}
	if assigned.GetActorUid() != actor.GetMetadata().GetUid() {
		return nil, resources.ActorRef{}, "worker is no longer assigned to this actor incarnation", errAssignmentMismatch
	}
	if assignment.GetWorker().GetName() != worker.GetMetadata().GetName() {
		return nil, resources.ActorRef{}, "actor no longer points to the requesting worker", errAssignmentMismatch
	}
	return actor, actorRef, "", nil
}

// assignmentToMintFor picks which of the worker's actors to mint for, or
// ErrNotFound when it hosts none. expected_actor_uid selects from what ateapi
// records the worker as hosting; it does not assert.
//
// Falling back to another of the worker's assignments keeps a bad binding
// (PermissionDenied) apart from a stale expectation (retryable). Reads go to
// the store: the binding was committed moments ago and the watch has not
// delivered it. Only one other assignment is needed to tell the two apart, so
// the fallback reads a single row rather than the Worker's whole occupancy.
func (s *Server) assignmentToMintFor(ctx context.Context, workerName, actorUID string) (*ateapipb.ActorAssignment, error) {
	assigned, err := s.store.GetWorkerAssignment(ctx, workerName, actorUID)
	if err == nil {
		return assigned, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	page, err := s.store.ListWorkerAssignments(ctx, workerName, store.ListOptions{PageSize: 1})
	if err != nil {
		return nil, err
	}
	if len(page.Items) == 0 {
		return nil, store.ErrNotFound
	}
	return page.Items[0], nil
}

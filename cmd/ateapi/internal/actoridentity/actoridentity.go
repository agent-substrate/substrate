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
	"os"
	"path"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/actoridjwt"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/workercache"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Server implements ateapipb.ActorIdentityServer
type Server struct {
	ateapipb.UnimplementedActorIdentityServer

	actorIdentityJWTIssuer string

	// TODO: Cache the signing keys in memory, so we don't read from a file every time.
	actorIDJWTPoolFile string
	actorIDCAPoolFile  string

	// store is the actor database. MintCert consults it to confirm the caller
	// is entitled to the actor it is asking for a credential for.
	store   store.Interface
	workers *workercache.Cache
}

var _ ateapipb.ActorIdentityServer = (*Server)(nil)

func New(actorIdentityJWTIssuer, actorIDJWTPoolFile, actorIDCAPoolFile string, store store.Interface, workers *workercache.Cache) *Server {
	return &Server{
		actorIdentityJWTIssuer: actorIdentityJWTIssuer,
		actorIDJWTPoolFile:     actorIDJWTPoolFile,
		actorIDCAPoolFile:      actorIDCAPoolFile,
		store:                  store,
		workers:                workers,
	}
}

// The SPIFFE identity that atelet client certs carry, as minted by the
// podidentity signer (cmd/podcertcontroller/internal/podidentitysigner).
//
// These mirror the constants the atelet dialer verifies against in
// cmd/ateapi/internal/controlapi/dialer.go. They are duplicated rather than
// imported so that this package does not depend on controlapi for three
// strings; if a third pkg that need these constants appears, they should move to a shared package.
const (
	ateletTrustDomain        = "cluster.local"
	ateletNamespace          = "ate-system"
	ateletSA                 = "atelet"
	actorCertificateLifetime = time.Hour
)

func (s *Server) MintJWT(ctx context.Context, req *ateapipb.MintJWTRequest) (*ateapipb.MintJWTResponse, error) {
	caller, ok := principal.FromContext(ctx)
	if !ok || caller.Kind != principal.KindJWT {
		return nil, status.Errorf(codes.Unauthenticated, "JWT authentication is required")
	}
	if caller.Issuer != s.actorIdentityJWTIssuer {
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor JWTs")
	}

	// TODO: Cross-check the verified caller and requested actor against the actor database.

	// TODO: Cache signing keys in memory, so we don't read from disk every time.
	signingPoolBytes, err := os.ReadFile(s.actorIDJWTPoolFile)
	if err != nil {
		return nil, fmt.Errorf("while reading signing pool bytes: %w", err)
	}

	signingPool, err := localjwtauthority.Unmarshal(signingPoolBytes)
	if err != nil {
		return nil, fmt.Errorf("while unmarshaling signing pool: %w", err)
	}
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
			ActorUid:  req.GetActorUid(),
		},
	}

	actorWireClaims, err := actoridjwt.ClaimsToWire(actorClaims)
	if err != nil {
		return nil, fmt.Errorf("while making actor JWT claims: %w", err)
	}

	// Assume the first authority is the one to use for signing.
	actorJWT, err := actoridjwt.Sign(actorWireClaims, signingPool.Authorities[0].SigningKey, signingPool.Authorities[0].Algorithm, signingPool.Authorities[0].ID)
	if err != nil {
		return nil, fmt.Errorf("while signing actor JWT: %w", err)
	}

	return &ateapipb.MintJWTResponse{
		ActorJwt: actorJWT,
	}, nil
}

func (s *Server) MintCert(ctx context.Context, req *ateapipb.MintCertRequest) (*ateapipb.MintCertResponse, error) {
	caller, err := authenticateAtelet(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetPurpose() != ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL {
		return nil, status.Error(codes.InvalidArgument, "unsupported actor certificate purpose")
	}

	if err := validateWorkerRef(req.GetWorker()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid worker: %v", err)
	}
	if req.GetExpectedActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "expected_actor_uid is required")
	}
	actor, actorRef, err := s.authorizeActor(ctx, caller, req)
	if err != nil {
		return nil, err
	}
	atespace, actorName := actorRef.Atespace, actorRef.Name

	// Actor identity comes only from ateapi state. expected_actor_uid is a
	// fail-closed guard against a request crossing an assignment change.
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

	// Load the CA pool for signing
	poolBytes, err := os.ReadFile(s.actorIDCAPoolFile)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read actor CA pool file", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to load actor CA")
	}
	caPool, err := localca.Unmarshal(poolBytes)
	if err != nil || len(caPool.CAs) == 0 {
		slog.ErrorContext(ctx, "Failed to load actor CA", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to load actor CA")
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
	ca := caPool.CAs[0]
	derBytes, err := x509.CreateCertificate(rand.Reader, template, ca.RootCertificate, csr.PublicKey, ca.SigningKey)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to sign certificate", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to sign certificate")
	}

	certificates := [][]byte{derBytes}
	for _, intermed := range ca.IntermediateCertificates {
		certificates = append(certificates, intermed.Raw)
	}

	return &ateapipb.MintCertResponse{
		ActorCertificates: certificates,
	}, nil
}

// ateletCaller is the verified identity of an atelet requesting an actor credential.
type ateletCaller struct {
	podName  string
	nodeName string
}

// authenticateAtelet verifies that the RPC arrived over mTLS from an atelet,
// and returns the identity that atelet's certificate asserts.
//
// The certificate chain is already verified by the TLS layer against the
// pod-identity CA (see buildServerCreds in cmd/ateapi/main.go), so the
// extensions read here are trustworthy: only the pod-identity signer can mint
// a certificate carrying a given pod's node name.
func authenticateAtelet(ctx context.Context) (*ateletCaller, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "no peer transport information found")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "unexpected peer transport credentials")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "could not verify peer certificate")
	}
	leaf := tlsInfo.State.PeerCertificates[0]

	// Only atelet may mint actor credentials. Everything else with a valid
	// pod-identity certificate — including the actor workloads themselves — is
	// rejected here.
	expected := (&url.URL{
		Scheme: "spiffe",
		Host:   ateletTrustDomain,
		Path:   path.Join("ns", ateletNamespace, "sa", ateletSA),
	}).String()
	if len(leaf.URIs) == 0 || leaf.URIs[0].String() != expected {
		slog.WarnContext(ctx, "ActorIdentity denied: caller is not atelet",
			slog.Any("uris", leaf.URIs), slog.String("expected", expected))
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor credentials")
	}

	identity, err := substratex509.PodIdentityFromCertificate(leaf)
	if err != nil {
		slog.WarnContext(ctx, "ActorIdentity denied: malformed PodIdentity extension", slog.Any("err", err))
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor credentials")
	}
	if identity == nil {
		slog.WarnContext(ctx, "ActorIdentity denied: certificate has no PodIdentity extension")
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor credentials")
	}

	return &ateletCaller{podName: identity.PodName, nodeName: identity.NodeName}, nil
}

// validateWorkerRef checks the reference to the Worker the certificate is
// minted for. Workers are global-scoped, so the reference carries no atespace.
func validateWorkerRef(worker *ateapipb.ObjectRef) error {
	return resources.ValidateGlobalObjectRef(worker, field.NewPath("worker")).ToAggregate()
}

// authorizeActor resolves the actor from the authenticated worker and verifies
// that the worker and actor still point at one another. Actor identity supplied
// by the requester never participates in this authorization decision.
// The worker is resolved from cache first (hot path), but cache misses and
// denials fall back to the authoritative store to handle watch-delivery lag
// right after ResumeActor.
func (s *Server) authorizeActor(ctx context.Context, caller *ateletCaller, req *ateapipb.MintCertRequest) (*ateapipb.Actor, resources.ActorRef, error) {
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
func (s *Server) denyMint(ctx context.Context, caller *ateletCaller, req *ateapipb.MintCertRequest, reason string, args ...any) error {
	slog.WarnContext(ctx, "ActorIdentity denied: "+reason,
		append([]any{slog.String("worker", req.GetWorker().GetName()), slog.String("callerPod", caller.podName), slog.String("callerNode", caller.nodeName)}, args...)...)
	return status.Error(codes.PermissionDenied, "caller is not permitted to mint credentials for this actor")
}

var errAssignmentMismatch = errors.New("assignment mismatch")

// authorizeWithWorker returns errAssignmentMismatch and a reason string if the authorization failed
// due to an assignment mismatch, indicating the caller may want to refetch the worker and retry.
func (s *Server) authorizeWithWorker(ctx context.Context, worker *ateapipb.Worker, caller *ateletCaller, req *ateapipb.MintCertRequest) (*ateapipb.Actor, resources.ActorRef, string, error) {
	if worker.GetNodeName() != caller.nodeName {
		return nil, resources.ActorRef{}, "worker is hosted on a different node", errAssignmentMismatch
	}

	actorRef := resources.ActorRefFromObjectRef(worker.GetStatus().GetAssignment().GetActor())
	if actorRef == (resources.ActorRef{}) {
		return nil, resources.ActorRef{}, "worker has no actor assignment", errAssignmentMismatch
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
	if worker.GetStatus().GetAssignment().GetActorUid() != actor.GetMetadata().GetUid() {
		return nil, resources.ActorRef{}, "worker is no longer assigned to this actor incarnation", errAssignmentMismatch
	}
	if assignment.GetWorker().GetName() != worker.GetMetadata().GetName() {
		return nil, resources.ActorRef{}, "actor no longer points to the requesting worker", errAssignmentMismatch
	}
	return actor, actorRef, "", nil
}

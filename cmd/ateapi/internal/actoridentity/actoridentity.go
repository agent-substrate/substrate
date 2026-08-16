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

	if req.GetWorkerNamespace() == "" || req.GetWorkerPod() == "" || req.GetWorkerPodUid() == "" || req.GetExpectedActorUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_namespace, worker_pod, worker_pod_uid, and expected_actor_uid are required")
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

// authorizeActor resolves the actor from the authenticated worker and verifies
// that the worker and actor still point at one another. Actor identity supplied
// by the requester never participates in this authorization decision.
func (s *Server) authorizeActor(ctx context.Context, caller *ateletCaller, req *ateapipb.MintCertRequest) (*ateapipb.Actor, resources.ActorRef, error) {
	// Denials are deliberately indistinguishable from each other: a caller that
	// is not entitled to a worker should not learn its assignment.
	deny := func(reason string, args ...any) error {
		slog.WarnContext(ctx, "ActorIdentity denied: "+reason,
			append([]any{slog.String("workerPod", req.GetWorkerNamespace()+"/"+req.GetWorkerPod()), slog.String("callerPod", caller.podName), slog.String("callerNode", caller.nodeName)}, args...)...)
		return status.Errorf(codes.PermissionDenied, "caller is not permitted to mint credentials for this actor")
	}

	worker, err := s.workers.Worker(req.GetWorkerNamespace(), req.GetWorkerPod())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, resources.ActorRef{}, deny("worker not found")
		}
		slog.ErrorContext(ctx, "ActorIdentity: failed to read worker", slog.Any("err", err))
		return nil, resources.ActorRef{}, status.Error(codes.Internal, "failed to look up worker")
	}
	if worker.GetNodeName() != caller.nodeName {
		return nil, resources.ActorRef{}, deny("worker is hosted on a different node", slog.String("workerNode", worker.GetNodeName()))
	}
	if worker.GetWorkerPodUid() != req.GetWorkerPodUid() {
		return nil, resources.ActorRef{}, deny("worker Pod UID does not match", slog.String("workerPodUID", req.GetWorkerPodUid()))
	}

	actorRef := resources.ActorRefFromObjectRef(worker.GetAssignment().GetActor())
	if actorRef == (resources.ActorRef{}) {
		return nil, resources.ActorRef{}, deny("worker has no actor assignment")
	}
	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, resources.ActorRef{}, deny("assigned actor not found")
		}
		slog.ErrorContext(ctx, "ActorIdentity: failed to read actor", slog.Any("actor", actorRef), slog.Any("err", err))
		return nil, resources.ActorRef{}, status.Error(codes.Internal, "failed to look up actor")
	}

	// Deletion is only entered from SUSPENDED or CRASHED, both of which
	// have already released the worker, so the assignment check below would
	// reject this too. It is kept for better visibility and logging.
	if actor.GetStatus() == ateapipb.Actor_STATUS_DELETING {
		slog.WarnContext(ctx, "ActorIdentity refused: actor is being deleted", slog.Any("actor", actorRef))
		return nil, resources.ActorRef{}, status.Error(codes.FailedPrecondition, "actor is being deleted")
	}

	// An actor placed on a worker always carries its placement fields. Missing
	// placement is a control-plane bug rather than a client error, so it is not
	// folded into deny().
	assignment := actor.GetWorkerAssignment()
	if assignment == nil {
		slog.ErrorContext(ctx, "ActorIdentity: running actor has no worker assignment", slog.Any("actor", actorRef))
		return nil, resources.ActorRef{}, status.Error(codes.FailedPrecondition, "actor has no worker assigned")
	}
	if worker.GetAssignment().GetActorUid() != actor.GetMetadata().GetUid() {
		return nil, resources.ActorRef{}, deny("worker is no longer assigned to this actor incarnation", slog.Any("actor", actorRef))
	}
	if assignment.GetWorkerNamespace() != worker.GetWorkerNamespace() ||
		assignment.GetWorkerPool() != worker.GetWorkerPool() ||
		assignment.GetWorkerPod() != worker.GetWorkerPod() ||
		assignment.GetWorkerPodUid() != worker.GetWorkerPodUid() {
		return nil, resources.ActorRef{}, deny("actor no longer points to the requesting worker", slog.Any("actor", actorRef))
	}
	return actor, actorRef, nil
}

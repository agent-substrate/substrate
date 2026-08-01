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
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/actoridjwt"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/k8sjwt"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server implements ateapipb.ActorIdentityServer
type Server struct {
	ateapipb.UnimplementedActorIdentityServer

	clientJWTIssuer   string
	clientJWTAudience string

	// TODO: Cache the signing keys in memory, so we don't read from a file every time.
	actorIDJWTPoolFile string
	actorIDCAPoolFile  string

	workerCACerts string
	httpClient    *http.Client

	// store is the actor database. MintCert consults it to confirm the caller
	// is entitled to the actor it is asking for a credential for.
	store store.Interface
}

var _ ateapipb.ActorIdentityServer = (*Server)(nil)

func New(clientJWTIssuer, clientJWTAudience, actorIDJWTPoolFile, actorIDCAPoolFile, workerCACerts string, httpClient *http.Client, store store.Interface) *Server {
	return &Server{
		clientJWTIssuer:    clientJWTIssuer,
		clientJWTAudience:  clientJWTAudience,
		actorIDJWTPoolFile: actorIDJWTPoolFile,
		actorIDCAPoolFile:  actorIDCAPoolFile,
		workerCACerts:      workerCACerts,
		httpClient:         httpClient,
		store:              store,
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
	ateletTrustDomain = "cluster.local"
	ateletNamespace   = "ate-system"
	ateletSA          = "atelet"
)

func (s *Server) MintJWT(ctx context.Context, req *ateapipb.MintJWTRequest) (*ateapipb.MintJWTResponse, error) {
	reqMetadata, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no metadata found")
	}

	authorization := reqMetadata["authorization"]
	if len(authorization) != 1 {
		return nil, status.Errorf(codes.Unauthenticated, "Need authorization header")
	}

	clientJWT := strings.TrimPrefix(authorization[0], "Bearer ")

	clientClaims, err := k8sjwt.Verify(ctx, s.httpClient, clientJWT, s.clientJWTIssuer, s.clientJWTAudience, time.Now())
	if err != nil {
		slog.ErrorContext(ctx, "Error while verifying client JWT", slog.Any("err", err))
		return nil, status.Errorf(codes.Unauthenticated, "Unauthenticated")
	}

	slog.InfoContext(ctx, "Verified client JWT", slog.Any("claims", clientClaims))

	// TODO: Extract K8s identity from incoming JWT

	// TODO: Cross-check requested actor and user claims against the actor database.

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

	atespace := req.GetAtespace()
	actorName := req.GetActorName()

	if atespace == "" || actorName == "" {
		return nil, status.Errorf(codes.InvalidArgument, "atespace and actor_name are required")
	}

	actorRef := resources.ActorRef{Atespace: atespace, Name: actorName}
	actor, err := s.authorizeActor(ctx, caller, actorRef)
	if err != nil {
		return nil, err
	}

	// The UID is taken from the actor database rather than from the request:
	// req.actor_uid is caller-supplied and unverified, and the certificate must
	// name the incarnation of the actor that is actually placed. A request
	// that names a different incarnation is refused rather than silently
	// upgraded, since the caller is asking for a credential it would not be
	// able to use.
	actorUID := actor.GetMetadata().GetUid()
	if actorUID == "" {
		slog.ErrorContext(ctx, "MintCert: actor has no UID", slog.Any("actor", actorRef))
		return nil, status.Errorf(codes.Internal, "actor has no UID")
	}
	if reqUID := req.GetActorUid(); reqUID != "" && reqUID != actorUID {
		slog.WarnContext(ctx, "MintCert denied: requested actor UID does not match the placed actor",
			slog.Any("actor", actorRef), slog.String("requestedUID", reqUID))
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint certificates for this actor")
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
		NotAfter:              time.Now().Add(15 * time.Minute),
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

// ateletCaller is the verified identity of an atelet that called MintCert.
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

	// Only atelet may mint actor certificates. Everything else with a valid
	// pod-identity certificate — including the actor workloads themselves — is
	// rejected here.
	expected := (&url.URL{
		Scheme: "spiffe",
		Host:   ateletTrustDomain,
		Path:   path.Join("ns", ateletNamespace, "sa", ateletSA),
	}).String()
	if len(leaf.URIs) == 0 || leaf.URIs[0].String() != expected {
		slog.WarnContext(ctx, "MintCert denied: caller is not atelet",
			slog.Any("uris", leaf.URIs), slog.String("expected", expected))
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor certificates")
	}

	identity, err := substratex509.PodIdentityFromCertificate(leaf)
	if err != nil {
		slog.WarnContext(ctx, "MintCert denied: malformed PodIdentity extension", slog.Any("err", err))
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor certificates")
	}
	if identity == nil {
		slog.WarnContext(ctx, "MintCert denied: certificate has no PodIdentity extension")
		return nil, status.Errorf(codes.PermissionDenied, "caller is not permitted to mint actor certificates")
	}

	return &ateletCaller{podName: identity.PodName, nodeName: identity.NodeName}, nil
}

// authorizeActor reports whether caller may mint a credential for actorRef,
// returning the actor record the decision was made against.
//
// The rule is that the actor must be placed on a worker pod that lives on the
// caller's own node, and that worker must still agree it is hosting the actor.
// An atelet is therefore confined to the actors it is actually hosting, and an
// actor that has been suspended, paused or migrated elsewhere can no longer
// have credentials minted for it.
func (s *Server) authorizeActor(ctx context.Context, caller *ateletCaller, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	// Denials are deliberately indistinguishable from each other: a caller that
	// is not entitled to an actor should not be able to use this RPC to learn
	// whether that actor exists, or where it is running.
	deny := func(reason string, args ...any) error {
		slog.WarnContext(ctx, "MintCert denied: "+reason,
			append([]any{slog.Any("actor", actorRef), slog.String("callerPod", caller.podName), slog.String("callerNode", caller.nodeName)}, args...)...)
		return status.Errorf(codes.PermissionDenied, "caller is not permitted to mint certificates for this actor")
	}

	actor, err := s.store.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, deny("actor not found")
		}
		slog.ErrorContext(ctx, "MintCert: failed to read actor", slog.Any("actor", actorRef), slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "failed to look up actor")
	}

	// Deletion is only entered from SUSPENDED or CRASHED, both of which
	// have already released the worker, so the assignment check below would
	// reject this too. It is kept because minting for better visbility and logging.
	if actor.GetStatus() == ateapipb.Actor_STATUS_DELETING {
		slog.WarnContext(ctx, "MintCert refused: actor is being deleted", slog.Any("actor", actorRef))
		return nil, status.Errorf(codes.FailedPrecondition, "actor is being deleted")
	}

	// An actor placed on a worker always carries its placement fields. Missing
	// placement is a control-plane bug rather than a client error, so it is not
	// folded into deny().
	podNamespace, podName, pool := actor.GetAteomPodNamespace(), actor.GetAteomPodName(), actor.GetWorkerPoolName()
	if podNamespace == "" || podName == "" || pool == "" {
		slog.ErrorContext(ctx, "MintCert: running actor has incomplete placement",
			slog.Any("actor", actorRef), slog.String("podNamespace", podNamespace),
			slog.String("podName", podName), slog.String("workerPool", pool))
		return nil, status.Errorf(codes.FailedPrecondition, "actor has no worker assigned")
	}

	worker, err := s.store.GetWorker(ctx, podNamespace, pool, podName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, deny("worker hosting the actor not found", slog.String("workerPod", podNamespace+"/"+podName))
		}
		slog.ErrorContext(ctx, "MintCert: failed to read worker", slog.Any("actor", actorRef), slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "failed to look up worker")
	}

	if worker.GetNodeName() != caller.nodeName {
		return nil, deny("actor is hosted on a different node", slog.String("actorNode", worker.GetNodeName()))
	}

	// The worker must still agree that it is hosting this actor.
	if assigned := worker.GetAssignment().GetActor(); resources.ActorRefFromObjectRef(assigned) != actorRef {
		return nil, deny("worker is no longer assigned to the actor",
			slog.String("workerAssignment", assigned.GetAtespace()+"/"+assigned.GetName()))
	}

	return actor, nil
}

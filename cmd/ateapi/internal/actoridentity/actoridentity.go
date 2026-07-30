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
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
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
}

var _ ateapipb.ActorIdentityServer = (*Server)(nil)

func New(clientJWTIssuer, clientJWTAudience, actorIDJWTPoolFile, actorIDCAPoolFile, workerCACerts string, httpClient *http.Client) *Server {
	return &Server{
		clientJWTIssuer:    clientJWTIssuer,
		clientJWTAudience:  clientJWTAudience,
		actorIDJWTPoolFile: actorIDJWTPoolFile,
		actorIDCAPoolFile:  actorIDCAPoolFile,
		workerCACerts:      workerCACerts,
		httpClient:         httpClient,
	}
}

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
		Issuer:     "https://broker.agentic-substrate-actor-id-broker.svc", // TODO: This needs to be globally unique.
		Subject:    fmt.Sprintf("apps/%s/users/%s/actors/%s", req.GetAppId(), req.GetUserId(), req.GetActorId()),
		Audiences:  req.GetAudience(),
		Expiration: time.Now().Add(15 * time.Minute),
		NotBefore:  time.Now().Add(-5 * time.Minute),
		IssuedAt:   time.Now(),
		JTI:        rand.Text(),

		Substrate: actoridjwt.SubstrateClaims{
			AppID:   req.GetAppId(),
			UserID:  req.GetUserId(),
			ActorID: req.GetActorId(),
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

	// TODO: How to verify pod cert <-> actor mapping?
	appID := req.GetAppId()
	userID := req.GetUserId()
	actorID := req.GetActorId()

	if appID == "" || userID == "" || actorID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "app_id, user_id, and actor_id are required")
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
		Path:   path.Join("app", appID, "user", userID, "actor", actorID),
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

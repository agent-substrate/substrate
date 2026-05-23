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

package sessionidentity

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/k8sjwt"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/sessionidjwt"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server implements ateapipb.SessionIdentityServer
type Server struct {
	ateapipb.UnimplementedSessionIdentityServer

	clientJWTIssuer   string
	clientJWTAudience string

	// TODO: Cache the signing keys in memory, so we don't read from a file every time.
	sessionIDJWTPoolFile string
	sessionIDCAPoolFile  string

	workerCACerts string
}

var _ ateapipb.SessionIdentityServer = (*Server)(nil)

func New(clientJWTIssuer, clientJWTAudience, sessionIDJWTPoolFile, sessionIDCAPoolFile, workerCACerts string) *Server {
	return &Server{
		clientJWTIssuer:      clientJWTIssuer,
		clientJWTAudience:    clientJWTAudience,
		sessionIDJWTPoolFile: sessionIDJWTPoolFile,
		sessionIDCAPoolFile:  sessionIDCAPoolFile,
		workerCACerts:        workerCACerts,
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

	clientClaims, err := k8sjwt.Verify(ctx, clientJWT, s.clientJWTIssuer, s.clientJWTAudience, time.Now())
	if err != nil {
		slog.ErrorContext(ctx, "Error while verifying client JWT", slog.Any("err", err))
		return nil, status.Errorf(codes.Unauthenticated, "Unauthenticated")
	}

	slog.InfoContext(ctx, "Verified client JWT", slog.Any("claims", clientClaims))

	// TODO: Extract K8s identity from incoming JWT

	// TODO: Cross-check requested session and user claims against the session database.

	// TODO: Cache signing keys in memory, so we don't read from disk every time.
	signingPoolBytes, err := os.ReadFile(s.sessionIDJWTPoolFile)
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

	sessionClaims := &sessionidjwt.Claims{
		Issuer:     "https://broker.agentic-substrate-session-id-broker.svc", // TODO: This needs to be globally unique.
		Subject:    fmt.Sprintf("apps/%s/users/%s/sessions/%s", req.GetAppId(), req.GetUserId(), req.GetSessionId()),
		Audiences:  req.GetAudience(),
		Expiration: time.Now().Add(15 * time.Minute),
		NotBefore:  time.Now().Add(-5 * time.Minute),
		IssuedAt:   time.Now(),
		JTI:        rand.Text(),

		Substrate: sessionidjwt.SubstrateClaims{
			AppID:     req.GetAppId(),
			UserID:    req.GetUserId(),
			SessionID: req.GetSessionId(),
		},
	}

	sessionWireClaims, err := sessionidjwt.ClaimsToWire(sessionClaims)
	if err != nil {
		return nil, fmt.Errorf("while making session JWT claims: %w", err)
	}

	// Assume the first authority is the one to use for signing.
	sessionJWT, err := sessionidjwt.Sign(sessionWireClaims, signingPool.Authorities[0].SigningKey, signingPool.Authorities[0].Algorithm, signingPool.Authorities[0].ID)
	if err != nil {
		return nil, fmt.Errorf("while signing session JWT: %w", err)
	}

	return &ateapipb.MintJWTResponse{
		SessionJwt: sessionJWT,
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

	// TODO: How to verify pod cert <-> session mapping?
	appID := req.GetAppId()
	userID := req.GetUserId()
	sessionID := req.GetSessionId()

	if appID == "" || userID == "" || sessionID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "app_id, user_id, and session_id are required")
	}

	// Load the CA pool for signing
	poolBytes, err := os.ReadFile(s.sessionIDCAPoolFile)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to read session CA pool file", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to load session CA")
	}
	caPool, err := localca.Unmarshal(poolBytes)
	if err != nil || len(caPool.CAs) == 0 {
		slog.ErrorContext(ctx, "Failed to load session CA", slog.Any("err", err))
		return nil, status.Errorf(codes.Internal, "Failed to load session CA")
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
		Host:   "substrate-session.local",
		Path:   path.Join("app", appID, "user", userID, "session", sessionID),
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

	// Sign and return the session cert.
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
		SessionCertificates: certificates,
	}, nil
}

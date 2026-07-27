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

// Package atunnel carries actor ingress and egress through an ateom worker pod.
package atunnel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	// StaleAssignmentHeader distinguishes an atunnel routing rejection from a
	// 421 returned by the actor application itself.
	StaleAssignmentHeader = "X-Ate-Assignment-Stale"
)

// Config configures an ingress Server.
type Config struct {
	CredentialBundlePath string
	TrustBundlePath      string
	AllowedClientID      string
	Upstream             *url.URL
}

// Server is an activation-aware HTTPS reverse proxy. It is long-lived across
// actor activations, but only routes requests for the actor currently assigned
// to its worker.
type Server struct {
	credentialBundlePath string
	tlsConfig            *tls.Config
	proxy                *httputil.ReverseProxy

	mu     sync.Mutex
	active *activation
}

type activation struct {
	atespace  string
	actorName string
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewServer creates a Server and validates its TLS material.
func NewServer(cfg Config) (*Server, error) {
	if cfg.CredentialBundlePath == "" {
		return nil, fmt.Errorf("atunnel: credential bundle path is required")
	}
	if cfg.TrustBundlePath == "" {
		return nil, fmt.Errorf("atunnel: trust bundle path is required")
	}
	if cfg.AllowedClientID == "" {
		return nil, fmt.Errorf("atunnel: allowed client identity is required")
	}
	if cfg.Upstream == nil || cfg.Upstream.Scheme == "" || cfg.Upstream.Host == "" {
		return nil, fmt.Errorf("atunnel: upstream URL is required")
	}

	// Load once at startup so a malformed or missing projection fails the pod
	// promptly. GetCertificate reloads the bundle for every new TLS connection,
	// allowing kubelet's projected certificate rotation to take effect.
	if _, err := loadCredentialBundle(cfg.CredentialBundlePath); err != nil {
		return nil, err
	}
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("atunnel: reading trust bundle: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("atunnel: trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}

	proxy := httputil.NewSingleHostReverseProxy(cfg.Upstream)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy.Transport = transport
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		slog.WarnContext(r.Context(), "atunnel upstream request failed", slog.Any("err", err))
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}

	s := &Server{
		credentialBundlePath: cfg.CredentialBundlePath,
		proxy:                proxy,
	}
	s.tlsConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCredentialBundle(s.credentialBundlePath)
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  clientCAs,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("atunnel: client certificate is required")
			}
			for _, uri := range cs.PeerCertificates[0].URIs {
				if uri.String() == cfg.AllowedClientID {
					return nil
				}
			}
			return fmt.Errorf("atunnel: client is not %q", cfg.AllowedClientID)
		},
	}
	return s, nil
}

func loadCredentialBundle(path string) (*tls.Certificate, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("atunnel: reading credential bundle: %w", err)
	}
	cert, err := tls.X509KeyPair(pemBytes, pemBytes)
	if err != nil {
		return nil, fmt.Errorf("atunnel: parsing credential bundle: %w", err)
	}
	return &cert, nil
}

// Serve serves HTTPS on lis until ctx is canceled or the server fails.
func (s *Server) Serve(ctx context.Context, lis net.Listener) error {
	httpServer := &http.Server{
		Handler:           s,
		TLSConfig:         s.tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = httpServer.Close()
		case <-done:
		}
	}()
	err := httpServer.ServeTLS(lis, "", "")
	close(done)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return nil
	}
	return err
}

// Activate allows requests for actorName in atespace. There can be only one
// active actor per worker.
func (s *Server) Activate(atespace, actorName string) error {
	if !resources.IsValidResourceName(atespace) || !resources.IsValidResourceName(actorName) {
		return fmt.Errorf("atunnel: invalid actor identity %q/%q", atespace, actorName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil {
		return fmt.Errorf("atunnel: actor %s/%s is already active", s.active.atespace, s.active.actorName)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.active = &activation{
		atespace:  atespace,
		actorName: actorName,
		ctx:       ctx,
		cancel:    cancel,
	}
	return nil
}

// Deactivate rejects new requests, cancels requests for the active actor, and
// waits for their handlers to exit before returning.
func (s *Server) Deactivate(ctx context.Context) error {
	s.mu.Lock()
	active := s.active
	s.active = nil
	if active != nil {
		active.cancel()
	}
	s.mu.Unlock()
	if active == nil {
		return nil
	}

	done := make(chan struct{})
	go func() {
		active.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.closeIdleUpstreamConnections()
		return nil
	case <-ctx.Done():
		s.closeIdleUpstreamConnections()
		return fmt.Errorf("atunnel: waiting for active requests to stop: %w", ctx.Err())
	}
}

func (s *Server) closeIdleUpstreamConnections() {
	if transport, ok := s.proxy.Transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

// ServeHTTP validates the actor hostname on every request before proxying it.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host, err := requestHostname(r.Host)
	if err != nil {
		s.reject(w)
		return
	}
	actorRef, err := resources.ParseActorDNSName(host)
	if err != nil {
		s.reject(w)
		return
	}

	s.mu.Lock()
	active := s.active
	if active == nil || active.atespace != actorRef.Atespace || active.actorName != actorRef.Name {
		s.mu.Unlock()
		s.reject(w)
		return
	}
	active.wg.Add(1)
	s.mu.Unlock()
	defer active.wg.Done()

	requestCtx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(active.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	// ReverseProxy changes the URL destination but intentionally retains Host,
	// allowing the actor application to observe its stable mesh hostname.
	s.proxy.ServeHTTP(w, r.WithContext(requestCtx))
}

func (s *Server) reject(w http.ResponseWriter) {
	w.Header().Set(StaleAssignmentHeader, "true")
	http.Error(w, "misdirected request", http.StatusMisdirectedRequest)
}

func requestHostname(hostport string) (string, error) {
	if hostport == "" {
		return "", fmt.Errorf("empty host")
	}
	host := hostport
	if strings.Contains(hostport, ":") {
		var port string
		var err error
		host, port, err = net.SplitHostPort(hostport)
		if err != nil {
			return "", fmt.Errorf("invalid host %q: %w", hostport, err)
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("invalid port in host %q", hostport)
		}
	}
	return strings.ToLower(host), nil
}

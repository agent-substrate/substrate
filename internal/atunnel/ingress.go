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
	"io"
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
	// DefaultConnectPort is the worker port on which atunnel accepts inbound
	// mTLS CONNECT tunnels from the ingress router.
	DefaultConnectPort = 8443

	// StaleAssignmentHeader distinguishes an atunnel routing rejection from a
	// 421 returned by the actor application itself.
	StaleAssignmentHeader = "X-Ate-Assignment-Stale"
	// OriginalHostHeader carries the actor authority across router dataplanes
	// that must use :authority to select the worker as their dynamic backend.
	// atunnel only accepts mTLS-authenticated router clients, and the router's
	// ext_proc server overwrites this header before every request.
	OriginalHostHeader = "X-Ate-Original-Host"

	// TargetPortHeader carries the port to reach on the actor: the CONNECT
	// :authority's port for arbitrary-port ingress, or the default 80
	// otherwise (see atenet-router's HandleRequestHeaders). cfg.Upstream is
	// fixed for the Server's lifetime, so this lets the port vary per
	// request; stripped before the request reaches the actor.
	TargetPortHeader = "X-Ate-Target-Port"
)

// ParsePort parses s as a TCP port number, returning ok=false for anything
// outside the valid 1-65535 range (including non-numeric input).
func ParsePort(s string) (port int, ok bool) {
	p, err := strconv.Atoi(s)
	if err != nil || p < 1 || p > 65535 {
		return 0, false
	}
	return p, true
}

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
	upstream             *url.URL

	mu     sync.Mutex
	active *activation
}

type activation struct {
	ref    resources.ActorRef
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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

	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(cfg.Upstream)
			// Retain the actor's stable mesh hostname rather than the
			// upstream's, matching NewSingleHostReverseProxy's default
			// behavior.
			pr.Out.Host = pr.In.Host

			port := pr.In.Header.Get(TargetPortHeader)
			pr.Out.Header.Del(TargetPortHeader)
			if p, ok := ParsePort(port); ok {
				pr.Out.URL.Host = net.JoinHostPort(cfg.Upstream.Hostname(), strconv.Itoa(p))
			}
			pr.SetXForwarded()
		},
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.WarnContext(r.Context(), "atunnel upstream request failed", slog.Any("err", err))
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}

	s := &Server{
		credentialBundlePath: cfg.CredentialBundlePath,
		proxy:                proxy,
		upstream:             cfg.Upstream,
	}
	s.tlsConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return loadCredentialBundle(s.credentialBundlePath)
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		// TODO(liorlieberman): reload the trust bundle per connection via
		// GetConfigForClient, mirroring GetCertificate above. kubelet keeps the
		// projected ClusterTrustBundle in sync with the signer, but this pool is
		// frozen at process start, so after a CA rotation a long-lived worker
		// rejects the router until its pod restarts.
		ClientCAs: clientCAs,
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
	return s.serve(ctx, lis, s, s.tlsConfig)
}

// ServeConnect serves the mTLS CONNECT endpoint. CONNECT is deliberately on a
// separate listener so ordinary actor ingress remains a request proxy, while
// the router can use this listener for a bidirectional tunnel.
func (s *Server) ServeConnect(ctx context.Context, lis net.Listener) error {
	// Keep ALPN confined to the CONNECT listener. The established ingress
	// listener is explicitly HTTP/1.1 in the router's upstream cluster, while
	// the CONNECT listener supports either HTTP/1.1 or HTTP/2.
	tlsConfig := s.tlsConfig.Clone()
	tlsConfig.NextProtos = []string{"h2", "http/1.1"}
	return s.serve(ctx, lis, http.HandlerFunc(s.ServeConnectHTTP), tlsConfig)
}

func (s *Server) serve(ctx context.Context, lis net.Listener, handler http.Handler, tlsConfig *tls.Config) error {
	httpServer := &http.Server{
		Handler:           handler,
		TLSConfig:         tlsConfig,
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

// ServeConnectHTTP accepts a router-authenticated CONNECT request and relays
// its tunnel to the named port on the currently active actor.
func (s *Server) ServeConnectHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
		return
	}
	ref, ctx, release, ok := s.authorize(r)
	if !ok {
		s.reject(w)
		return
	}
	defer release()

	_, port, err := net.SplitHostPort(r.Host)
	if err != nil {
		http.Error(w, "CONNECT authority must include a port", http.StatusBadRequest)
		return
	}
	if _, ok := ParsePort(port); !ok {
		http.Error(w, "invalid CONNECT port", http.StatusBadRequest)
		return
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	upstream, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(s.upstream.Hostname(), port))
	if err != nil {
		slog.WarnContext(r.Context(), "atunnel CONNECT upstream failed", slog.Any("actor", ref), slog.Any("err", err))
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	if r.ProtoMajor == 2 {
		s.serveH2Connect(w, r, upstream, ctx)
		return
	}
	s.serveH1Connect(w, upstream, ctx)
}

func (s *Server) serveH1Connect(w http.ResponseWriter, upstream net.Conn, ctx context.Context) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "CONNECT hijacking unsupported", http.StatusInternalServerError)
		return
	}
	client, rw, err := hj.Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}

	relayIngressWithHalfClose(ctx, upstream, rw, client, client)
}

func (s *Server) serveH2Connect(w http.ResponseWriter, r *http.Request, upstream net.Conn, ctx context.Context) {
	// A HTTP/2 CONNECT tunnel is a pair of streams, not a hijackable TCP
	// socket. Send the response headers before copying so the peer can start
	// sending DATA frames, then flush each upstream write promptly.
	w.WriteHeader(http.StatusOK)
	if err := http.NewResponseController(w).Flush(); err != nil {
		return
	}

	relayIngressWithHalfClose(ctx, upstream, r.Body, flushingWriter{ResponseWriter: w}, r.Body)
}

// relayIngressWithHalfClose copies a CONNECT stream. When the client request
// stream ends, it half-closes the actor connection and continues forwarding
// actor output until that stream ends too.
func relayIngressWithHalfClose(ctx context.Context, upstream net.Conn, clientReader io.Reader, clientWriter io.Writer, clientCloser io.Closer) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, clientReader)
		closeWrite(upstream)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(clientWriter, upstream)
		done <- struct{}{}
	}()
	for range 2 {
		select {
		case <-ctx.Done():
			_ = upstream.Close()
			_ = clientCloser.Close()
			return
		case <-done:
		}
	}
}

type flushingWriter struct {
	http.ResponseWriter
}

func (w flushingWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if err != nil {
		return n, err
	}
	if err := http.NewResponseController(w.ResponseWriter).Flush(); err != nil {
		return n, err
	}
	return n, nil
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
		return fmt.Errorf("atunnel: actor %s is already active", s.active.ref)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.active = &activation{
		ref:    resources.ActorRef{Atespace: atespace, Name: actorName},
		ctx:    ctx,
		cancel: cancel,
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
	_, requestCtx, release, ok := s.authorize(r)
	if !ok {
		s.reject(w)
		return
	}
	defer release()

	// Do not expose the router-only routing header to actor code. Restore Host
	// so dataplanes that route dynamically on worker IP still give the actor its
	// stable actor DNS name.
	actorHost := r.Header.Get(OriginalHostHeader)
	if actorHost == "" {
		actorHost = r.Host
	}
	r.Header.Del(OriginalHostHeader)
	r.Host = actorHost

	// ReverseProxy changes the URL destination but intentionally retains Host,
	// allowing the actor application to observe its stable actor DNS name.
	s.proxy.ServeHTTP(w, r.WithContext(requestCtx))
}

func (s *Server) authorize(r *http.Request) (resources.ActorRef, context.Context, func(), bool) {
	actorHost := r.Header.Get(OriginalHostHeader)
	if actorHost == "" {
		actorHost = r.Host
	}
	host, err := requestHostname(actorHost)
	if err != nil {
		return resources.ActorRef{}, nil, nil, false
	}
	ref, err := resources.ParseActorDNSName(host)
	if err != nil {
		return resources.ActorRef{}, nil, nil, false
	}

	s.mu.Lock()
	active := s.active
	if active == nil || active.ref != ref {
		s.mu.Unlock()
		return resources.ActorRef{}, nil, nil, false
	}
	active.wg.Add(1)
	s.mu.Unlock()
	requestCtx, cancel := context.WithCancel(r.Context())
	stop := context.AfterFunc(active.ctx, cancel)
	release := func() {
		active.wg.Done()
		stop()
		cancel()
	}
	return ref, requestCtx, release, true
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
		if _, ok := ParsePort(port); !ok {
			return "", fmt.Errorf("invalid port in host %q", hostport)
		}
	}
	return strings.ToLower(host), nil
}

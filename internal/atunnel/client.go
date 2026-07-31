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

package atunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	// ActorAtespaceHeader identifies the atespace whose actor opened an egress
	// tunnel. The egress gateway must authenticate this metadata before using it
	// for policy decisions.
	ActorAtespaceHeader = "X-Ate-Atespace"
	// ActorNameHeader identifies the actor that opened an egress tunnel.
	ActorNameHeader = "X-Ate-Actor-Name"
	// ActorVersionHeader is the Actor resource version observed when the worker
	// was assigned. Gateways use it as a lower bound on cached Actor metadata.
	ActorVersionHeader = "X-Ate-Actor-Version"
)

// TODO(liorlieberman): support/use CONNECT on Ingress as well.
// ClientConfig configures an egress CONNECT client.
type ClientConfig struct {
	GatewayAddress       string
	ServerName           string
	CredentialBundlePath string
	TrustBundlePath      string
}

// DialFunc dials a network address. It matches net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ClientOption customizes a Client beyond its configuration. Production
// callers need none of these.
type ClientOption func(*Client)

// WithDialer overrides how the client reaches the egress gateway. It exists so
// tests can substitute a transport; by default a net.Dialer is used.
func WithDialer(dial DialFunc) ClientOption {
	return func(c *Client) {
		if dial != nil {
			c.dialContext = dial
		}
	}
}

// EgressMetadata is attached to an egress CONNECT request. BearerToken is
// optional until actor JWT issuance is wired into ateom.
type EgressMetadata struct {
	Atespace     string
	ActorName    string
	ActorVersion int64
	BearerToken  string
}

// Client opens actor egress streams through an mTLS-authenticated gateway.
type Client struct {
	gatewayAddress string
	tlsConfig      *tls.Config
	dialContext    DialFunc
}

// Client implements EgressDialer.
var _ EgressDialer = (*Client)(nil)

// NewClient creates an egress CONNECT client and validates its TLS material.
func NewClient(cfg ClientConfig, opts ...ClientOption) (*Client, error) {
	if _, _, err := net.SplitHostPort(cfg.GatewayAddress); err != nil {
		return nil, fmt.Errorf("atunnel: invalid egress gateway address %q: %w", cfg.GatewayAddress, err)
	}
	if cfg.ServerName == "" {
		return nil, fmt.Errorf("atunnel: egress gateway server name is required")
	}
	if cfg.CredentialBundlePath == "" {
		return nil, fmt.Errorf("atunnel: credential bundle path is required")
	}
	if cfg.TrustBundlePath == "" {
		return nil, fmt.Errorf("atunnel: trust bundle path is required")
	}
	if _, err := loadCredentialBundle(cfg.CredentialBundlePath); err != nil {
		return nil, err
	}
	trustPEM, err := os.ReadFile(cfg.TrustBundlePath)
	if err != nil {
		return nil, fmt.Errorf("atunnel: reading trust bundle: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(trustPEM) {
		return nil, fmt.Errorf("atunnel: trust bundle %q contains no certificates", cfg.TrustBundlePath)
	}

	credentialBundlePath := cfg.CredentialBundlePath
	client := &Client{
		gatewayAddress: cfg.GatewayAddress,
		dialContext:    (&net.Dialer{}).DialContext,
		tlsConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
			ServerName: cfg.ServerName,
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return loadCredentialBundle(credentialBundlePath)
			},
		},
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

// DialContext opens a CONNECT tunnel to destination. destination becomes the
// request authority, so it must include an explicit port.
func (c *Client) DialContext(ctx context.Context, destination string, metadata EgressMetadata) (net.Conn, error) {
	if err := validateDestination(destination); err != nil {
		return nil, err
	}
	if !resources.IsValidResourceName(metadata.Atespace) || !resources.IsValidResourceName(metadata.ActorName) {
		return nil, fmt.Errorf("atunnel: invalid actor identity %q/%q", metadata.Atespace, metadata.ActorName)
	}
	if metadata.ActorVersion < 1 {
		return nil, fmt.Errorf("atunnel: actor version must be positive")
	}

	rawConn, err := c.dialContext(ctx, "tcp", c.gatewayAddress)
	if err != nil {
		return nil, fmt.Errorf("atunnel: connecting to egress gateway: %w", err)
	}
	tlsConn := tls.Client(rawConn, c.tlsConfig.Clone())
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = rawConn.Close()
		return nil, fmt.Errorf("atunnel: egress gateway TLS handshake: %w", err)
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: destination},
		Host:   destination,
		Header: http.Header{
			ActorAtespaceHeader: []string{metadata.Atespace},
			ActorNameHeader:     []string{metadata.ActorName},
			ActorVersionHeader:  []string{strconv.FormatInt(metadata.ActorVersion, 10)},
		},
	}
	if metadata.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+metadata.BearerToken)
	}
	if err := req.Write(tlsConn); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("atunnel: writing CONNECT request: %w", err)
	}

	reader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("atunnel: reading CONNECT response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		_ = tlsConn.Close()
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(resp.StatusCode)
		}
		return nil, fmt.Errorf("atunnel: egress gateway rejected CONNECT with %s: %s", resp.Status, message)
	}

	return &bufferedConn{Conn: tlsConn, reader: reader}, nil
}

func validateDestination(destination string) error {
	host, port, err := net.SplitHostPort(destination)
	if err != nil {
		return fmt.Errorf("atunnel: invalid egress destination %q: %w", destination, err)
	}
	if host == "" {
		return fmt.Errorf("atunnel: invalid egress destination %q: host is empty", destination)
	}
	if net.ParseIP(host) == nil {
		return fmt.Errorf("atunnel: invalid egress destination %q: host must be an IP address", destination)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("atunnel: invalid egress destination %q: port must be between 1 and 65535", destination)
	}
	return nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *bufferedConn) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return nil
}

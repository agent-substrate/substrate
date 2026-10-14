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

package csi

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the gRPC connection and the individual service clients for a CSI driver.
type Client struct {
	conn       *grpc.ClientConn
	identity   csi.IdentityClient
	controller csi.ControllerClient
	node       csi.NodeClient
}

// validateHostPort checks that hostport is "host:port" with a non-empty host
// and a port in [1, 65535].
func validateHostPort(hostport string) error {
	host, portStr, err := net.SplitHostPort(hostport)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("missing host")
	}
	if port, err := strconv.Atoi(portStr); err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid port %q (must be 1-65535)", portStr)
	}
	return nil
}

func parseEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse endpoint: %w", err)
	}
	switch u.Scheme {
	case "unix":
		if !strings.HasPrefix(endpoint, "unix:///") || strings.HasPrefix(endpoint, "unix:////") || u.Path == "/" {
			return "", "", fmt.Errorf("unix endpoint must be an absolute socket path (unix:///path/to/csi.sock): %s", endpoint)
		}
		return "unix", endpoint, nil
	case "tcp":
		if err := validateHostPort(u.Host); err != nil {
			return "", "", fmt.Errorf("invalid tcp endpoint %q: %w", endpoint, err)
		}
		return "tcp", u.Host, nil
	case "dns":
		// Custom DNS authorities (dns://server/...) are intentionally unsupported.
		if !strings.HasPrefix(endpoint, "dns:///") {
			return "", "", fmt.Errorf("dns endpoint must not specify an authority (use dns:///host:port): %s", endpoint)
		}
		if err := validateHostPort(strings.TrimPrefix(u.Path, "/")); err != nil {
			return "", "", fmt.Errorf("invalid dns endpoint %q: %w", endpoint, err)
		}
		return "dns", endpoint, nil
	default:
		return "", "", fmt.Errorf("unsupported scheme %q, must be unix, tcp or dns", u.Scheme)
	}
}

// NewCSIClient establishes a gRPC connection to the CSI driver over UDS or TCP
// and returns a client initialized with Identity, Controller, and Node service clients.
func NewCSIClient(endpoint string, tlsCfg *tls.Config) (*Client, error) {
	_, target, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("failed to dial CSI endpoint %q: %w", target, err)
	}

	return &Client{
		conn:       conn,
		identity:   csi.NewIdentityClient(conn),
		controller: csi.NewControllerClient(conn),
		node:       csi.NewNodeClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection to the CSI driver.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

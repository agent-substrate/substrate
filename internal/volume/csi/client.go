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
	"fmt"
	"log/slog"
	"net/url"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the gRPC connection and the individual service clients for a CSI driver.
type Client struct {
	conn       *grpc.ClientConn
	identity   csi.IdentityClient
	controller csi.ControllerClient
	node       csi.NodeClient
}

func parseEndpoint(endpoint string) (string, string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse endpoint: %w", err)
	}
	switch u.Scheme {
	case "unix":
		if u.Path == "" {
			return "", "", fmt.Errorf("unix endpoint missing path: %s", endpoint)
		}
		return "unix", endpoint, nil
	case "tcp":
		if u.Host == "" {
			return "", "", fmt.Errorf("tcp endpoint missing host:port: %s", endpoint)
		}
		return "tcp", u.Host, nil
	case "dns":
		return "dns", endpoint, nil
	default:
		return "", "", fmt.Errorf("unsupported scheme %q, must be unix, tcp or dns", u.Scheme)
	}
}

// NewCSIClient establishes a gRPC connection to the CSI driver over UDS or TCP
// and returns a client initialized with Identity, Controller, and Node service clients.
func NewCSIClient(endpoint string) (*Client, error) {
	scheme, target, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	if scheme == "tcp" {
		slog.Warn("CSI connection is unencrypted over TCP! (TODO: Implement TLS)", slog.String("endpoint", target))
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(target, dialOpts...)
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

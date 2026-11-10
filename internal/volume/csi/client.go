// Copyright 2026 The Agent Substrate Authors
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
	"net/url"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"go.ate.dev/substrate/internal/grpclb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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

// controllerConnectParams caps the reconnect back-off to a tcp or dns
// endpoint. round_robin reconnects as soon as a connection drops, and gRPC's
// default back-off grows to two minutes, so calls could fail long after the
// controller is back. MinConnectTimeout keeps gRPC's default.
var controllerConnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  backoff.DefaultConfig.BaseDelay,
		Multiplier: backoff.DefaultConfig.Multiplier,
		Jitter:     backoff.DefaultConfig.Jitter,
		MaxDelay:   5 * time.Second,
	},
	MinConnectTimeout: 20 * time.Second,
}

// dialOptions returns the options to dial an endpoint with the given scheme.
//
// tcp and dns endpoints use round_robin, so RPCs spread over every address
// the endpoint resolves to, such as each ready pod behind a headless Service.
// The dns resolver looks up addresses again only after a connection fails or
// drops, and ate-api-server keeps its client for the life of the process. So
// RPCs spread only over the pods resolved when the client connected, and a
// pod added later gets no RPCs until a connection breaks.
//
// A unix socket has one address, so it keeps gRPC's default, pick_first,
// which goes idle when the connection drops and reconnects on the next RPC.
func dialOptions(scheme string, creds credentials.TransportCredentials) []grpc.DialOption {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	switch scheme {
	case "tcp", "dns":
		opts = append(opts,
			grpc.WithDefaultServiceConfig(grpclb.RoundRobinServiceConfig),
			grpc.WithConnectParams(controllerConnectParams))
	}
	return opts
}

// NewCSIClient establishes a gRPC connection to the CSI driver over UDS or TCP
// and returns a client initialized with Identity, Controller, and Node service clients.
func NewCSIClient(endpoint string, tlsCfg *tls.Config) (*Client, error) {
	scheme, target, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(target, dialOptions(scheme, creds)...)
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

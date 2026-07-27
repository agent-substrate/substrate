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

// NewCSIClient establishes a gRPC connection to the CSI driver over a Unix Domain Socket (UDS)
// and returns a client initialized with Identity, Controller, and Node service clients.
func NewCSIClient(socketPath string) (*Client, error) {
	// Setup gRPC connection dial options for Unix domain sockets
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(socketPath, dialOpts...)
	if err != nil {
		return nil, err
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

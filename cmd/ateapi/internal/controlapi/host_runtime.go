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

package controlapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// HostRuntime routes lifecycle operations to an external Actor host provider.
// endpoint is copied into the assignment when the Worker is claimed, keeping
// retries independent of a second Worker lookup.
type HostRuntime interface {
	Activate(context.Context, string, *hostruntimepb.ActivateRequest) (*hostruntimepb.ActivateResponse, error)
	Terminate(context.Context, string, *hostruntimepb.TerminateRequest) error
}

// GRPCHostRuntime dispatches lifecycle calls to the endpoint stored on each
// external Worker. Connections are authenticated with one control-plane client
// certificate and cached by endpoint.
type GRPCHostRuntime struct {
	credentials credentials.TransportCredentials
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
}

// NewGRPCHostRuntime builds an mTLS HostRuntime client. The server certificate
// must match the hostname in the Worker's runtime endpoint.
func NewGRPCHostRuntime(clientCertPath, clientKeyPath, serverCAPath string) (*GRPCHostRuntime, error) {
	cert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("loading HostRuntime client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(serverCAPath)
	if err != nil {
		return nil, fmt.Errorf("reading HostRuntime server CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("HostRuntime server CA %q contains no certificates", serverCAPath)
	}
	return &GRPCHostRuntime{
		credentials: credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{cert},
			RootCAs:      roots,
		}),
		connections: make(map[string]*grpc.ClientConn),
	}, nil
}

func (r *GRPCHostRuntime) client(endpoint string) (hostruntimepb.HostRuntimeClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn := r.connections[endpoint]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient(endpoint,
			grpc.WithTransportCredentials(r.credentials),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		if err != nil {
			return nil, fmt.Errorf("creating HostRuntime connection to %q: %w", endpoint, err)
		}
		r.connections[endpoint] = conn
	}
	return hostruntimepb.NewHostRuntimeClient(conn), nil
}

func (r *GRPCHostRuntime) Activate(ctx context.Context, endpoint string, req *hostruntimepb.ActivateRequest) (*hostruntimepb.ActivateResponse, error) {
	client, err := r.client(endpoint)
	if err != nil {
		return nil, err
	}
	return client.Activate(ctx, req)
}

func (r *GRPCHostRuntime) Terminate(ctx context.Context, endpoint string, req *hostruntimepb.TerminateRequest) error {
	client, err := r.client(endpoint)
	if err != nil {
		return err
	}
	_, err = client.Terminate(ctx, req)
	return err
}

// Close releases all cached HostRuntime connections.
func (r *GRPCHostRuntime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result error
	for endpoint, conn := range r.connections {
		if err := conn.Close(); err != nil {
			result = fmt.Errorf("closing HostRuntime connection to %q: %w", endpoint, err)
		}
		delete(r.connections, endpoint)
	}
	return result
}

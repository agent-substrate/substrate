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
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		wantSrc  string
		wantTgt  string
		wantErr  bool
	}{
		{
			name:     "valid unix absolute",
			endpoint: "unix:///run/csi.sock",
			wantSrc:  "unix",
			wantTgt:  "unix:///run/csi.sock",
			wantErr:  false,
		},
		{
			name:     "valid tcp loopback",
			endpoint: "tcp://127.0.0.1:50051",
			wantSrc:  "tcp",
			wantTgt:  "127.0.0.1:50051",
			wantErr:  false,
		},
		{
			name:     "valid tcp hostname",
			endpoint: "tcp://localhost:50051",
			wantSrc:  "tcp",
			wantTgt:  "localhost:50051",
			wantErr:  false,
		},
		{
			name:     "valid dns",
			endpoint: "dns:///csi-service:9000",
			wantSrc:  "dns",
			wantTgt:  "dns:///csi-service:9000",
			wantErr:  false,
		},
		{
			name:     "invalid scheme",
			endpoint: "http://localhost:50051",
			wantErr:  true,
		},
		{
			name:     "no scheme",
			endpoint: "/run/csi.sock",
			wantErr:  true,
		},
		{
			name:     "tcp missing port",
			endpoint: "tcp://127.0.0.1",
			wantSrc:  "tcp",
			wantTgt:  "127.0.0.1",
			wantErr:  false,
		},
		{
			name:     "tcp missing host",
			endpoint: "tcp://:50051",
			wantSrc:  "tcp",
			wantTgt:  ":50051",
			wantErr:  false,
		},
		{
			name:     "unix missing path",
			endpoint: "unix://",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, tgt, err := parseEndpoint(tt.endpoint)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseEndpoint() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if src != tt.wantSrc {
					t.Errorf("parseEndpoint() src = %q, want %q", src, tt.wantSrc)
				}
				if tgt != tt.wantTgt {
					t.Errorf("parseEndpoint() tgt = %q, want %q", tgt, tt.wantTgt)
				}
			}
		})
	}
}

type mockIdentityServer struct {
	csi.UnimplementedIdentityServer
}

func (s *mockIdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          "mock-driver",
		VendorVersion: "v1.0.0",
	}, nil
}

func TestNewCSIClient_TCP(t *testing.T) {
	// Start a mock gRPC server on a local TCP port
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	mockIdentity := &mockIdentityServer{}
	csi.RegisterIdentityServer(grpcServer, mockIdentity)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// Connect to the mock server using NewCSIClient
	endpoint := "tcp://" + lis.Addr().String()
	client, err := NewCSIClient(endpoint, nil)
	if err != nil {
		t.Fatalf("failed to create CSI client: %v", err)
	}
	defer client.Close()

	// Verify we can call GetPluginInfo
	resp, err := client.identity.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	if err != nil {
		t.Fatalf("GetPluginInfo failed: %v", err)
	}

	if resp.GetName() != "mock-driver" {
		t.Errorf("expected driver name 'mock-driver', got %q", resp.GetName())
	}
}

// countingIdentityServer counts its GetPluginInfo calls.
type countingIdentityServer struct {
	csi.UnimplementedIdentityServer
	calls atomic.Int64
}

func (s *countingIdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	s.calls.Add(1)
	return &csi.GetPluginInfoResponse{Name: "mock-driver"}, nil
}

// startCountingServer serves a countingIdentityServer on a local TCP port.
func startCountingServer(t *testing.T) (*countingIdentityServer, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	srv := &countingIdentityServer{}
	grpcServer := grpc.NewServer()
	csi.RegisterIdentityServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)
	return srv, lis.Addr().String()
}

// An endpoint that resolves to two addresses spreads calls over both for tcp
// and dns, and sends every call to one address for unix.
func TestDialOptionsLoadBalancing(t *testing.T) {
	tests := []struct {
		scheme     string
		wantSpread bool
	}{
		{scheme: "tcp", wantSpread: true},
		{scheme: "dns", wantSpread: true},
		{scheme: "unix", wantSpread: false},
	}
	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			srv1, addr1 := startCountingServer(t)
			srv2, addr2 := startCountingServer(t)

			r := manual.NewBuilderWithScheme("csitest")
			r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: addr1}, {Addr: addr2}}})
			opts := append(dialOptions(tt.scheme, insecure.NewCredentials()), grpc.WithResolvers(r))
			conn, err := grpc.NewClient(r.Scheme()+":///csi", opts...)
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}
			defer conn.Close()
			client := csi.NewIdentityClient(conn)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			call := func() {
				t.Helper()
				if _, err := client.GetPluginInfo(ctx, &csi.GetPluginInfoRequest{}, grpc.WaitForReady(true)); err != nil {
					t.Fatalf("GetPluginInfo failed: %v", err)
				}
			}

			if tt.wantSpread {
				// round_robin adds each address once it connects, so call
				// until both have served one.
				deadline := time.Now().Add(5 * time.Second)
				for srv1.calls.Load() == 0 || srv2.calls.Load() == 0 {
					if time.Now().After(deadline) {
						t.Fatalf("calls per server = [%d %d], want both above 0", srv1.calls.Load(), srv2.calls.Load())
					}
					call()
				}
				return
			}
			const n = 20
			for range n {
				call()
			}
			if got1, got2 := srv1.calls.Load(), srv2.calls.Load(); got1 != n && got2 != n {
				t.Errorf("calls per server = [%d %d], want all %d on one server", got1, got2, n)
			}
		})
	}
}

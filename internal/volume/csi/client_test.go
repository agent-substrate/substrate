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
	"context"
	"net"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
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
	client, err := NewCSIClient(endpoint)
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

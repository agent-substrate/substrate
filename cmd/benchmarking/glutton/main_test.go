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

package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agent-substrate/substrate/internal/proto/glutton"
)

// TestSplitGRPCServesReadyzAndGRPCOnOneListener starts the grpc-mode handler
// on a real listener and exercises both protocols against it: the readyz
// probe is a plain HTTP GET, and it must not stop gRPC from being served.
func TestSplitGRPCServesReadyzAndGRPCOnOneListener(t *testing.T) {
	svc, err := newGluttonService(t.TempDir())
	if err != nil {
		t.Fatalf("newGluttonService: %v", err)
	}
	defer svc.Close()

	grpcSrv := grpc.NewServer()
	glutton.RegisterGluttonServer(grpcSrv, svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := newServer(splitGRPC(grpcSrv, mux))
	go srv.Serve(lis)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := http.Get("http://" + lis.Addr().String() + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /readyz = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	pong, err := glutton.NewGluttonClient(conn).Ping(ctx, &glutton.PingRequest{Message: "hi"})
	if err != nil {
		t.Fatalf("Ping over gRPC: %v", err)
	}
	if pong.GetMessage() != "hi" {
		t.Errorf("Ping = %q, want %q", pong.GetMessage(), "hi")
	}
}

// TestSplitGRPCRoutesOnContentType pins the routing rule itself: an HTTP/2
// request is not enough to reach the gRPC server, the content type is what
// decides.
func TestSplitGRPCRoutesOnContentType(t *testing.T) {
	grpcHit := false
	handler := splitGRPC(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { grpcHit = true }),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) }),
	)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := newServer(handler)
	go srv.Serve(lis)
	defer srv.Close()

	resp, err := http.Get("http://" + lis.Addr().String() + "/anything")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("HTTP/1.1 GET = %d, want %d (the non-gRPC handler)", resp.StatusCode, http.StatusTeapot)
	}
	if grpcHit {
		t.Error("HTTP/1.1 GET reached the gRPC handler")
	}
}

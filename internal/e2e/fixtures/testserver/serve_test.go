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
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestServeAll covers the shared origin answering two protocols at once, each
// on its own port, which is the whole reason the networking suite can deploy
// one pod instead of one per protocol.
func TestServeAll(t *testing.T) {
	httpServer := &http.Server{Handler: newServeHandler()}
	grpcServer := newServer()
	t.Cleanup(func() {
		_ = httpServer.Close()
		grpcServer.Stop()
	})

	// Each serve reports the address it was handed, which is how the test
	// learns the port the kernel picked for it.
	httpAddr, grpcAddr := make(chan string, 1), make(chan string, 1)
	stopped := make(chan error, 1)
	go func() {
		stopped <- serveAll([]protocolListener{{
			name:    "http",
			address: "127.0.0.1:0",
			serve: func(listener net.Listener) error {
				httpAddr <- listener.Addr().String()
				return httpServer.Serve(listener)
			},
		}, {
			name:    "grpc",
			address: "127.0.0.1:0",
			serve: func(listener net.Listener) error {
				grpcAddr <- listener.Addr().String()
				return grpcServer.Serve(listener)
			},
		}})
	}()

	httpAt, grpcAt := <-httpAddr, <-grpcAddr
	select {
	case err := <-stopped:
		t.Fatalf("serveAll returned before anything was dialed: %v", err)
	default:
	}

	// The readiness kubelet probes. It is on the HTTP port alone, and stands
	// for both, which is what binding before serving buys.
	resp, err := http.Get("http://" + httpAt + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz on the http port: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz = %d, want 200", resp.StatusCode)
	}

	conn, err := grpc.NewClient(grpcAt, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the grpc port %s: %v", grpcAt, err)
	}
	defer conn.Close()
	ctx := testContext(t)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("health check on the grpc port: %v", err)
	}

	// Nothing but gRPC on the gRPC port: an HTTP request has to come back
	// unusable. A fixture that answered both would hide a tunnel that turned
	// one into the other, and hide it from the suite whose whole subject is
	// what the tunnel does to a protocol.
	if plain, err := http.Get("http://" + grpcAt + "/healthz"); err == nil {
		defer plain.Body.Close()
		t.Errorf("the grpc port answered an HTTP GET with %d; it must speak nothing but cleartext HTTP/2", plain.StatusCode)
	}
}

// TestServeAllBindFailure covers the failure that readiness cannot report. A
// port this process cannot have has to take the whole process down before
// anything is served, because the probe is on one port and a test that sees
// Ready dials all of them.
func TestServeAllBindFailure(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("taking a port to collide with: %v", err)
	}
	defer taken.Close()

	served := make(chan struct{}, 2)
	err = serveAll([]protocolListener{{
		name:    "http",
		address: "127.0.0.1:0",
		serve: func(net.Listener) error {
			served <- struct{}{}
			return nil
		},
	}, {
		name:    "grpc",
		address: taken.Addr().String(),
		serve: func(net.Listener) error {
			served <- struct{}{}
			return nil
		},
	}})

	if err == nil {
		t.Fatal("serveAll succeeded with a port that was already taken")
	}
	// Which port, in the only place anyone will look: a pod that exits before
	// it is ever ready leaves its logs and nothing else.
	if got := err.Error(); !strings.Contains(got, "grpc") || !strings.Contains(got, taken.Addr().String()) {
		t.Errorf("serveAll error = %q, want it to name the grpc listener and %s", got, taken.Addr())
	}

	// The listener that did bind must not have been served: a readiness 200
	// from it would claim the whole origin is up.
	select {
	case <-served:
		t.Error("a listener was served even though a later one could not be bound")
	case <-time.After(100 * time.Millisecond):
	}
}

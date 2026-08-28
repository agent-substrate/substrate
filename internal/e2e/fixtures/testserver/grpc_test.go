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
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
)

// The e2e suite reads this fixture's answers as evidence about the egress
// tunnel, so a wrong answer here would be read as a broken gateway. These tests
// pin the answers against a loopback listener, where no gateway is involved.

// dialLocal starts the fixture's server on loopback and returns a connection to
// it. A real listener rather than an in-memory pipe, so the registrations and
// the HTTP/2 framing are the ones the pod serves.
func dialLocal(t *testing.T) *grpc.ClientConn {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := newServer()
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", listener.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestEcho(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	for _, message := range []string{"hello", "", "unicode: é世界", "with spaces and\nnewline"} {
		response, err := client.Echo(testContext(t), &grpcechopb.EchoRequest{Message: message})
		if err != nil {
			t.Fatalf("Echo(%q): %v", message, err)
		}
		if response.GetMessage() != message {
			t.Errorf("Echo(%q) = %q, want the message back unchanged", message, response.GetMessage())
		}
	}
}

func TestEchoStream(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	const (
		message = "streamed"
		count   = 3
	)
	stream, err := client.EchoStream(testContext(t), &grpcechopb.EchoStreamRequest{Message: message, Count: count})
	if err != nil {
		t.Fatalf("EchoStream: %v", err)
	}

	var got []*grpcechopb.EchoResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("EchoStream Recv after %d responses: %v", len(got), err)
		}
		got = append(got, response)
	}

	if len(got) != count {
		t.Fatalf("EchoStream sent %d responses, want %d", len(got), count)
	}
	// Indexes are what tell a reordered or deduplicated stream from an intact
	// one, which is the only thing the e2e assertion can check about ordering.
	for i, response := range got {
		if response.GetMessage() != message {
			t.Errorf("response %d message = %q, want %q", i, response.GetMessage(), message)
		}
		if int(response.GetIndex()) != i {
			t.Errorf("response %d index = %d, want %d", i, response.GetIndex(), i)
		}
	}
}

// The bidi handler must answer each request as it arrives. Sending everything
// and only then reading would pass against a server that drains first, which is
// exactly the implementation this test exists to rule out -- so this sends one
// message at a time and blocks on its response before sending the next.
func TestEchoBidi(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	stream, err := client.EchoBidi(testContext(t))
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	const count = 3
	for i := range count {
		message := fmt.Sprintf("message-%d", i)
		if err := stream.Send(&grpcechopb.EchoRequest{Message: message}); err != nil {
			t.Fatalf("EchoBidi Send %d: %v", i, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("EchoBidi Recv %d: %v", i, err)
		}
		if response.GetMessage() != message {
			t.Errorf("response %d message = %q, want %q", i, response.GetMessage(), message)
		}
		if int(response.GetIndex()) != i {
			t.Errorf("response %d index = %d, want %d", i, response.GetIndex(), i)
		}
	}

	// Half-close the request direction and drain the response direction. The
	// server must end with OK here: a handler that treated the half-close as an
	// error would still have echoed everything above.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("EchoBidi CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("EchoBidi Recv after CloseSend = %v, want io.EOF", err)
	}
}

// A non-positive count must fail loudly. Returning an empty stream instead
// would make a caller that forgot to set count look like a working one.
func TestEchoStreamRejectsBadCount(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	for _, count := range []int32{0, -1, maxStreamCount + 1} {
		stream, err := client.EchoStream(testContext(t), &grpcechopb.EchoStreamRequest{Message: "x", Count: count})
		if err == nil {
			_, err = stream.Recv()
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("EchoStream(count=%d) status = %s (%v), want InvalidArgument", count, got, err)
		}
	}
}

// The pod's readinessProbe is a grpc probe, which checks the health service's
// empty service name. Nothing else in the fixture would notice if that
// registration disappeared, and the pod would simply never become ready.
func TestHealthServiceIsServing(t *testing.T) {
	response, err := healthpb.NewHealthClient(dialLocal(t)).Check(testContext(t), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if response.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("health Check status = %s, want SERVING", response.GetStatus())
	}
}

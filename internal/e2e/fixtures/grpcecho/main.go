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

// Command grpcecho is the gRPC origin the egress e2e suites dial through the
// egress gateway. It serves cleartext HTTP/2 -- no TLS anywhere -- because the
// leg under test is the tunnel, not the origin's identity, and because the
// gateway relays a terminated CONNECT as opaque TCP: whatever the actor speaks
// is what arrives here.
//
// It answers the grpc health service as well as Echo, which is what the pod's
// readinessProbe checks. A separate HTTP port for readiness would need an h2c
// handler multiplexed onto this listener, and the whole point of this fixture
// is that nothing between the actor and here parses HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
)

var listenAddress = flag.String("listen", ":50051", "Address the gRPC server listens on, cleartext HTTP/2.")

// maxStreamCount bounds EchoStream. A test asks for a handful of messages; a
// request for millions is a bug in the caller, and answering it would look like
// a hung tunnel rather than the mistake it is.
const maxStreamCount = 1000

type echoServer struct {
	grpcechopb.UnimplementedEchoServer
}

func (echoServer) Echo(_ context.Context, req *grpcechopb.EchoRequest) (*grpcechopb.EchoResponse, error) {
	return &grpcechopb.EchoResponse{Message: req.GetMessage()}, nil
}

func (echoServer) EchoStream(req *grpcechopb.EchoStreamRequest, stream grpc.ServerStreamingServer[grpcechopb.EchoResponse]) error {
	count := req.GetCount()
	if count <= 0 {
		return status.Errorf(codes.InvalidArgument, "count must be positive, got %d", count)
	}
	if count > maxStreamCount {
		return status.Errorf(codes.InvalidArgument, "count must be at most %d, got %d", maxStreamCount, count)
	}
	for i := range count {
		if err := stream.Send(&grpcechopb.EchoResponse{Message: req.GetMessage(), Index: i}); err != nil {
			return err
		}
	}
	return nil
}

// EchoBidi answers each request as it arrives rather than draining the request
// direction first. Batching the responses would deadlock a caller that waits
// for each one before sending the next, and would also stop this from testing
// anything a server-stream does not already cover: the point is frames moving
// both ways at once.
func (echoServer) EchoBidi(stream grpc.BidiStreamingServer[grpcechopb.EchoRequest, grpcechopb.EchoResponse]) error {
	for index := int32(0); ; index++ {
		req, err := stream.Recv()
		// io.EOF is the client's half-close: the request direction is done,
		// this direction still has to end cleanly with an OK status.
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&grpcechopb.EchoResponse{Message: req.GetMessage(), Index: index}); err != nil {
			return err
		}
	}
}

// newServer returns a server with everything registered on it, so the unit
// tests exercise the same registrations the pod serves.
func newServer() *grpc.Server {
	server := grpc.NewServer()
	grpcechopb.RegisterEchoServer(server, echoServer{})
	healthpb.RegisterHealthServer(server, health.NewServer())
	return server
}

func main() {
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("grpcecho: listening on %s: %v", *listenAddress, err)
	}
	log.Printf("grpcecho: serving on %s", listener.Addr())
	log.Fatal(newServer().Serve(listener))
}

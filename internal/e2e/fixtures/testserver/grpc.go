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
	"log"
	"net"
	"net/http"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/grpcechopb"
)

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

// newHealthHandler answers the HTTP readiness probe. Deliberately trivial: it
// reports that the process is up, and the gRPC listener is opened before this
// one, so a 200 here means the RPC port is already accepting.
func newHealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	return mux
}

// newGRPCCmd is the gRPC server both networking e2e suites talk to. One
// subcommand serves both directions so the two legs cannot drift apart in what
// they consider a working RPC:
//
//   - egress: deployed as a plain pod, it is the origin an Actor dials through
//     the egress gateway.
//   - ingress: deployed as an Actor, it is the origin a client reaches through
//     atenet-router.
//
// It serves cleartext HTTP/2 -- no TLS anywhere -- because the leg under test
// is the tunnel, not the origin's identity, and because the egress gateway
// relays a terminated CONNECT as opaque TCP: whatever the actor speaks is what
// arrives here.
//
// It answers the grpc health service as well as Echo, which is what the egress
// pod's readinessProbe checks. Multiplexing readiness onto that listener as an
// h2c handler would defeat the point of the egress fixture, where nothing
// between the actor and here parses HTTP -- so --health-listen puts it on a
// second port instead, off unless asked for. The ingress Actor needs it because
// an ActorTemplate's readyz is an HTTP GET and nothing else: a gRPC server
// answers one with a protocol error, so without it the Actor never boots.
func newGRPCCmd() *cobra.Command {
	var listenAddress string
	var healthAddress string
	cmd := &cobra.Command{
		Use:   "grpc",
		Short: "Serve a cleartext HTTP/2 gRPC echo origin.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			listener, err := net.Listen("tcp", listenAddress)
			if err != nil {
				return fmt.Errorf("listening on %s: %w", listenAddress, err)
			}
			log.Printf("testserver grpc: serving on %s", listener.Addr())

			// Bound before Serve below, so a readiness 200 can never precede
			// the gRPC port being open.
			if healthAddress != "" {
				healthListener, err := net.Listen("tcp", healthAddress)
				if err != nil {
					return fmt.Errorf("listening for health on %s: %w", healthAddress, err)
				}
				log.Printf("testserver grpc: serving /readyz on %s", healthListener.Addr())
				go func() {
					log.Fatal(http.Serve(healthListener, newHealthHandler()))
				}()
			}

			return newServer().Serve(listener)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":50051", "Address the gRPC server listens on, cleartext HTTP/2.")
	cmd.Flags().StringVar(&healthAddress, "health-listen", "", "Address for an HTTP/1.1 /readyz listener. Empty serves no HTTP at all, which is what the egress fixture wants; the ingress Actor sets it because an ActorTemplate readyz is an HTTP GET.")
	return cmd
}

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

package ingress

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	networkextprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/network_ext_proc/v3"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Envoy's network ext_proc filter only ingests DynamicMetadata fields whose
// top-level key is allowlisted via MetadataOptions.ReceivingNamespaces (see
// xds.go's buildTcpConnectFilterChain), merging that field's own (nested)
// value into the connection's dynamic metadata under the same key; the
// tcp_proxy leg's cluster needs an OriginalDstLbConfig.MetadataKey pointed at
// OriginalDstMetadataKey/OriginalDstAddressKey to pick it up. Requires Envoy
// >= envoyproxy/envoy@b27925c960 (first released in 1.39) for the
// ReceivingNamespaces field to exist at all.

// NetworkExtProcServer implements Envoy's network (L4) external processing
// gRPC server for CONNECT-tunneled TCP traffic reinjected through
// main_internal (see xds.go's buildMainInternalListener/buildTcpConnectFilterChain).
// Unlike Handler, it never sees an HTTP Host header to resolve the actor
// from -- the connect_terminate listener already captured the tunnel's
// authority into dynamic metadata when the CONNECT was established, and that
// is all this server needs: there is nothing to inspect on the wire, and the
// routing decision is made once, from that one piece of metadata, at the
// start of the connection.
type NetworkExtProcServer struct {
	resumer *ActorResumer
}

// NewNetworkExtProcServer constructs a NetworkExtProcServer. Unlike Handler it
// does not park: parking exists to retry/hold a request while an actor's
// worker pool is briefly saturated, and how that interacts with a long-lived
// TCP connection (whose ext_proc round trip guards a full data frame's worth
// of end-user latency rather than a single header exchange) is not yet
// understood -- resume failures here fail fast instead.
func NewNetworkExtProcServer(apiClient ateapipb.ControlClient) *NetworkExtProcServer {
	return &NetworkExtProcServer{
		resumer: NewActorResumer(apiClient),
	}
}

// Register adds the NetworkExternalProcessor service to grpcServer. It shares
// the same server (and so the same listener, port, and graceful-drain
// sequence) as the HTTP ext_proc service registered by extproc.Server --
// gRPC dispatches by the fully-qualified service name in the request path,
// not by port, so two unrelated services on one *grpc.Server is the ordinary
// case, not a special one. Both are the same process either way, so there is
// no isolation to lose, and the CONNECT-tunneled TCP connections this server
// handles now drain on the same timed GracefulStop as parked HTTP requests
// instead of being cut off wherever shutdown happens to be when the wider
// work context is cancelled.
func (s *NetworkExtProcServer) Register(grpcServer *grpc.Server) {
	networkextprocv3.RegisterNetworkExternalProcessorServer(grpcServer, s)
}

// Process implements the bidirectional NetworkExternalProcessor stream. Envoy
// opens one stream per TCP connection and sends one ProcessingRequest per data
// frame in whichever direction(s) ProcessingMode enables (see
// xds.go's buildTcpConnectFilterChain); only the first request -- which
// carries the connection's forwarded metadata -- is needed to decide whether,
// and where, to route the connection. Every later frame passes through
// unmodified.
func (s *NetworkExtProcServer) Process(stream networkextprocv3.NetworkExternalProcessor_ProcessServer) error {
	var routed bool
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if routed {
			if err := stream.Send(passthroughResponse()); err != nil {
				return err
			}
			continue
		}

		resp, err := s.handleFirstFrame(stream.Context(), req)
		if err != nil {
			slog.ErrorContext(stream.Context(), "Error during network ext_proc processing", slog.String("err", err.Error()))
			// The connection's fate is decided; there is nothing useful left
			// to do with this stream.
			return stream.Send(closeResponse())
		}
		routed = true
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// handleFirstFrame resumes the actor identified by the connection's forwarded
// authority attribute and builds the CONTINUE response that routes the
// connection to that actor's worker.
//
// The authority comes from AuthorityFilterStateAttribute, evaluated by
// xds.go's buildTcpConnectFilterChain via ConnectionAttributes -- the
// network-filter counterpart of the HTTP leg's RequestAttributes, and only
// available since Envoy gained connection-attribute/filter-state support for
// NetworkExternalProcessor (envoyproxy/envoy#46551). Before that, this
// server had no way to read anything Envoy captured at connect_terminate:
// dynamic metadata does not survive the connect_terminate -> main_internal
// internal-listener hop, and there was no filter-state-reading mechanism to
// fall back on the way the HTTP leg's HandleRequestHeaders always could.
func (s *NetworkExtProcServer) handleFirstFrame(ctx context.Context, req *networkextprocv3.ProcessingRequest) (*networkextprocv3.ProcessingResponse, error) {
	authority := attributeFromRequest(req.GetAttributes(), AuthorityFilterStateAttribute)
	if authority == "" {
		return nil, fmt.Errorf("network ext_proc request missing %s attribute", AuthorityFilterStateAttribute)
	}

	actorRef, err := parseActorRef(authority)
	if err != nil {
		return nil, fmt.Errorf("invalid authority %q: %w", authority, err)
	}

	slog.InfoContext(ctx, "ResumeActor", slog.Any("actor", actorRef))
	actor, _, err := s.resumer.ResumeActor(ctx, actorRef)
	if err != nil {
		return nil, fmt.Errorf("resuming actor %s: %w", actorRef, err)
	}

	workerIP := actor.GetWorkerAssignment().GetWorkerPodIp()
	if net.ParseIP(workerIP) == nil {
		return nil, fmt.Errorf("actor %s routing failed: invalid worker IP %q", actorRef, workerIP)
	}
	// The actor is reached through the same in-worker atunnel ingress server as
	// the HTTP leg (see ingress.go). Unlike the HTTP leg, there is no header to
	// carry the CONNECT authority's arbitrary port through atunnel's reverse
	// proxy on this raw TCP leg -- untouched for now, pending its own fix.
	targetAddr := net.JoinHostPort(workerIP, "443")
	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	dynamicMetadata, err := structpb.NewStruct(map[string]any{
		OriginalDstMetadataKey: map[string]any{
			OriginalDstAddressKey: targetAddr,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("building dynamic metadata for actor %s: %w", actorRef, err)
	}

	return &networkextprocv3.ProcessingResponse{
		DataProcessingStatus: networkextprocv3.ProcessingResponse_UNMODIFIED,
		ConnectionStatus:     networkextprocv3.ProcessingResponse_CONTINUE,
		DynamicMetadata:      dynamicMetadata,
	}, nil
}

// attributeFromRequest scans every filter's connection_attributes for name,
// mirroring extproc.RequestMetadata.Attribute's handling of the HTTP leg's
// equivalent map: attrs is keyed by the filter that reported it (here,
// envoy.filters.network.ext_proc's own well-known name), which callers should
// not need to hardcode.
func attributeFromRequest(attrs map[string]*structpb.Struct, name string) string {
	for _, s := range attrs {
		if v, ok := s.GetFields()[name]; ok {
			return v.GetStringValue()
		}
	}
	return ""
}

// passthroughResponse continues a connection whose routing decision was
// already made by handleFirstFrame, leaving the data frame unmodified.
func passthroughResponse() *networkextprocv3.ProcessingResponse {
	return &networkextprocv3.ProcessingResponse{
		DataProcessingStatus: networkextprocv3.ProcessingResponse_UNMODIFIED,
		ConnectionStatus:     networkextprocv3.ProcessingResponse_CONTINUE,
	}
}

// closeResponse rejects the connection outright, e.g. because its authority
// metadata was missing or the actor failed to resume.
func closeResponse() *networkextprocv3.ProcessingResponse {
	return &networkextprocv3.ProcessingResponse{
		ConnectionStatus: networkextprocv3.ProcessingResponse_CLOSE_RST,
	}
}

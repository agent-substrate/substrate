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

// Package extproc implements the external processing (ext_proc) gRPC server
// that the atenet router serves to its dataplane gateways.
//
// This package is the multiplexer and nothing else: it terminates the
// ext_proc stream, works out which direction — ingress or egress — a request
// arrived on, dispatches to the Handler registered for that direction, and
// records the latency and outcome. The routing and authentication policy for
// each direction lives in the sibling ingress and egress packages, which apply
// opposite trust models and are deliberately kept apart.
package extproc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
)

// Handlers maps each direction this server serves to the handler for it. A
// direction absent from the map is refused at the mux — see Server.Process.
type Handlers map[Direction]Handler

// Server implements the external processing gRPC server, dispatching each
// request to the Handler for the direction it arrived on.
type Server struct {
	port          int
	handlers      Handlers
	recorder      *QueryRecorder
	routeDuration metric.Float64Histogram
}

// NewServer builds the ext_proc mux serving the given handlers. Passing a
// subset of the directions is how --mode restricts an instance to the traffic
// its deployment fronts.
func NewServer(port int, routeDuration metric.Float64Histogram, handlers Handlers) *Server {
	return &Server{
		port:          port,
		handlers:      handlers,
		recorder:      NewQueryRecorder(100),
		routeDuration: routeDuration,
	}
}

// Queries returns the most recently processed requests, newest first, for the
// /statusz page.
func (s *Server) Queries() []RecordedQuery {
	return s.recorder.Get()
}

// Recorder exposes the query ring buffer so tests and the status page can seed
// or read it.
func (s *Server) Recorder() *QueryRecorder { return s.recorder }

// NewGRPCServer builds the gRPC server with the ext_proc service registered.
// The caller owns its lifecycle: Run serves it, and the drain sequence in
// drain.go stops it — gracefully first so in-flight streams (parked requests
// above all) finish, forcefully past the drain timeout.
func (s *Server) NewGRPCServer() *grpc.Server {
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	extprocv3.RegisterExternalProcessorServer(grpcServer, s)
	return grpcServer
}

func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var resp *extprocv3.ProcessingResponse

		switch reqType := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			resp = s.processRequestHeaders(stream.Context(), req, reqType.RequestHeaders)

		default:
			// No modification for other processing states, but log because this should
			// not be called.
			slog.Error("Unexpected request type", slog.String("reqType", fmt.Sprintf("%T", reqType)))
			resp = &extprocv3.ProcessingResponse{
				Response: &extprocv3.ProcessingResponse_RequestHeaders{
					RequestHeaders: &extprocv3.HeadersResponse{
						Response: &extprocv3.CommonResponse{},
					},
				},
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// processRequestHeaders runs one RequestHeaders callback through the handler
// for its direction, and records the latency and outcome either way.
func (s *Server) processRequestHeaders(
	ctx context.Context,
	req *extprocv3.ProcessingRequest,
	reqHeaders *extprocv3.HttpHeaders,
) *extprocv3.ProcessingResponse {
	start := time.Now()
	md := NewRequestMetadata(reqHeaders.GetHeaders().GetHeaders(), req.GetAttributes())

	// One atenet binary serves both directions, as two ext_proc handlers
	// selected here. They are deployed separately today — atenet-router fronts
	// the ingress dataplane, atenet-egress the egress gateway — because the two
	// scale independently, and --mode restricts an instance to the direction
	// its deployment fronts. Nothing stops a single instance from serving both.
	//
	// Which handler runs is decided by the filter chain the dataplane says
	// accepted the request, never by anything in the request itself (see
	// directionOf).
	dir := directionOf(req)

	var res Result
	var err error
	if handler, ok := s.handlers[dir]; ok {
		res, err = handler.HandleRequestHeaders(ctx, md)
	} else {
		// The dataplane in front of this instance is sending traffic the
		// instance was not started to serve. Refuse it rather than falling back
		// to the other direction's handler: the two apply opposite trust
		// models, so a fallback would run a request through the wrong one.
		err = NewReqError(envoy_type.StatusCode_NotFound,
			"this router does not serve %s traffic", dir)
	}

	elapsed := time.Since(start)
	s.recordRouteDuration(ctx, elapsed, res.TemplateNamespace, res.TemplateName, classifyOutcome(err), res.resume())

	if err != nil {
		slog.ErrorContext(ctx, "Error during ext_proc RequestHeaders processing",
			slog.String("direction", string(dir)),
			slog.String("err", err.Error()))
		s.recorder.AddRouterRequest(start, elapsed, "Error", "-", md)

		var reqErr *ReqError
		if errors.As(err, &reqErr) {
			return ImmediateResponse(envoy_type.StatusCode(reqErr.StatusCode), reqErr.Error())
		}
		return ImmediateResponse(envoy_type.StatusCode_InternalServerError, err.Error())
	}

	s.recorder.AddRouterRequest(start, elapsed, "Route ok", res.Target, md)
	return &extprocv3.ProcessingResponse{
		Response:        &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: res.Response},
		DynamicMetadata: res.DynamicMetadata,
	}
}

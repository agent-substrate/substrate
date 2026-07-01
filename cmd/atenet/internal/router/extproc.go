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

package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc"
)

// ExtProcServer implements the Envoy external processing gRPC server.
// It is a thin adapter: it translates Envoy ExtProc protocol to/from the
// gateway-neutral RouteResolver and records metrics.
type ExtProcServer struct {
	port          int
	resolver      RouteResolver
	recorder      *QueryRecorder
	routeDuration metric.Float64Histogram
}

// NewExtProcServer creates an ExtProcServer backed by the given resolver.
func NewExtProcServer(port int, resolver RouteResolver, routeDuration metric.Float64Histogram) *ExtProcServer {
	return &ExtProcServer{
		port:          port,
		resolver:      resolver,
		recorder:      NewQueryRecorder(100),
		routeDuration: routeDuration,
	}
}

func (s *ExtProcServer) Serve(ctx context.Context, lis net.Listener) error {
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	extprocv3.RegisterExternalProcessorServer(grpcServer, s)

	errChan := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (s *ExtProcServer) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp := &extprocv3.ProcessingResponse{}

		switch reqType := req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			start := time.Now()
			hResponse, rqm, res, err := s.handleRequestHeaders(stream.Context(), reqType.RequestHeaders)
			elapsed := time.Since(start)
			if err != nil {
				slog.ErrorContext(stream.Context(), "Error during ext_proc RequestHeaders processing", slog.String("err", err.Error()))
				var reqErr *reqError
				if errors.As(err, &reqErr) {
					resp = immediateResponse(envoy_type.StatusCode(reqErr.statusCode), reqErr.Error())
				} else {
					resp = immediateResponse(envoy_type.StatusCode_InternalServerError, err.Error())
				}
				var tmplNs, tmplName string
				if res != nil && res.Denial != nil {
					// outcome already captured; labels stay empty for denials from parse failures
					_ = res
				}
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, classifyOutcome(err))
				s.recorder.AddRouterRequest(start, elapsed, "Error", "-", rqm)
			} else {
				resp.Response = &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: hResponse}
				tmplNs := res.Success.TemplateRef.Namespace
				tmplName := res.Success.TemplateRef.Name
				target := net.JoinHostPort(res.Success.Backend.IP, fmt.Sprintf("%d", res.Success.Backend.Port))
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, "ok")
				s.recorder.AddRouterRequest(start, elapsed, "Route ok", target, rqm)
			}

		default:
			// No modification for other processing states, but log because this should
			// not be called.
			slog.Error("Unexpected request type", slog.String("reqType", fmt.Sprintf("%T", reqType)))
			resp.Response = &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{
					Response: &extprocv3.CommonResponse{},
				},
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// handleRequestHeaders is the ExtProc adapter for a single request. It
// extracts trace context, builds a RouteRequest, calls the resolver, and
// maps the RouteResolution to an extproc HeadersResponse.
//
// On denial, it returns a non-nil *reqError so Process() can build an
// ImmediateResponse. The RouteResolution is returned alongside for label access.
func (s *ExtProcServer) handleRequestHeaders(
	ctx context.Context,
	reqHeaders *extprocv3.HttpHeaders,
) (*extprocv3.HeadersResponse, *requestMetadata, *RouteResolution, error) {
	metadata := newRequestMetadata(reqHeaders.Headers.GetHeaders())
	slog.InfoContext(ctx, "Request", slog.String("host", metadata.host))

	// Envoy doesn't propagate trace context into the ext_proc gRPC
	// stream's metadata — the per-request traceparent arrives in the
	// HTTP headers carried inside the ProcessingRequest payload. Extract
	// from there so our span links to the Envoy ingress span.
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(metadata.headers))
	ctx, span := otel.Tracer(routerServiceName).Start(ctx, "ExtProc.RequestHeaders")
	defer span.End()

	req := RouteRequest{
		Authority: metadata.host,
		Path:      metadata.path,
		Headers:   metadata.headers,
		Adapter:   "extproc",
	}

	res := s.resolver.ResolveRoute(ctx, req)
	if res.Denial != nil {
		d := res.Denial
		re := &reqError{
			msg:        d.Message,
			cause:      d.Cause,
			statusCode: d.HTTPStatus,
		}
		return nil, metadata, &res, re
	}

	success := res.Success
	targetAddr := net.JoinHostPort(success.Backend.IP, fmt.Sprintf("%d", success.Backend.Port))
	slog.InfoContext(ctx, "Route ok", slog.String("actorID", success.ActorID), slog.String("targetAddr", targetAddr))

	mutation := &extprocv3.HeaderMutation{}
	addAuthorityMutation(targetAddr, mutation)

	return &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{
			HeaderMutation: mutation,
		},
	}, metadata, &res, nil
}

func (s *ExtProcServer) recordRouteDuration(ctx context.Context, d time.Duration, tmplNs, tmplName, outcome string) {
	if s.routeDuration == nil {
		return
	}
	s.routeDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("actor_template_namespace", tmplNs),
		attribute.String("actor_template_name", tmplName),
		attribute.String("outcome", outcome),
	))
}

func classifyOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		var re *reqError
		if errors.As(err, &re) && re.statusCode == int(envoy_type.StatusCode_NotFound) {
			return "not_found"
		}
		return "error"
	}
}

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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ExtProcServer implements the Envoy external processing gRPC server
// to dynamically manage actor activations based on request traffic.
type ExtProcServer struct {
	port          int
	apiClient     ateapipb.ControlClient
	recorder      *QueryRecorder
	resumer       *ActorResumer
	routeDuration metric.Float64Histogram
}

func NewExtProcServer(port int, apiClient ateapipb.ControlClient, routeDuration metric.Float64Histogram) *ExtProcServer {
	return &ExtProcServer{
		port:          port,
		apiClient:     apiClient,
		recorder:      NewQueryRecorder(100),
		resumer:       NewActorResumer(apiClient),
		routeDuration: routeDuration,
	}
}

func (s *ExtProcServer) Serve(ctx context.Context, lis net.Listener) error {
	grpcServer := grpc.NewServer()
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
			hResponse, rqm, target, tmplNs, tmplName, err := s.handleRequestHeaders(stream.Context(), reqType.RequestHeaders)
			elapsed := time.Since(start)
			if err != nil {
				slog.ErrorContext(stream.Context(), "Error during ext_proc RequestHeaders processing", slog.String("err", err.Error()))
				var reqErr *reqError
				if errors.As(err, &reqErr) {
					resp = immediateResponse(envoy_type.StatusCode(reqErr.statusCode), reqErr.Error())
				} else {
					resp = immediateResponse(envoy_type.StatusCode_InternalServerError, err.Error())
				}
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, classifyOutcome(err))
				s.recorder.AddRouterRequest(start, elapsed, "Error", "-", rqm)
			} else {
				resp.Response = &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: hResponse}
				s.recordRouteDuration(stream.Context(), elapsed, tmplNs, tmplName, "ok")
				s.recorder.AddRouterRequest(start, elapsed, "Route ok", target, rqm)
			}

		default:
			// No modification for other processing states, but log because this should
			// not be called.
			slog.Error("Unexpected request type", slog.Any("reqType", reqType))
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

func (s *ExtProcServer) handleRequestHeaders(
	ctx context.Context,
	reqHeaders *extprocv3.HttpHeaders,
) (*extprocv3.HeadersResponse, *requestMetadata, string, string, string, error) {
	metadata := newRequestMetadata(reqHeaders.Headers.GetHeaders())
	slog.InfoContext(ctx, "Request", slog.String("metadata", metadata.String()))

	actorID, err := parseActorID(metadata.host)
	if err != nil {
		// Host is invalid, respond with 404.
		return nil, metadata, "", "", "", invalidHostErr(metadata.host, err)
	}

	slog.InfoContext(ctx, "ResumeActor", slog.String("actorID", actorID))
	actor, err := s.resumer.ResumeActor(ctx, actorID)

	slog.InfoContext(ctx, "ResumeActor result",
		slog.String("actor", fmt.Sprintf("%+v", actor)),
		slog.String("worker_ip", actor.GetAteomPodIp()),
		slog.Any("err", err))

	if err != nil {
		return nil, metadata, "", "", "", mapResumeError(actorID, err)
	}

	// Actor template identity, used as low-cardinality route-latency metric
	// attributes (see recordRouteDuration).
	tmplNs := actor.GetActorTemplateNamespace()
	tmplName := actor.GetActorTemplateName()

	workerIP := actor.GetAteomPodIp()
	if ip := net.ParseIP(workerIP); ip == nil {
		return nil, metadata, "", tmplNs, tmplName, newReqError(envoy_type.StatusCode_InternalServerError,
			"actor %q routing failed", actorID)
	}

	// TODO(bowei) -- handle more than port 80 on the actor.
	targetAddr := net.JoinHostPort(workerIP, "80")

	slog.InfoContext(ctx, "Route ok", slog.String("actorID", actorID), slog.String("targetAddr", targetAddr))

	// Route by rewriting the :authority header.
	mutation := &extprocv3.HeaderMutation{}
	addAuthorityMutation(targetAddr, mutation)

	return &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{
			HeaderMutation: mutation,
		},
	}, metadata, targetAddr, tmplNs, tmplName, nil
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

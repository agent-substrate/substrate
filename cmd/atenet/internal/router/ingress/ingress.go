// Copyright 2026 The Agent Substrate Authors
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

// Package ingress implements the ext_proc handler for traffic arriving at the
// ingress gateway: it resolves the actor a request is addressed to, resumes it
// through the control plane (parking the request while the worker pool is
// saturated), and points the dataplane at the worker that ends up hosting it.
//
// Everything reaching this handler is unauthenticated client input. The
// opposite trust model — an actor identity carried by a CA-signed client
// certificate — belongs to the sibling egress package, and the two are kept
// apart deliberately.
package ingress

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/structpb"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/atenet"
	atunnelingress "go.ate.dev/substrate/internal/atunnel/ingress"
)

// defaultActorPort is the actor's port when a request names no other one.
const defaultActorPort = 80

const (
	// OriginalDstMetadataKey is the dynamic-metadata namespace carrying the
	// resolved worker address and port. xds.go's ORIGINAL_DST cluster reads
	// it to pick the upstream.
	OriginalDstMetadataKey = "envoy.filters.listener.original_dst"
	// OriginalDstAddressKey is the resolved worker atunnel address (IP:443).
	OriginalDstAddressKey = "local"
	// OriginalDstPortKey is the actor's target port.
	OriginalDstPortKey = "port"
	// WorkerSPIFFEIDKey is the SPIFFE ID atunnel's certificate must carry.
	WorkerSPIFFEIDKey = "worker_spiffe_id"
	// ActorUIDMetadataKey is the UID of the actor the route was resolved for.
	// atunnel refuses a request whose UID is not the actor it hosts, so a
	// stale route cannot reach a later actor of the same name. It travels as
	// metadata, not as a request header, so a client cannot set it.
	ActorUIDMetadataKey = "actor_uid"

	// ResponseCodeDetailsAttribute is the Envoy attribute that says where a
	// response came from: "via_upstream" when the upstream sent it, anything
	// else when Envoy replied itself.
	ResponseCodeDetailsAttribute = "response.code_details"
)

// Handler routes ingress requests to the worker hosting their actor.
type Handler struct {
	resumer *ActorResumer
	parking *parkingLot

	upstreamConnect   bool
	warmRouteCacheTTL time.Duration
}

// Option configures a Handler.
type Option func(*Handler)

// WithUpstreamConnect says whether the dataplane reaches workers through a
// CONNECT tunnel (the default) or sends them requests directly.
func WithUpstreamConnect(connect bool) Option {
	return func(h *Handler) { h.upstreamConnect = connect }
}

// WithWarmRouteCacheTTL caches a running actor's route for ttl, so requests
// for it skip the ateapi ResumeActor call. The dataplane must send response
// headers to ext_proc for stale routes to be dropped. 0 leaves the cache off.
func WithWarmRouteCacheTTL(ttl time.Duration) Option {
	return func(h *Handler) { h.warmRouteCacheTTL = ttl }
}

func New(apiClient ateapi.ControlClient, parkCfg ParkedRequestConfig, parkMetrics *ParkingMetrics, opts ...Option) *Handler {
	lot := newParkingLot(parkCfg, parkMetrics)
	h := &Handler{parking: lot, upstreamConnect: true}
	for _, opt := range opts {
		opt(h)
	}
	h.resumer = NewActorResumer(apiClient, withParking(parkCfg), withParkingLot(lot), withWarmRouteCache(h.warmRouteCacheTTL))
	return h
}

// onResponseHeaders drops actorKey's cached route if it still matches routed
// when the response shows that the worker no longer serves the actor.
// Responses from the actor itself, whatever their status, leave the route alone.
func (h *Handler) onResponseHeaders(actorKey string, routed routePlacement, resp *extproc.RequestMetadata) *extprocv3.ProcessingResponse {
	// Envoy replied itself: it could not reach the worker, or atunnel refused
	// the CONNECT tunnel. Envoy's reply goes out as it is.
	if details := resp.Attribute(ResponseCodeDetailsAttribute); details != "" && details != "via_upstream" {
		h.resumer.invalidateRoute(actorKey, routed)
		return nil
	}
	// Without a tunnel, atunnel refuses in-band with a marked 421. It strips
	// the marker from actor responses, so this one is its own and the actor
	// never saw the request. Answer 503 (gRPC Unavailable) so clients retry.
	// Inside a tunnel atunnel does not see responses, so the marker proves
	// nothing there.
	if !h.upstreamConnect && resp.Header(":status") == "421" && resp.Header(atenet.StaleAssignmentHeader) == "true" {
		h.resumer.invalidateRoute(actorKey, routed)
		return extproc.ImmediateResponse(envoy_type.StatusCode_ServiceUnavailable,
			fmt.Sprintf("actor %s unavailable: stale assignment", actorKey))
	}
	return nil
}

func (h *Handler) Direction() extproc.Direction { return extproc.DirectionIngress }

// ParkingStatus returns a snapshot of the parking lot for the /statusz page.
func (h *Handler) ParkingStatus() ParkingStatus { return h.parking.status() }

func (h *Handler) HandleRequestHeaders(ctx context.Context, md *extproc.RequestMetadata) (extproc.Result, error) {
	slog.InfoContext(ctx, "Request", slog.String("host", md.Host))

	// The dataplane doesn't propagate trace context into the ext_proc gRPC
	// stream's metadata — the per-request traceparent arrives in the
	// HTTP headers carried inside the ProcessingRequest payload. Extract
	// from there so our span links to the gateway's ingress span.
	ctx = otel.GetTextMapPropagator().Extract(ctx, md)
	ctx, span := otel.Tracer(serviceName).Start(ctx, "ExtProc.RequestHeaders")
	defer span.End()

	actorRef, err := atenet.ParseTargetActor(routingValue(md, atenet.TargetActorHeader, extproc.TargetActorFilterStateAttribute))
	if err != nil {
		return extproc.Result{}, extproc.NewReqError(envoy_type.StatusCode_NotFound, "invalid actor reference")
	}

	// CONNECT traffic can name a port other than defaultActorPort. After Envoy
	// terminates CONNECT, filter state retains the outer authority while md.Host
	// belongs to the inner request.
	targetPort := defaultActorPort
	targetAuthority := md.Attribute(extproc.ConnectAuthorityFilterStateAttribute)
	if targetAuthority == "" && md.Method == http.MethodConnect {
		targetAuthority = md.Host
	}
	if _, portStr, err := net.SplitHostPort(targetAuthority); err == nil {
		if p, ok := atunnelingress.ParsePort(portStr); ok {
			targetPort = p
		}
	}

	slog.InfoContext(ctx, "ResumeActor", slog.Any("actor", actorRef))
	actor, resumeOutcome, err := h.resumer.ResumeActor(ctx, actorRef)
	if err != nil {
		return extproc.Result{Resume: string(resumeOutcome)}, mapResumeError(actorRef, err)
	}

	// ActorTemplate reference, used as low-cardinality route-latency metric
	// attributes.
	res := extproc.Result{
		TemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		TemplateName:     actor.GetActorTemplate().GetName(),
		Resume:           string(resumeOutcome),
	}
	if resumeOutcome == ResumeOutcomeCached {
		actorKey := actorRef.String()
		routed := placementOf(actor)
		res.OnResponseHeaders = func(resp *extproc.RequestMetadata) *extprocv3.ProcessingResponse {
			return h.onResponseHeaders(actorKey, routed, resp)
		}
	}

	// The first IP is in the cluster's primary IP family.
	// TODO: choose the IP family that matches the dataplane's own address.
	assignment := actor.GetStatus().GetWorkerAssignment()
	var workerIP string
	if ips := assignment.GetWorkerPodIps(); len(ips) > 0 {
		workerIP = ips[0]
	}
	workerSPIFFEID := assignment.GetWorkerSpiffeId()
	slog.InfoContext(ctx, "ResumeActor result",
		slog.Any("actor", actorRef),
		slog.String("state", actor.GetStatus().GetState().String()),
		slog.String("workerIP", workerIP),
		slog.String("workerSPIFFEID", workerSPIFFEID))

	// Without the SPIFFE ID, atunnel's identity can't be checked.
	if ip := net.ParseIP(workerIP); ip == nil || workerSPIFFEID == "" {
		return res, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// atunnel's regular HTTPS ingress listens on :443 and forwards to the
	// actor's targetPort.
	targetAddr := net.JoinHostPort(workerIP, "443")

	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	// ext_proc clients may use regular HTTPS on :443 or choose to CONNECT to
	// atunnel instead.
	originalDst := map[string]any{
		OriginalDstAddressKey: targetAddr,
		OriginalDstPortKey:    strconv.Itoa(targetPort),
		WorkerSPIFFEIDKey:     workerSPIFFEID,
	}
	if uid := actor.GetMetadata().GetUid(); uid != "" {
		originalDst[ActorUIDMetadataKey] = uid
	}
	dynamicMetadata, err := structpb.NewStruct(map[string]any{
		OriginalDstMetadataKey: originalDst,
	})
	if err != nil {
		return res, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// Overwrite the routing header so a client-provided value cannot select a
	// different actor after this request has been resolved.
	mutation := &extprocv3.HeaderMutation{}
	mutation.SetHeaders = append(mutation.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      atenet.TargetActorHeader,
			RawValue: []byte(actorRef.String()),
		},
		AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
	})

	res.Target = targetAddr
	res.Response = &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{
			HeaderMutation: mutation,
		},
	}
	res.DynamicMetadata = dynamicMetadata
	return res, nil
}

func routingValue(md *extproc.RequestMetadata, header, attribute string) string {
	if value := md.Attribute(attribute); value != "" && value != "-" {
		return value
	}
	// Requests reaching main_internal via a terminated CONNECT tunnel retain
	// the tunnel's authority in ConnectAuthorityFilterStateAttribute. For such
	// requests, the actor header must have arrived on the outer CONNECT request;
	// inner tunneled headers are untrusted and must never be used for routing.
	if md.Attribute(extproc.ConnectAuthorityFilterStateAttribute) != "" {
		return ""
	}
	return md.Header(header)
}

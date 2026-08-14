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
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// OriginalDstMetadataKey is the dynamic-metadata namespace both this
	// handler and the network ext_proc leg (network.go) write the resolved
	// worker address and target port into, and the one namespace xds.go's
	// ORIGINAL_DST cluster reads from for Envoy. The exact same struct also
	// reaches agentgateway, which surfaces ext_proc's dynamic_metadata field
	// verbatim to CEL as `extproc` (see configmap.yaml's dynamic backend
	// `target` expression) -- one response payload, two dataplanes reading it
	// their own way. Reuses Envoy's own
	// envoy.filters.listener.original_dst listener filter's namespace
	// instead of inventing one; see xds.go's OriginalDstClusterName doc for
	// why that does not collide with the listener filter's own, unrelated
	// use of it.
	OriginalDstMetadataKey = "envoy.filters.listener.original_dst"
	// OriginalDstAddressKey is the field within OriginalDstMetadataKey
	// carrying the resolved worker atunnel address (IP:443). Reuses the same
	// field name (rather than "address") that the
	// envoy.filters.listener.original_dst listener filter itself reads for
	// its own, unrelated EnvoyInternal fallback path.
	OriginalDstAddressKey = "local"
	// OriginalDstPortKey is the field within OriginalDstMetadataKey carrying
	// the actor's target port (the CONNECT authority's port, or 80 for plain
	// ingress -- see HandleRequestHeaders). atunnel can't read Envoy's
	// dynamic metadata directly, so for envoy mode xds.go's buildRoutes
	// derives a real atunnel.TargetPortHeader header for it from this field
	// via a %DYNAMIC_METADATA(...)% format string; HandleRequestHeaders also
	// sets that same header directly (redundant for envoy, but agentgateway
	// mode has no equivalent route-level mechanism and depends on it).
	OriginalDstPortKey = "port"

	// AuthorityFilterStateKey is the filter-state object key holding a
	// CONNECT request's (or, for plain ingress, any request's) :authority,
	// set by xds.go's authorityFilterStateFilter and shared with the
	// upstream internal connection so main_internal's HTTP and network
	// ext_proc legs can both read it back -- via
	// AuthorityFilterStateAttribute -- across the internal-listener hop that
	// dynamic metadata does not survive.
	AuthorityFilterStateKey = "dev.ate.authority"
	// AuthorityFilterStateAttribute is the request_attributes/
	// connection_attributes CEL expression (see xds.go's buildHcm and
	// buildTcpConnectFilterChain) that reads AuthorityFilterStateKey back out
	// for ext_proc. Its exact text is also the field key ext_proc reports the
	// value under, which HandleRequestHeaders (HTTP leg) and handleFirstFrame
	// (network leg) both read via a scan over every filter's attributes
	// rather than hardcoding the name of the filter that reported it.
	AuthorityFilterStateAttribute = "filter_state['" + AuthorityFilterStateKey + "']"
)

// Handler routes ingress requests to the worker hosting their actor.
type Handler struct {
	resumer *ActorResumer
	parking *parkingLot
}

func New(apiClient ateapipb.ControlClient, parkCfg ParkedRequestConfig, parkMetrics *ParkingMetrics) *Handler {
	return &Handler{
		resumer: NewActorResumer(apiClient, withParking(parkCfg)),
		parking: newParkingLot(parkCfg, parkMetrics),
	}
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
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(md.Headers))
	ctx, span := otel.Tracer(extproc.ServiceName).Start(ctx, "ExtProc.RequestHeaders")
	defer span.End()

	// The actor is always resolved from the forwarded filter-state authority
	// attribute, never the Host/:authority header directly: that header is
	// only reliable for the plain ingress_http/ingress_https listeners
	// (which populate the filter state themselves via xds.go's
	// authorityFilterStateFilter). For CONNECT-tunneled traffic reinjected
	// through main_internal, the tunneled protocol's own :authority is
	// unrelated to the actor's DNS name; the authoritative value is whatever
	// connect_terminate captured at CONNECT time and shared with upstream via
	// filter state. Same source network.go's handleFirstFrame uses for the
	// TCP leg.
	authority := md.Attribute(AuthorityFilterStateAttribute)
	if authority == "" {
		return extproc.Result{}, invalidHostErr(md.Host, fmt.Errorf("missing %s request attribute", AuthorityFilterStateAttribute))
	}
	actorRef, err := parseActorRef(authority)
	if err != nil {
		// Authority is invalid, respond with 404.
		return extproc.Result{}, invalidHostErr(authority, err)
	}

	// The port to reach on the actor itself travels in the same authority:
	// for CONNECT-tunneled traffic it's the arbitrary port the client asked
	// for (e.g. ":9090"), and for plain ingress_http/ingress_https it's
	// absent, defaulting to the actor's normal port 80. atunnel's Server
	// can't learn this any other way -- its Config.Upstream is fixed for its
	// whole lifetime -- so it's forwarded via atunnel.TargetPortHeader.
	targetPort := 80
	if _, portStr, err := net.SplitHostPort(authority); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 && p <= 65535 {
			targetPort = p
		}
	}

	// Admit the request to the parking lot before resuming. While resume is
	// in-flight the request occupies a slot; if the actor's worker pool is
	// momentarily saturated the resumer parks (retries) here rather than failing
	// fast. A full lot sheds the request immediately so the router applies
	// backpressure instead of queueing without bound.
	release, ok := h.parking.enter(ctx)
	if !ok {
		return extproc.Result{}, parkingFullErr(actorRef.String())
	}

	slog.InfoContext(ctx, "ResumeActor", slog.Any("actor", actorRef))
	actor, resumeOutcome, err := h.resumer.ResumeActor(ctx, actorRef)
	release(parkOutcomeFor(err))
	if err != nil {
		return extproc.Result{Resume: string(resumeOutcome)}, mapResumeError(actorRef, err)
	}

	// Actor template identity, used as low-cardinality route-latency metric
	// attributes.
	res := extproc.Result{
		TemplateNamespace: actor.GetActorTemplateNamespace(),
		TemplateName:      actor.GetActorTemplateName(),
		Resume:            string(resumeOutcome),
	}

	workerIP := actor.GetWorkerAssignment().GetWorkerPodIp()
	slog.InfoContext(ctx, "ResumeActor result",
		slog.Any("actor", actorRef),
		slog.String("status", actor.GetStatus().String()),
		slog.String("workerIP", workerIP))

	if ip := net.ParseIP(workerIP); ip == nil {
		return res, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// The actor is reached through the in-worker atunnel ingress server, which
	// listens on :443 (mTLS) and forwards to targetPort on the actor. The
	// worker no longer DNATs pod-IP:80 to the actor, so the router dials :443
	// and the ORIGINAL_DST cluster's upstream TLS context presents the
	// router's podidentity client cert (see xds.go's buildOriginalDstCluster
	// and buildUpstreamTransportSocket).
	targetAddr := net.JoinHostPort(workerIP, "443")

	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	// Report the resolved worker address and target port as dynamic metadata
	// rather than a header mutation: a header only works for HTTP traffic,
	// and this same server may in principle be reused by transports that
	// aren't. atunnel can't read this metadata directly, so xds.go's
	// buildRoutes derives its own copy of targetPort as a real header from
	// OriginalDstPortKey via a %DYNAMIC_METADATA(...)% format string. See
	// OriginalDstMetadataKey and xds.go's buildOriginalDstCluster's
	// MetadataKey.
	dynamicMetadata, err := structpb.NewStruct(map[string]any{
		OriginalDstMetadataKey: map[string]any{
			OriginalDstAddressKey: targetAddr,
			OriginalDstPortKey:    strconv.Itoa(targetPort),
		},
	})
	if err != nil {
		return res, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// Neither dataplane needs a header to route to the resolved worker
	// address: both read it straight from dynamicMetadata above -- Envoy via
	// the ORIGINAL_DST cluster's MetadataKey (see xds.go's
	// buildOriginalDstCluster), agentgateway via a CEL expression on its
	// dynamic backend (see configmap.yaml). :authority/Host is never touched
	// either, so atunnel authorizes the actor by its own, unmodified Host.
	mutation := &extprocv3.HeaderMutation{}
	// atunnel picks which port on the actor to reach from this header (the
	// CONNECT authority's port, or 80 for plain ingress -- see targetPort
	// above). For envoy mode this duplicates what xds.go's buildRoutes
	// derives declaratively from OriginalDstPortKey via
	// %DYNAMIC_METADATA(...)% -- harmless, since both compute the same
	// value, and the route's OVERWRITE_IF_EXISTS_OR_ADD wins last.
	// Agentgateway has no equivalent of that route-level mechanism, so it
	// depends on this header mutation being set directly.
	mutation.SetHeaders = append(mutation.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{
			Key:      atunnel.TargetPortHeader,
			RawValue: []byte(strconv.Itoa(targetPort)),
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

// parseActorRef extracts the actor an incoming request is addressed to from its
// Host/:authority, which has the form
// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev" (optionally with a
// port). The atespace is part of the name because an actor name is only unique
// within its atespace.
func parseActorRef(host string) (resources.ActorRef, error) {
	if strings.Contains(host, ":") {
		h, _, err := net.SplitHostPort(host)
		if err != nil {
			return resources.ActorRef{}, err
		}
		host = h
	}
	return resources.ParseActorDNSName(host)
}

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
	"log/slog"
	"net"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// OriginalDstHeader carries the resolved worker atunnel address (IP:443) to
// the ORIGINAL_DST cluster. It is this handler's contract with the ingress
// Envoy configuration the xDS server generates.
const OriginalDstHeader = "x-ate-original-dst"

// Handler routes ingress requests to the worker hosting their actor.
type Handler struct {
	resumer *ActorResumer
	parking *parkingLot
	// routeViaAuthority rewrites :authority to the worker atunnel address for
	// data planes that dial it rather than OriginalDstHeader. See
	// addRoutingMutations.
	routeViaAuthority bool
}

func New(apiClient ateapipb.ControlClient, parkCfg ParkedRequestConfig, parkMetrics *ParkingMetrics, routeViaAuthority bool) *Handler {
	return &Handler{
		resumer:           NewActorResumer(apiClient, withParking(parkCfg)),
		parking:           newParkingLot(parkCfg, parkMetrics),
		routeViaAuthority: routeViaAuthority,
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

	actorRef, err := parseActorRef(md.Host)
	if err != nil {
		// Host is invalid, respond with 404.
		return extproc.Result{}, invalidHostErr(md.Host, err)
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
	// listens on :443 (mTLS) and forwards to the actor's :80. The worker no
	// longer DNATs pod-IP:80 to the actor, so the router dials :443 and the
	// ORIGINAL_DST cluster's upstream TLS context presents the router's
	// podidentity client cert (see buildOriginalDstCluster and
	// buildUpstreamTransportSocket).
	// TODO(bowei) -- handle more than port 80 on the actor.
	targetAddr := net.JoinHostPort(workerIP, "443")

	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	// Route by telling the ORIGINAL_DST cluster which worker atunnel address to
	// dial, without touching :authority — atunnel authorizes the actor by the
	// original Host (actor DNS name).
	mutation := &extprocv3.HeaderMutation{}
	addRoutingMutations(targetAddr, md.Host, h.routeViaAuthority, mutation)

	res.Target = targetAddr
	res.Response = &extprocv3.HeadersResponse{
		Response: &extprocv3.CommonResponse{
			HeaderMutation: mutation,
		},
	}
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

// addOriginalDstMutation sets the header the ORIGINAL_DST cluster reads to pick
// the upstream address (the worker atunnel IP:443). Unlike an :authority
// rewrite it leaves the request Host intact, so atunnel still sees the actor
// DNS name and can authorize the active actor.
//
// Nothing strips this header from the incoming request, so overwrite rather
// than append: a client-supplied value must never influence the address Envoy
// dials. ext_proc mutations already default to replace, but the default is
// split across the deprecated append field and append_action — pin it.
func addOriginalDstMutation(dst string, mut *extprocv3.HeaderMutation) {
	mut.SetHeaders = append(mut.SetHeaders,
		&corev3.HeaderValueOption{
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
			Header: &corev3.HeaderValue{
				Key:      OriginalDstHeader,
				RawValue: []byte(dst),
			},
		},
	)
}

// addRoutingMutations overwrites all routing metadata derived from the
// control-plane result. Envoy dials OriginalDstHeader while preserving
// :authority. Agentgateway v1.4.1's static dynamic backend instead dials the
// request :authority, so that mode rewrites it to the worker atunnel address.
// OriginalHostHeader lets atunnel restore and authorize the actor authority.
func addRoutingMutations(dst, actorHost string, routeViaAuthority bool, mut *extprocv3.HeaderMutation) {
	addOriginalDstMutation(dst, mut)
	mut.SetHeaders = append(mut.SetHeaders, &corev3.HeaderValueOption{
		AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
		Header: &corev3.HeaderValue{
			Key:      atunnel.OriginalHostHeader,
			RawValue: []byte(actorHost),
		},
	})
	if routeViaAuthority {
		mut.SetHeaders = append(mut.SetHeaders, &corev3.HeaderValueOption{
			AppendAction: corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD,
			Header: &corev3.HeaderValue{
				Key:      extproc.AuthorityHeader,
				RawValue: []byte(dst),
			},
		})
	}
}

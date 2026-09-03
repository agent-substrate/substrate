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

	// Resolved from filter state rather than Host/:authority directly: a
	// reinjected CONNECT tunnel's own :authority has nothing to do with the
	// actor, so xds.go captures the real one at connect_terminate instead.
	authority := md.Attribute(extproc.AuthorityFilterStateAttribute)
	if authority == "" {
		return extproc.Result{}, invalidHostErr(md.Host, fmt.Errorf("missing %s request attribute", extproc.AuthorityFilterStateAttribute))
	}
	actorRef, err := parseActorRef(authority)
	if err != nil {
		// Authority is invalid, respond with 404.
		return extproc.Result{}, invalidHostErr(authority, err)
	}

	// CONNECT traffic can name a port other than defaultActorPort in the
	// authority.
	targetPort := defaultActorPort
	if _, portStr, err := net.SplitHostPort(authority); err == nil {
		if p, ok := atunnel.ParsePort(portStr); ok {
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
		TemplateAtespace: actor.GetActorTemplate().GetAtespace(),
		TemplateName:     actor.GetActorTemplate().GetName(),
		Resume:           string(resumeOutcome),
	}

	workerIP := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp()
	slog.InfoContext(ctx, "ResumeActor result",
		slog.Any("actor", actorRef),
		slog.String("state", actor.GetStatus().GetState().String()),
		slog.String("workerIP", workerIP))

	if ip := net.ParseIP(workerIP); ip == nil {
		return res, extproc.NewReqError(envoy_type.StatusCode_InternalServerError,
			"actor %s routing failed", actorRef)
	}

	// atunnel's regular HTTPS ingress listens on :443 and forwards to the
	// actor's targetPort.
	targetAddr := net.JoinHostPort(workerIP, "443")

	slog.InfoContext(ctx, "Route ok", slog.Any("actor", actorRef), slog.String("targetAddr", targetAddr))

	// ext_proc clients may use regular HTTPS on :443 or choose to CONNECT to
	// atunnel instead.
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

	// :authority/Host stays untouched, so atunnel authorizes by the actor's
	// own DNS name. The target port still goes as a header: atunnel can't
	// read dynamic metadata.
	mutation := &extprocv3.HeaderMutation{}
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

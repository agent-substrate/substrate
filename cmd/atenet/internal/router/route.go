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

import "context"

// RouteRequest is a gateway-neutral description of an inbound request that
// needs actor-aware routing. Adapters (ExtProc, future xDS, gRPC) fill this
// from their own protocol types and hand it to a RouteResolver.
type RouteRequest struct {
	// Authority is the HTTP :authority / Host header value.
	// The actor ID is parsed from the subdomain suffix.
	Authority string

	// Path is the HTTP request path (passed through; not used for routing in M1).
	Path string

	// Headers carries trace-propagation headers (e.g. traceparent).
	// Not used for actor identity.
	Headers map[string]string

	// SourceActorID is the verified identity of the calling actor.
	// Reserved for M4 (pre-resume authorization); ignored in M1.
	SourceActorID string

	// Adapter is an opaque label identifying which adapter is calling the
	// resolver (e.g. "extproc"). Used for metrics and logging.
	Adapter string
}

// RouteSuccess is the happy-path result of a route resolution.
type RouteSuccess struct {
	// ActorID is the resolved actor identifier.
	ActorID string

	// Backend is the IP/port of the worker pod to forward traffic to.
	Backend Backend

	// TemplateRef identifies the ActorTemplate that owns this actor,
	// used as low-cardinality metric attributes.
	TemplateRef ActorTemplateRef
}

// Backend is the network endpoint of a live actor worker pod.
type Backend struct {
	IP   string
	Port int
}

// ActorTemplateRef identifies an ActorTemplate by namespace and name.
type ActorTemplateRef struct {
	Namespace string
	Name      string
}

// RouteDenial is a gateway-neutral error result. HTTP status and message are
// client-safe; Cause is preserved for log inspection only.
type RouteDenial struct {
	// HTTPStatus is the HTTP status code to return to the client (e.g. 404, 503).
	HTTPStatus int

	// Message is a client-safe human-readable explanation.
	Message string

	// Cause is the underlying error preserved for logs. Never sent to clients.
	Cause error

	// OutcomeCode is a low-cardinality label for metrics (ok/not_found/cancelled/error).
	OutcomeCode string
}

func (d *RouteDenial) Error() string { return d.Message }
func (d *RouteDenial) Unwrap() error { return d.Cause }

// RouteResolution holds either a successful resolution or a denial.
// Exactly one of Success or Denial is non-nil.
type RouteResolution struct {
	Success *RouteSuccess
	Denial  *RouteDenial
}

// RouteResolver resolves an inbound RouteRequest to a backend endpoint or denial.
// Implementations must be safe for concurrent use.
type RouteResolver interface {
	ResolveRoute(ctx context.Context, req RouteRequest) RouteResolution
}

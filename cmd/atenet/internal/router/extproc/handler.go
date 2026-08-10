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

package extproc

import (
	"context"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// Handler applies one direction's policy to a request. Implementations live in
// the ingress and egress packages; the mux never inspects what they do, only
// which direction they serve.
type Handler interface {
	// Direction reports the traffic direction this handler serves. The mux keys
	// its dispatch table by it.
	Direction() Direction

	// HandleRequestHeaders decides what the dataplane should do with a request
	// whose headers have just arrived.
	//
	// A returned error denies the request: a *ReqError carries the status code
	// and client-safe body to answer with, anything else becomes a 500. The
	// Result is read even when an error is returned, so a handler that got far
	// enough to learn the metric attributes (template identity, resume outcome)
	// should still fill them in.
	HandleRequestHeaders(ctx context.Context, md *RequestMetadata) (Result, error)
}

// Result is what a handler tells the mux about a request it allowed.
type Result struct {
	// Response is the header mutation the dataplane applies before the request
	// continues. Handlers that only authenticate return an empty CommonResponse.
	Response *extprocv3.HeadersResponse

	// Target is the upstream address the request was routed to, shown on the
	// /statusz page. Empty for handlers that do not pick an upstream.
	Target string

	// TemplateNamespace and TemplateName identify the actor template the
	// request resolved to. They are the low-cardinality attributes on the
	// route-duration metric, and are empty when the direction has no template
	// (or the request failed before resolving one).
	TemplateNamespace string
	TemplateName      string

	// Resume is the actor-resume outcome, as one of the ateattr.RouterResume*
	// values. Empty means "none" — the direction never resumes an actor, or the
	// request never got that far.
	Resume string
}

// resume returns the resume label for the route-duration metric, defaulting an
// unset outcome to "none".
func (r Result) resume() string {
	if r.Resume == "" {
		return ateattr.RouterResumeNone
	}
	return r.Resume
}

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

package inner

import (
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// Request includes all the information needed to make an authorization decision.
type Request struct {
	// Method is the method of the HTTP request from an actor.
	Method string

	// Authority comes from the `:authority` header, and falls back to the `host` header.
	Authority string

	// Path comes from the `:path` header.
	Path string

	// Scheme comes from the `:scheme` header.
	Scheme string

	// Headers include all the request headers, keys lower-cased.
	Headers map[string]string

	// ActorSpiffe is the SPIFFE ID for the actor.
	// Example: spiffe://substrate-actor.local/atespace/demo/actor/egress-demo
	ActorSpiffe string

	// Atespace and ActorName are ActorSpiffe split into its two parts, both ""
	// when ActorSpiffe is "" or does not parse.
	Atespace  string
	ActorName string
}

// Host is Authority with any port removed and the name lower-cased, which is
// the form hostname policy should match on. DNS names are case-insensitive, so
// a policy comparing Authority directly would be bypassed by an uppercase Host
// header.
func (r *Request) Host() string {
	authority := r.Authority
	if host, _, err := net.SplitHostPort(authority); err == nil {
		authority = host
	}
	return strings.ToLower(strings.TrimSuffix(authority, "."))
}

// newRequest projects one ext_proc message onto the Request a Policy sees: the
// headers Envoy decrypted, plus the identity the gateway captured.
func newRequest(attr map[string]*structpb.Struct, headers *extprocv3.HttpHeaders) *Request {
	req := requestFromHeaders(headers)
	req.ActorSpiffe = actorFromAttributes(attr)
	req.Atespace, req.ActorName = parseActor(req.ActorSpiffe)
	return req
}

// requestFromHeaders projects the ext_proc header map onto a Request.
func requestFromHeaders(headers *extprocv3.HttpHeaders) *Request {
	all := make(map[string]string)
	for _, header := range headers.GetHeaders().GetHeaders() {
		all[strings.ToLower(header.GetKey())] = headerValue(header)
	}
	return &Request{
		Method:    all[":method"],
		Authority: authorityOf(all),
		Path:      all[":path"],
		Scheme:    all[":scheme"],
		Headers:   all,
	}
}

// authorityOf prefers :authority and falls back to host.
func authorityOf(headers map[string]string) string {
	if authority := headers[":authority"]; authority != "" {
		return authority
	}
	return headers["host"]
}

// headerValue reads a header value, preferring raw_value.
//
// Recent Envoy sends values in HeaderValue.raw_value (bytes) and leaves
// HeaderValue.value (string) empty.
func headerValue(header *corev3.HeaderValue) string {
	if raw := header.GetRawValue(); len(raw) > 0 {
		return string(raw)
	}
	return header.GetValue()
}

// requestAttrs describes a request for a decision log line: who asked, for
// what, and with which headers.
func requestAttrs(req *Request) []any {
	return []any{
		slog.String("authority", req.Authority),
		slog.String("method", req.Method),
		slog.String("path", req.Path),
		slog.String("scheme", req.Scheme),
		slog.String("actor", req.ActorSpiffe),
		slog.String("atespace", req.Atespace),
		slog.String("actor_name", req.ActorName),
		slog.Any("headers", loggedHeaders(req.Headers)),
	}
}

// loggedHeaders is a header set on its way into a log record. Request.Headers
// is a plain map; this exists only at the log call sites, so that policy code
// handles headers as an ordinary map and only the rendering is special.
type loggedHeaders map[string]string

// redactedHeaders are logged by name with their value withheld. The gateway
// logs the full header set, and an actor's tunneled request can carry its own
// upstream credentials — so without this, turning on debug logging copies
// every actor's secrets into the cluster's log pipeline, where they outlive
// the request and are readable by anyone with log access.
var redactedHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"set-cookie":          true,
	"x-api-key":           true,
}

// LogValue renders the headers as a sorted group, one attribute per header.
func (h loggedHeaders) LogValue() slog.Value {
	keys := make([]string, 0, len(h))
	for key := range h {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	attrs := make([]slog.Attr, 0, len(keys))
	for _, key := range keys {
		if redactedHeaders[key] {
			attrs = append(attrs, slog.String(key, fmt.Sprintf("<redacted, %d bytes>", len(h[key]))))
			continue
		}
		attrs = append(attrs, slog.String(key, h[key]))
	}
	return slog.GroupValue(attrs...)
}

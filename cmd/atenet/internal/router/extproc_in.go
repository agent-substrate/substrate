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
	"net"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// extProcFilterName keys this filter's entry in a ProcessingRequest's
	// attributes map.
	extProcFilterName = "envoy.filters.http.ext_proc"
	// tlsVersionAttribute is the Envoy attribute holding the TLS version
	// negotiated on the downstream connection. It is requested via
	// ExternalProcessor.request_attributes (see buildHcm). Envoy omits it
	// entirely for connections that did not complete a TLS handshake.
	tlsVersionAttribute = "connection.tls_version"

	// Ports the actor serves on. Plaintext ingress is forwarded to 80;
	// TLS ingress is re-originated as TLS to 443.
	actorHTTPPort  = "80"
	actorHTTPSPort = "443"
)

type requestMetadata struct {
	headers map[string]string
	path    string
	host    string
}

func newRequestMetadata(headers []*corev3.HeaderValue) *requestMetadata {
	headersMap := make(map[string]string)
	var path string
	var host string

	for _, h := range headers {
		k := strings.ToLower(h.Key)
		val := h.Value
		if val == "" && len(h.RawValue) > 0 {
			val = string(h.RawValue)
		}

		headersMap[k] = val
		if k == ":path" {
			path = val
		}
		if k == ":authority" || k == "host" {
			host = val
		}
	}

	return &requestMetadata{
		headers: headersMap,
		path:    path,
		host:    host,
	}
}

// requestTLSVersion reports the TLS version the request's downstream connection
// negotiated, from the attributes Envoy attaches to the ProcessingRequest. It is
// "" for a plaintext connection.
//
// The connection's own TLS state decides whether ingress was encrypted, rather
// than request.scheme, because Envoy derives :scheme from x-forwarded-proto and
// preserves a caller-supplied one unless configured as an edge proxy — reading
// the scheme would let a caller pick which actor port this routes to. Nothing on
// the wire can influence connection state.
//
// An absent attribute also yields "", which is read as plaintext — the safe
// direction, since it cannot turn a plaintext request into an upstream TLS one.
func requestTLSVersion(attrs map[string]*structpb.Struct) string {
	return attrs[extProcFilterName].GetFields()[tlsVersionAttribute].GetStringValue()
}

// actorPortForIngress returns the actor port a request is forwarded to. TLS
// ingress is re-originated as TLS to 443; plaintext ingress goes to 80.
func actorPortForIngress(tls bool) string {
	if tls {
		return actorHTTPSPort
	}
	return actorHTTPPort
}

// parseActorRef extracts the (atespace, actor name) an incoming request is
// addressed to from its Host/:authority, which has the form
// "<actor_name>.<atespace>.actors.resources.substrate.ate.dev" (optionally with a
// port). The atespace is required because an actor name is only unique within its
// atespace.
func parseActorRef(host string) (atespace, actorName string, err error) {
	if strings.Contains(host, ":") {
		host, _, err = net.SplitHostPort(host)
		if err != nil {
			return "", "", err
		}
	}
	return resources.ParseActorDNSName(host)
}

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
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
)

// Direction is the gateway a request arrived through. It selects the handler,
// so it must come from something the dataplane asserts rather than from the
// request.
type Direction string

const (
	// DirectionIngress is inbound traffic addressed to an actor.
	DirectionIngress Direction = "ingress"
	// DirectionEgress is outbound traffic tunneled out of an actor.
	DirectionEgress Direction = "egress"
)

// EgressFilterChainName is the Envoy filter chain that terminates actor egress
// CONNECTs, and so the one that selects the egress handler. It must stay in sync
// with the filter chain name in manifests/ate-install/atenet-egress.yaml.
const EgressFilterChainName = "egress"

// directionOf reports which direction's handler an ext_proc RequestHeaders
// callback belongs to.
//
// Dispatch is by filter chain, not by :method, because the two directions apply
// opposite trust models: on egress the actor identity comes from a client
// certificate Envoy validated against the actor-identity CA, while on ingress
// every request header is unauthenticated client input. Keying on :method would
// let any external client sending CONNECT select the egress handler and use its
// denial messages as an actor-existence and status oracle. Envoy asserts the
// filter chain name; the request cannot influence it.
//
// An unrecognized or absent attribute means ingress, the fail-safe direction:
// an egress request misrouted to the ingress handler fails to parse as an actor
// DNS name and 404s, whereas the reverse leaks control-plane state.
func directionOf(req *extprocv3.ProcessingRequest) Direction {
	if requestAttribute(req, directionAttribute) == string(DirectionEgress) {
		return DirectionEgress
	}
	if filterChainName(req) == EgressFilterChainName {
		return DirectionEgress
	}
	return DirectionIngress
}

// filterChainName returns the xds.filter_chain_name attribute Envoy attached to
// the request, or "" when the listener did not request the attribute. The
// attributes map is keyed by the ext_proc filter's name within the HCM chain,
// which we do not want to hardcode here, so scan every entry.
func filterChainName(req *extprocv3.ProcessingRequest) string {
	return requestAttribute(req, FilterChainNameAttribute)
}

func requestAttribute(req *extprocv3.ProcessingRequest, name string) string {
	for _, attrs := range req.GetAttributes() {
		if v, ok := attrs.GetFields()[name]; ok {
			return v.GetStringValue()
		}
	}
	return ""
}

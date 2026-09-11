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

// Substrate's dataplane attribute namespace: the filter-state objects and CEL
// request attributes the gateways carry alongside a request, declared here once
// so a key means the same thing in the Go that reads it and in the dataplane
// configuration that sets it.
//
// Every substrate-owned key is rooted at the reverse-DNS "dev.ate." prefix.
// These keys live in namespaces shared with the proxies that carry them -- Envoy
// filter state, agentgateway CEL -- where a vendor-qualified root is what keeps
// substrate's keys from colliding with anyone else's. That is a different
// constraint from the telemetry attributes in internal/ateattr, which are
// substrate's own metric dimensions and stay on dotted "ate.". Neither is the
// "ate.dev/" slash form, which is Kubernetes labels only.
const (
	// TargetActorFilterStateKey carries the ingress actor routing target across
	// Envoy's CONNECT internal-listener hop.
	TargetActorFilterStateKey      = "dev.ate.target.actor"
	ConnectAuthorityFilterStateKey = "dev.ate.connect.authority"

	// TargetActorFilterStateAttribute is the CEL expression ext_proc evaluates
	// to read the corresponding filter state.
	TargetActorFilterStateAttribute      = "filter_state['" + TargetActorFilterStateKey + "']"
	ConnectAuthorityFilterStateAttribute = "filter_state['" + ConnectAuthorityFilterStateKey + "']"

	// ActorIdentityFilterStateKey holds the actor's SPIFFE ID
	// (resources.ActorSPIFFEID), read from the peer certificate's URI SAN.
	// The outer CONNECT chain sets it from %DOWNSTREAM_PEER_URI_SAN% and
	// shares it with the inner legs, which have no certificate of their own.
	ActorIdentityFilterStateKey = "dev.ate.actor.identity"
	// ActorIdentityFilterStateAttribute is the CEL expression ext_proc
	// evaluates to read ActorIdentityFilterStateKey back out.
	ActorIdentityFilterStateAttribute = "filter_state['" + ActorIdentityFilterStateKey + "']"

	// EgressMetadataNamespace is the dynamic-metadata namespace the CONNECT
	// leg answers in. Envoy only keeps it when the outer ext_proc filter lists
	// it under metadata_options.receiving_namespaces; the manifest tests check.
	EgressMetadataNamespace = "dev.ate.egress"
	// EgressPassthroughDestinationKey, under EgressMetadataNamespace, is the
	// original destination as IP:port, present only when an address rule
	// allowed it. The outer chain copies it into the ORIGINAL_DST filter state;
	// absent, a TLS or opaque connection has no upstream and is closed.
	EgressPassthroughDestinationKey = "passthrough_destination"

	// directionAttribute carries the Direction outright, for dataplanes that
	// have no Envoy filter chain to name. It is set from a dataplane expression,
	// never from a client header. No dataplane in this repository sets it today:
	// Envoy names its filter chain, and agentgateway routes both directions
	// through its own substrateIngress/substrateEgress policies rather than
	// ext_proc.
	directionAttribute = "dev.ate.extproc.direction"
)

// FilterChainNameAttribute is the CEL attribute carrying the name of the filter
// chain that accepted the request. Envoy's own, not substrate's, so it is not
// under "dev.ate.". The egress Envoy asks for it via request_attributes on its
// ext_proc filter.
//
// Do not "improve" this to xds.listener_name: Envoy 1.34 cannot parse that one,
// and rather than failing config load it logs "error parsing cel expression" at
// trace level and sends an empty attributes map. An absent attribute means
// ingress here, so every egress CONNECT would silently take the ingress path and
// 404 on the actor DNS name parse.
const FilterChainNameAttribute = "xds.filter_chain_name"

// EgressPassthroughDestinationFormat is the access-log and set_filter_state
// format string that reads EgressPassthroughDestinationKey back out.
const EgressPassthroughDestinationFormat = "%DYNAMIC_METADATA(" + EgressMetadataNamespace + ":" + EgressPassthroughDestinationKey + ")%"

// OriginalDstFilterStateKey is Envoy's filter-state key for the address an
// ORIGINAL_DST cluster dials. The outer CONNECT chain sets it from
// EgressPassthroughDestinationKey; the request legs read it for their logs.
const OriginalDstFilterStateKey = "envoy.network.transport_socket.original_dst_address"

// OriginalDstAttribute is the CEL expression that reads OriginalDstFilterStateKey.
const OriginalDstAttribute = "filter_state['" + OriginalDstFilterStateKey + "']"

// RequestedServerNameAttribute is the SNI of the connection a request arrived
// on. The handler logs it next to the Host it authorized.
const RequestedServerNameAttribute = "connection.requested_server_name"

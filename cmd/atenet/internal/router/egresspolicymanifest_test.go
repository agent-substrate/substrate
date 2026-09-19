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
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
)

// These tests pin the egress manifests to the Go constants the handler
// dispatches on and answers with. A chain renamed on one side only fails
// closed at runtime, and nothing else in the tree would catch it.

const (
	extProcFilter        = "envoy.filters.http.ext_proc"
	setFilterStateFilter = "envoy.filters.http.set_filter_state"
	luaFilter            = "envoy.filters.http.lua"
	extProcServerCluster = "ext_proc_server"
	passthroughCluster   = "egress_tcp_passthrough"
	originalDstKey       = "envoy.network.transport_socket.original_dst_address"
	dfpClusterType       = "envoy.clusters.dynamic_forward_proxy"
)

const peerCertificateNamespace = "dev.ate.egress.peer_certificate"

func TestEgressManifestsForwardPeerCertificateMetadata(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			outer := outerChain(t, tree)
			ts := child(outer, "transport_socket")
			tls := child(ts, "typed_config")
			if required, _ := tls["require_client_certificate"].(bool); !required {
				t.Error("outer TLS no longer requires an actor client certificate")
			}
			if _, ok := child(child(tls, "common_tls_context"), "validation_context")["trusted_ca"]; !ok {
				t.Error("outer TLS has no actor identity trusted_ca")
			}
			h := hcm(outer)
			if got := str(h, "forward_client_cert_details"); got != "SANITIZE" {
				t.Errorf("forward_client_cert_details = %q, want SANITIZE", got)
			}
			if _, ok := h["set_current_client_cert_details"]; ok {
				t.Error("outer HCM still generates certificate headers")
			}
			filters := list(h, "http_filters")
			luaAt, extAt := filterIndex(filters, luaFilter), filterIndex(filters, extProcFilter)
			if count := slices.IndexFunc(filters, func(f node) bool { return str(f, "name") == luaFilter }); count < 0 || slices.IndexFunc(filters[count+1:], func(f node) bool { return str(f, "name") == luaFilter }) >= 0 {
				t.Errorf("outer HCM must have exactly one Lua producer")
			}
			if luaAt < 0 || extAt < 0 || luaAt >= extAt {
				t.Fatalf("outer HCM Lua/ext_proc order = %d/%d", luaAt, extAt)
			}
			script := str(child(child(filters[luaAt], "typed_config"), "default_source_code"), "inline_string")
			for _, want := range []string{"downstreamSslConnection", "urlEncodedPemEncodedPeerCertificateChain", peerCertificateNamespace, "chain", "chain = \"\""} {
				if !strings.Contains(script, want) {
					t.Errorf("Lua producer does not contain %q", want)
				}
			}
			cfg := child(filters[extAt], "typed_config")
			forward := strs(child(child(cfg, "metadata_options"), "forwarding_namespaces"), "untyped")
			receive := strs(child(child(cfg, "metadata_options"), "receiving_namespaces"), "untyped")
			if !slices.Equal(forward, []string{peerCertificateNamespace}) || !slices.Equal(receive, []string{extproc.EgressMetadataNamespace}) {
				t.Errorf("metadata namespaces = forward %v receive %v", forward, receive)
			}
			if got := filterIndex(filters, luaFilter); got != 1 {
				t.Errorf("Lua producer index = %d, want immediately after actor filter state", got)
			}
			if writers := filterStateWriters(tree, peerCertificateNamespace); len(writers) != 0 {
				t.Errorf("certificate namespace appears in filter state writers: %d", len(writers))
			}
			for _, lc := range allChains(tree) {
				if str(lc.chain, "name") == extproc.EgressFilterChainName {
					continue
				}
				if containsString(lc.chain, peerCertificateNamespace) {
					t.Errorf("certificate namespace appears on inner chain %q", str(lc.chain, "name"))
				}
			}
		})
	}
}

// requestLegs are the chains that decide per request and answer with a dial.
var requestLegs = []string{extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName}

type node = map[string]any

// bootstrapTree parses the envoy.yaml of the atenet-egress ConfigMap in path
// as a generic tree; the assertions read a few leaves scattered across it.
func bootstrapTree(t *testing.T, path string) node {
	t.Helper()
	var tree node
	if err := yaml.Unmarshal([]byte(envoyConfig(t, path)), &tree); err != nil {
		t.Fatalf("parsing envoy.yaml of %s: %v", path, err)
	}
	return tree
}

func list(n node, key string) []node {
	raw, _ := n[key].([]any)
	out := make([]node, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(node); ok {
			out = append(out, m)
		}
	}
	return out
}

func child(n node, key string) node {
	m, _ := n[key].(node)
	return m
}

func str(n node, key string) string {
	s, _ := n[key].(string)
	return s
}

func strs(n node, key string) []string {
	raw, _ := n[key].([]any)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func listeners(tree node) []node { return list(child(tree, "static_resources"), "listeners") }

func clusters(tree node) []node { return list(child(tree, "static_resources"), "clusters") }

func byName(items []node, name string) node {
	for _, item := range items {
		if str(item, "name") == name {
			return item
		}
	}
	return nil
}

// hcm returns the http_connection_manager's typed_config of a filter chain, or
// nil when the chain has none (a tcp_proxy chain).
func hcm(chain node) node {
	for _, f := range list(chain, "filters") {
		if str(f, "name") == "envoy.filters.network.http_connection_manager" {
			return child(f, "typed_config")
		}
	}
	return nil
}

// filterIndex returns the position of the named filter in filters, or -1.
func filterIndex(filters []node, name string) int {
	for i, f := range filters {
		if str(f, "name") == name {
			return i
		}
	}
	return -1
}

// allChains returns every filter chain in the bootstrap with the listener it
// belongs to.
func allChains(tree node) []struct{ listener, chain node } {
	var out []struct{ listener, chain node }
	for _, l := range listeners(tree) {
		for _, c := range list(l, "filter_chains") {
			out = append(out, struct{ listener, chain node }{l, c})
		}
	}
	return out
}

// filterStateWriters returns every set_filter_state entry anywhere in the
// bootstrap that writes key. Only object_key counts; access-log reads do not.
func filterStateWriters(v any, key string) []node {
	var found []node
	switch t := v.(type) {
	case map[string]any:
		if str(t, "object_key") == key {
			found = append(found, t)
		}
		for _, sub := range t {
			found = append(found, filterStateWriters(sub, key)...)
		}
	case []any:
		for _, sub := range t {
			found = append(found, filterStateWriters(sub, key)...)
		}
	}
	return found
}

func containsString(v any, want string) bool {
	switch t := v.(type) {
	case string:
		return t == want
	case map[string]any:
		for _, value := range t {
			if containsString(value, want) {
				return true
			}
		}
	case []any:
		for _, value := range t {
			if containsString(value, want) {
				return true
			}
		}
	}
	return false
}

// outerChain returns the egress listener's CONNECT chain.
func outerChain(t *testing.T, tree node) node {
	t.Helper()
	outer := byName(list(byName(listeners(tree), "egress"), "filter_chains"), extproc.EgressFilterChainName)
	if outer == nil {
		t.Fatalf("no %q chain on the egress listener", extproc.EgressFilterChainName)
	}
	return outer
}

// Every chain that calls ext_proc must be one the handler knows as an egress
// leg, and each manifest must have exactly the legs its topology implies.
func TestEgressManifestsNameEveryExtProcChain(t *testing.T) {
	want := map[string][]string{
		egressManifests[0]: {extproc.EgressFilterChainName, extproc.EgressCleartextFilterChainName},
		egressManifests[1]: {extproc.EgressFilterChainName, extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName},
	}
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var got []string
			for _, lc := range allChains(bootstrapTree(t, path)) {
				h := hcm(lc.chain)
				if h == nil || filterIndex(list(h, "http_filters"), extProcFilter) < 0 {
					continue
				}
				name := str(lc.chain, "name")
				if !extproc.IsEgressFilterChain(name) {
					t.Errorf("listener %q has a filter chain %q that calls ext_proc but is not a chain the egress handler serves; its callouts would be refused", str(lc.listener, "name"), name)
				}
				got = append(got, name)
			}
			slices.Sort(got)
			expected := slices.Clone(want[path])
			slices.Sort(expected)
			if !slices.Equal(got, expected) {
				t.Errorf("ext_proc-calling filter chains = %v, want %v", got, expected)
			}
		})
	}
}

// extProcOf returns the ext_proc filter config of a chain's HCM and its index
// among the http_filters, or nil and -1.
func extProcOf(chain node) (node, int, []node) {
	filters := list(hcm(chain), "http_filters")
	i := filterIndex(filters, extProcFilter)
	if i < 0 {
		return nil, -1, filters
	}
	return child(filters[i], "typed_config"), i, filters
}

// Every ext_proc in the egress gateway fails closed, talks to the co-located
// sidecar, asks for the attributes its leg reads, and can rewrite nothing that
// routes. A missing attribute reads as "absent" and denies every request.
func TestEgressManifestsExtProcFilters(t *testing.T) {
	required := map[string][]string{
		extproc.EgressFilterChainName:          {extproc.FilterChainNameAttribute},
		extproc.EgressCleartextFilterChainName: {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.OriginalDstIPAttribute, extproc.OriginalDstPortAttribute},
		extproc.EgressTLSMITMFilterChainName:   {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.OriginalDstIPAttribute, extproc.OriginalDstPortAttribute},
	}
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			prefixes := map[string]string{} // stat_prefix -> chain
			for _, lc := range allChains(bootstrapTree(t, path)) {
				name := str(lc.chain, "name")
				attrs, isLeg := required[name]
				if !isLeg {
					continue
				}
				cfg, i, filters := extProcOf(lc.chain)
				if cfg == nil {
					t.Errorf("chain %q has no ext_proc filter", name)
					continue
				}
				if got := str(child(child(cfg, "grpc_service"), "envoy_grpc"), "cluster_name"); got != extProcServerCluster {
					t.Errorf("chain %q ext_proc calls cluster %q, want %q", name, got, extProcServerCluster)
				}
				if allow, _ := cfg["failure_mode_allow"].(bool); allow {
					t.Errorf("chain %q ext_proc has failure_mode_allow: true; a sidecar outage would let every request through", name)
				}
				if prefix := str(cfg, "stat_prefix"); prefix == "" {
					t.Errorf("chain %q ext_proc has no stat_prefix; its failures would be indistinguishable from the other legs'", name)
				} else if other, seen := prefixes[prefix]; seen {
					t.Errorf("chain %q ext_proc reuses stat_prefix %q of chain %q; their stats would be summed", name, prefix, other)
				} else {
					prefixes[prefix] = name
				}
				got := strs(cfg, "request_attributes")
				for _, attr := range attrs {
					if !slices.Contains(got, attr) {
						t.Errorf("chain %q ext_proc does not request %q; the handler would read it as absent and deny", name, attr)
					}
				}
				// The decision has to come before the request is routed, and
				// before dynamic_forward_proxy resolves the Host it names.
				for _, later := range []string{"envoy.filters.http.dynamic_forward_proxy", "envoy.filters.http.router"} {
					if j := filterIndex(filters, later); j >= 0 && j < i {
						t.Errorf("chain %q runs %s before ext_proc", name, later)
					}
				}
				// No leg rewrites routing: the name that was policed is the
				// name that is dialed.
				rules := child(cfg, "mutation_rules")
				if isError, _ := rules["disallow_is_error"].(bool); !isError {
					t.Errorf("chain %q ext_proc does not set mutation_rules.disallow_is_error", name)
				}
				routing, _ := rules["allow_all_routing"].(bool)
				system, _ := rules["disallow_system"].(bool)
				if !system || routing {
					t.Errorf("chain %q ext_proc may rewrite routing headers (disallow_system=%v allow_all_routing=%v)", name, system, routing)
				}
				// Every leg answers with metadata in the egress namespace, the
				// CONNECT leg's passthrough destination or a request leg's
				// dial, and Envoy drops metadata from a namespace the filter
				// does not admit.
				admitted := strs(child(child(cfg, "metadata_options"), "receiving_namespaces"), "untyped")
				if !slices.Contains(admitted, extproc.EgressMetadataNamespace) {
					t.Errorf("chain %q ext_proc does not admit dynamic metadata in %q; its answer would be dropped", name, extproc.EgressMetadataNamespace)
				}
			}
		})
	}
}

// The CONNECT leg's answer is the only source of the address the passthrough
// chain dials: the outer chain copies it into the ORIGINAL_DST filter state
// after ext_proc ran and writes nothing when there was no address.
func TestEgressManifestsConnectLegDecidesThePassthroughDestination(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)

			// One writer in the whole bootstrap: a second one could put back
			// an address the CONNECT leg withheld, since skip_if_empty would
			// not overwrite it.
			writers := filterStateWriters(tree, originalDstKey)
			if len(writers) != 1 {
				t.Fatalf("%s is set by %d filters; only the CONNECT leg's answer may set it", originalDstKey, len(writers))
			}

			filters := list(hcm(outerChain(t, tree)), "http_filters")
			extProcAt := filterIndex(filters, extProcFilter)
			var entry node
			for i, f := range filters {
				if str(f, "name") != setFilterStateFilter {
					continue
				}
				for _, v := range list(child(f, "typed_config"), "on_request_headers") {
					if str(v, "object_key") != originalDstKey {
						continue
					}
					if i < extProcAt {
						t.Errorf("%s is set at http_filters[%d], before ext_proc at [%d]; the metadata it reads does not exist yet", originalDstKey, i, extProcAt)
					}
					entry = v
				}
			}
			if entry == nil {
				t.Fatalf("the one writer of %s is not on the outer chain, where the metadata is", originalDstKey)
			}
			format := child(entry, "format_string")
			if got := str(child(format, "text_format_source"), "inline_string"); got != extproc.EgressPassthroughDestinationFormat {
				t.Errorf("%s is set from %q, want %q", originalDstKey, got, extproc.EgressPassthroughDestinationFormat)
			}
			if omit, _ := format["omit_empty_values"].(bool); !omit {
				t.Errorf("%s format does not omit empty values; an absent answer would render as \"-\"", originalDstKey)
			}
			if skip, _ := entry["skip_if_empty"].(bool); !skip {
				t.Errorf("%s is not skip_if_empty; an absent answer would be written as an unparseable address", originalDstKey)
			}
			if str(entry, "shared_with_upstream") == "" {
				t.Errorf("%s is not shared with upstream; the passthrough chain would never see it", originalDstKey)
			}
		})
	}
}

// Every inner chain without an HCM is a passthrough chain: a plain tcp_proxy
// to the ORIGINAL_DST cluster, dialing the filter state the CONNECT leg's
// answer produced and nothing else. The plain gateway needs one per transport
// protocol; on sdsmint egress_tls_mitm claims tls, so only raw_buffer is left.
// See TestEgressManifestsClaimEveryTransportProtocol.
var wantPassthroughChains = map[string][]string{
	egressManifests[0]: {"egress_passthrough", "egress_tls_passthrough"},
	egressManifests[1]: {"egress_passthrough"},
}

func TestEgressManifestsPassthroughChainDialsOnlyTheDecidedAddress(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			var passthrough []string
			for _, lc := range allChains(tree) {
				if hcm(lc.chain) != nil || str(lc.listener, "name") == "egress" {
					continue
				}
				name := str(lc.chain, "name")
				filters := list(lc.chain, "filters")
				if len(filters) != 1 || str(filters[0], "name") != "envoy.filters.network.tcp_proxy" {
					t.Errorf("non-HTTP chain %q must be a single tcp_proxy, got %d filters", name, len(filters))
					continue
				}
				proxy := child(filters[0], "typed_config")
				if got := str(proxy, "cluster"); got != passthroughCluster {
					t.Errorf("chain %q tcp_proxy dials %q, want %q", name, got, passthroughCluster)
				}
				if proxy["tunneling_config"] != nil {
					t.Errorf("chain %q wraps the connection in a CONNECT; the passthrough chain relays bytes as they are", name)
				}
				passthrough = append(passthrough, name)
			}
			slices.Sort(passthrough)
			if want := wantPassthroughChains[path]; !slices.Equal(passthrough, want) {
				t.Fatalf("passthrough chains = %v, want %v", passthrough, want)
			}

			cluster := byName(clusters(tree), passthroughCluster)
			if cluster == nil {
				t.Fatalf("no %s cluster", passthroughCluster)
			}
			if got := str(cluster, "type"); got != "ORIGINAL_DST" {
				t.Errorf("%s is of type %q, want ORIGINAL_DST", passthroughCluster, got)
			}
			// The filter state is the one input. A header or metadata override
			// would be a second way to pick the address.
			if lb := child(cluster, "original_dst_lb_config"); len(lb) != 0 {
				t.Errorf("%s has original_dst_lb_config %v; the address must come from the filter state alone", passthroughCluster, lb)
			}
			if cluster["transport_socket"] != nil {
				t.Errorf("%s has a transport socket; the payload is the actor's own bytes and must not be wrapped", passthroughCluster)
			}
			for _, c := range clusters(tree) {
				if strings.Contains(mustJSON(t, c), "dynamic_forward_proxy") && !strings.Contains(mustJSON(t, c), `"typed_extension_protocol_options"`) {
					t.Errorf("cluster %q is a dynamic forward proxy without HTTP protocol options; a raw by-name dial has no leg to authorize it", str(c, "name"))
				}
			}
		})
	}
}

// Envoy buckets a listener's filter chains by transport protocol and never
// falls back out of a populated bucket. egress_cleartext claims raw_buffer
// with HTTP application protocols alone, so a chain that matches nothing is
// unreachable and every opaque or unclassified connection is closed as
// no_filter_chain_match, allowed or not.
//
// The inner listener's filters produce exactly two transport protocols: tls
// from tls_inspector, and raw_buffer for everything else, sniff timeout
// included. Each needs a chain with no application_protocols as its catch-all.
// Every ORIGINAL_DST cluster, the by-address routes' included, dials the
// filter state alone. A metadata_key, use_http_header or port_override would
// give a request a way to name an address other than the one the CONNECT leg
// allowed and the request legs policed.
func TestEgressManifestsOriginalDstClustersDialTheFilterStateAlone(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			found := 0
			for _, cluster := range clusters(tree) {
				if str(cluster, "type") != "ORIGINAL_DST" {
					continue
				}
				found++
				if lb := child(cluster, "original_dst_lb_config"); len(lb) != 0 {
					t.Errorf("cluster %q has original_dst_lb_config %v; the address must come from the filter state alone", str(cluster, "name"), lb)
				}
			}
			if found < 2 {
				t.Errorf("found %d ORIGINAL_DST clusters, want at least the passthrough and the by-address ones", found)
			}
		})
	}
}

func TestEgressManifestsClaimEveryTransportProtocol(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			for _, l := range listeners(bootstrapTree(t, path)) {
				if str(l, "name") == "egress" {
					continue
				}
				catchAll := map[string][]string{}
				for _, c := range list(l, "filter_chains") {
					name := str(c, "name")
					match := child(c, "filter_chain_match")
					transport := str(match, "transport_protocol")
					if transport == "" {
						t.Errorf("chain %q matches no transport protocol, so it is only reachable when no chain claims the connection's own; give it one", name)
						continue
					}
					if _, seen := catchAll[transport]; !seen {
						catchAll[transport] = nil
					}
					if len(strs(match, "application_protocols")) == 0 {
						catchAll[transport] = append(catchAll[transport], name)
					}
				}
				for _, transport := range []string{"tls", "raw_buffer"} {
					names, claimed := catchAll[transport]
					if !claimed {
						t.Errorf("listener %q has no chain matching transport protocol %q; a connection the listener filters classify that way is closed as no_filter_chain_match", str(l, "name"), transport)
						continue
					}
					if len(names) != 1 {
						t.Errorf("listener %q has %d chains matching %q with no application_protocols (%v), want exactly one to catch what the inspectors could not name", str(l, "name"), len(names), transport, names)
					}
				}
				for transport := range catchAll {
					if transport != "tls" && transport != "raw_buffer" {
						t.Errorf("listener %q has a chain matching transport protocol %q, which its listener filters never set", str(l, "name"), transport)
					}
				}
			}
		})
	}
}

// The identity crosses the inner hop as filter state, which internal_upstream
// copies from the options the connection pool was created with. A string
// object is not part of the pool key, so the outer HCM keys the pool per actor
// itself, and must not share that key upstream as an SNI override.
func TestEgressManifestsKeyTheInnerPoolPerActor(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var identity, poolKey node
			for _, f := range list(hcm(outerChain(t, bootstrapTree(t, path))), "http_filters") {
				if str(f, "name") != setFilterStateFilter {
					continue
				}
				for _, v := range list(child(f, "typed_config"), "on_request_headers") {
					switch str(v, "object_key") {
					case extproc.ActorIdentityFilterStateKey:
						identity = v
					case "envoy.network.upstream_server_name":
						poolKey = v
					}
				}
			}
			if identity == nil {
				t.Fatalf("the egress chain never sets %s", extproc.ActorIdentityFilterStateKey)
			}
			if got := str(identity, "shared_with_upstream"); got == "" {
				t.Errorf("%s is not shared with upstream; the inner legs would see no actor", extproc.ActorIdentityFilterStateKey)
			}
			if poolKey == nil {
				t.Fatal("the egress chain does not key the inner pool per actor (no envoy.network.upstream_server_name entry)")
			}
			if str(poolKey, "shared_with_upstream") != "" {
				t.Error("envoy.network.upstream_server_name must not be shared with upstream; it would override the SNI of re-originated TLS")
			}
			if !strings.Contains(mustJSON(t, poolKey), "DOWNSTREAM_PEER_URI_SAN") {
				t.Error("the pool key must derive from the verified peer certificate")
			}
		})
	}
}

// http_inspector steers HTTP/1.0 onto the cleartext chain; the codec has to
// accept it there, or those requests fail before any filter runs, unlogged.
func TestEgressManifestsCleartextAcceptsHTTP10(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			for _, lc := range allChains(bootstrapTree(t, path)) {
				if str(lc.chain, "name") != extproc.EgressCleartextFilterChainName {
					continue
				}
				if ok, _ := child(hcm(lc.chain), "http_protocol_options")["accept_http_10"].(bool); !ok {
					t.Errorf("%s does not accept HTTP/1.0", extproc.EgressCleartextFilterChainName)
				}
			}
		})
	}
}

// One tunnel per inner connection: the filter state internal_upstream copies
// belongs to the tunnel the connection was made for, and a reused connection
// would carry it into the next one.
func TestEgressManifestsNeverReuseATunnelConnection(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			for _, cluster := range clusters(bootstrapTree(t, path)) {
				if !strings.Contains(mustJSON(t, cluster), `"server_listener_name"`) {
					continue
				}
				if got := maxRequestsPerConnection(child(cluster, "typed_extension_protocol_options")); got != 1 {
					t.Errorf("internal cluster %q allows %v requests per connection, want 1", str(cluster, "name"), got)
				}
			}
		})
	}
}

// maxRequestsPerConnection reads the limit out of a cluster's
// HttpProtocolOptions, or 0 when there is none.
func maxRequestsPerConnection(opts node) float64 {
	for _, v := range opts {
		if m, ok := v.(node); ok {
			if n, ok := child(m, "common_http_protocol_options")["max_requests_per_connection"].(float64); ok {
				return n
			}
		}
	}
	return 0
}

// dialMatchOf returns the dial a route's match requires, or "" when it
// requires none.
func dialMatchOf(match node) string {
	for _, m := range list(match, "dynamic_metadata") {
		if str(m, "filter") != extproc.EgressMetadataNamespace {
			continue
		}
		if segs := list(m, "path"); len(segs) != 1 || str(segs[0], "key") != extproc.EgressDialKey {
			continue
		}
		return str(child(child(m, "value"), "string_match"), "exact")
	}
	return ""
}

// autoSNIAndSAN reports whether a cluster takes the SNI and the certificate
// check from the request's Host.
func autoSNIAndSAN(cluster node) bool {
	for _, v := range child(cluster, "typed_extension_protocol_options") {
		m, ok := v.(node)
		if !ok {
			continue
		}
		up := child(m, "upstream_http_protocol_options")
		sni, _ := up["auto_sni"].(bool)
		san, _ := up["auto_san_validation"].(bool)
		return sni && san
	}
	return false
}

// A request leg's answer picks its route: one route per dial and no default,
// so a request the sidecar did not answer for has no route. dial=name goes to
// a dynamic forward proxy cluster, which resolves the Host; dial=address to an
// ORIGINAL_DST cluster fed by the same filter state as the passthrough chains.
// On the MITM leg both re-originate TLS and verify the origin against the Host;
// on the cleartext leg neither wraps the actor's plaintext.
func TestEgressManifestsRequestLegsRouteByTheDial(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			all := clusters(tree)
			for _, lc := range allChains(tree) {
				name := str(lc.chain, "name")
				if !slices.Contains(requestLegs, name) {
					continue
				}
				dials := map[string]int{}
				for _, vh := range list(child(hcm(lc.chain), "route_config"), "virtual_hosts") {
					for _, r := range list(vh, "routes") {
						dial := dialMatchOf(child(r, "match"))
						clusterName := str(child(r, "route"), "cluster")
						cluster := byName(all, clusterName)
						if cluster == nil {
							t.Errorf("chain %q routes to cluster %q, which does not exist", name, clusterName)
							continue
						}
						dials[dial]++
						switch dial {
						case extproc.EgressDialName:
							if got := str(child(cluster, "cluster_type"), "name"); got != dfpClusterType {
								t.Errorf("chain %q sends dial=name to %q of type %q, want a dynamic forward proxy", name, clusterName, got)
							}
						case extproc.EgressDialAddress:
							if got := str(cluster, "type"); got != "ORIGINAL_DST" {
								t.Errorf("chain %q sends dial=address to %q of type %q, want ORIGINAL_DST", name, clusterName, got)
							}
							if lb := child(cluster, "original_dst_lb_config"); len(lb) != 0 {
								t.Errorf("cluster %q has original_dst_lb_config %v; the address must come from the filter state alone", clusterName, lb)
							}
						default:
							t.Errorf("chain %q has a route to %q that matches no dial; a request the sidecar did not answer for would take it", name, clusterName)
						}
						tls := cluster["transport_socket"] != nil
						if name == extproc.EgressTLSMITMFilterChainName && (!tls || !autoSNIAndSAN(cluster)) {
							t.Errorf("chain %q routes to %q, which does not re-originate TLS with the SNI and certificate check taken from the Host", name, clusterName)
						}
						if name == extproc.EgressCleartextFilterChainName && tls {
							t.Errorf("chain %q routes to %q, which wraps the actor's plaintext in TLS", name, clusterName)
						}
					}
				}
				for _, dial := range []string{extproc.EgressDialName, extproc.EgressDialAddress} {
					if dials[dial] == 0 {
						t.Errorf("chain %q has no route for dial=%s", name, dial)
					}
				}
			}
		})
	}
}

func mustJSON(t *testing.T, n node) string {
	t.Helper()
	b, err := yaml.Marshal(n)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	j, err := yaml.YAMLToJSON(b)
	if err != nil {
		t.Fatalf("to json: %v", err)
	}
	return string(j)
}

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
	extProcFilter                  = "envoy.filters.http.ext_proc"
	compositeFilter                = "envoy.filters.http.composite"
	setFilterStateFilter           = "envoy.filters.http.set_filter_state"
	extProcServerCluster           = "ext_proc_server"
	passthroughCluster             = "egress_tcp_passthrough"
	passthroughForwardProxyCluster = "egress_forward_proxy_passthrough"
	originalDstKey                 = "envoy.network.transport_socket.original_dst_address"
	dfpClusterType                 = "envoy.clusters.dynamic_forward_proxy"
)

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
		egressManifests[0]: {extproc.EgressFilterChainName, extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName},
	}
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var got []string
			for _, lc := range allChains(bootstrapTree(t, path)) {
				cfg, _, _ := extProcOf(lc.chain)
				if cfg == nil {
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

func TestEgressManifestsDisableWebSocketUpgrades(t *testing.T) {
	want := map[string][]string{
		egressManifests[0]: {extproc.EgressCleartextFilterChainName, extproc.EgressTLSMITMFilterChainName},
	}
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			chains := map[string]node{}
			for _, lc := range allChains(tree) {
				name := str(lc.chain, "name")
				if slices.Contains(requestLegs, name) {
					chains[name] = lc.chain
				}
			}
			for _, name := range want[path] {
				chain, present := chains[name]
				if !present {
					t.Errorf("expected request-leg chain %q is absent", name)
					continue
				}
				var disabled bool
				for _, upgrade := range list(hcm(chain), "upgrade_configs") {
					if str(upgrade, "upgrade_type") == "websocket" {
						enabled, ok := upgrade["enabled"].(bool)
						disabled = ok && !enabled
						break
					}
				}
				if !disabled {
					t.Errorf("chain %q must explicitly disable websocket upgrades", name)
				}
				var routes []node
				for _, virtualHost := range list(child(hcm(chain), "route_config"), "virtual_hosts") {
					routes = append(routes, list(virtualHost, "routes")...)
				}
				if len(routes) == 0 {
					t.Errorf("chain %q has no routes", name)
				}
				for i, route := range routes {
					action := child(route, "route")
					if action == nil {
						t.Errorf("chain %q route %d has no route action", name, i)
						continue
					}
					disabled := false
					for _, upgrade := range list(action, "upgrade_configs") {
						if str(upgrade, "upgrade_type") == "websocket" {
							enabled, ok := upgrade["enabled"].(bool)
							disabled = ok && !enabled
							break
						}
					}
					if !disabled {
						t.Errorf("chain %q route %d must explicitly disable websocket upgrades", name, i)
					}
				}
			}
		})
	}
}

// extProcOf returns the ext_proc filter config of a chain's HCM and its index
// among the http_filters, or nil and -1.
func extProcOf(chain node) (node, int, []node) {
	filters := list(hcm(chain), "http_filters")
	if i := filterIndex(filters, extProcFilter); i >= 0 {
		return child(filters[i], "typed_config"), i, filters
	}
	for i, f := range filters {
		if str(f, "name") != compositeFilter {
			continue
		}
		for _, m := range list(child(child(child(f, "typed_config"), "matcher"), "matcher_list"), "matchers") {
			inner := child(child(child(child(m, "on_match"), "action"), "typed_config"), "typed_config")
			if str(inner, "name") == extProcFilter {
				return child(inner, "typed_config"), i, filters
			}
		}
	}
	return nil, -1, filters
}

// Every ext_proc in the egress gateway fails closed, talks to the co-located
// sidecar, asks for the attributes its leg reads, and can rewrite nothing that
// routes. A missing attribute reads as "absent" and denies every request.
func TestEgressManifestsExtProcFilters(t *testing.T) {
	required := map[string][]string{
		extproc.EgressFilterChainName:          {extproc.FilterChainNameAttribute},
		extproc.EgressCleartextFilterChainName: {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.ConnectAuthorityFilterStateAttribute, extproc.OriginalDstIPAttribute, extproc.OriginalDstPortAttribute},
		extproc.EgressTLSMITMFilterChainName:   {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.ConnectAuthorityFilterStateAttribute, extproc.OriginalDstIPAttribute, extproc.OriginalDstPortAttribute},
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
				t.Errorf("found %d ORIGINAL_DST clusters, want at least the by-address ones", found)
			}
		})
	}
}

// The identity and the dialed authority cross the inner hop as filter state
// shared with the upstream; nothing else carries them.
func TestEgressManifestsShareIdentityWithTheInnerListener(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			var identity, authority node
			for _, f := range list(hcm(outerChain(t, bootstrapTree(t, path))), "http_filters") {
				if str(f, "name") != setFilterStateFilter {
					continue
				}
				for _, v := range list(child(f, "typed_config"), "on_request_headers") {
					switch str(v, "object_key") {
					case extproc.ActorIdentityFilterStateKey:
						identity = v
					case extproc.ConnectAuthorityFilterStateKey:
						authority = v
					}
				}
			}
			if identity == nil {
				t.Fatalf("the egress chain never sets %s", extproc.ActorIdentityFilterStateKey)
			}
			if got := str(identity, "shared_with_upstream"); got == "" {
				t.Errorf("%s is not shared with upstream; the inner legs would see no actor", extproc.ActorIdentityFilterStateKey)
			}
			// The dialed port rides along the same way; without it the request
			// legs cannot enforce a rule's ports.
			if authority == nil {
				t.Fatalf("the egress chain never sets %s", extproc.ConnectAuthorityFilterStateKey)
			}
			if got := str(authority, "shared_with_upstream"); got == "" {
				t.Errorf("%s is not shared with upstream; the inner legs would see no dialed port", extproc.ConnectAuthorityFilterStateKey)
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

// One pool per tunnel: internal_upstream copies the filter state of the tunnel
// that created the pool, not of the tunnel asking for a connection, and a
// string object is not part of the pool key. A pool per downstream connection
// is a pool per tunnel only while that connection carries one CONNECT. A
// connection its tunnel left behind keeps its pool alive until the idle
// timeout, which must not be disabled.
func TestEgressManifestsGiveEachTunnelItsOwnInnerPool(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			tree := bootstrapTree(t, path)
			internal := 0
			for _, cluster := range clusters(tree) {
				if !strings.Contains(mustJSON(t, cluster), `"server_listener_name"`) {
					continue
				}
				internal++
				if ok, _ := cluster["connection_pool_per_downstream_connection"].(bool); !ok {
					t.Errorf("internal cluster %q shares its pool across tunnels; want connection_pool_per_downstream_connection: true", str(cluster, "name"))
				}
				tcp := child(child(cluster, "typed_extension_protocol_options"), "envoy.extensions.upstreams.tcp.v3.TcpProtocolOptions")
				if got := str(tcp, "idle_timeout"); got == "" || strings.Trim(got, "0.s") == "" {
					t.Errorf("internal cluster %q idle_timeout is %q; want a non-zero timeout so a pool its tunnel left behind is freed", str(cluster, "name"), got)
				}
			}
			if internal == 0 {
				t.Fatal("no cluster targets an internal listener")
			}
			if got := str(hcm(outerChain(t, tree)), "codec_type"); got != "HTTP1" {
				t.Errorf("egress chain codec_type is %q, want HTTP1: any other codec can carry several CONNECTs on one downstream connection, and they would share a pool", got)
			}
		})
	}
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

// requiresDialHost reports whether a route matches only when the answer
// carries the name to dial.
func requiresDialHost(match node) bool {
	for _, m := range list(match, "dynamic_metadata") {
		if str(m, "filter") != extproc.EgressMetadataNamespace {
			continue
		}
		if segs := list(m, "path"); len(segs) != 1 || str(segs[0], "key") != extproc.EgressDialHostKey {
			continue
		}
		if present, _ := child(m, "value")["present_match"].(bool); present {
			return true
		}
	}
	return false
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
// a dynamic forward proxy cluster, and only with the answered name it dials;
// dial=address to an ORIGINAL_DST cluster fed by the same filter state as the
// passthrough chains. On the MITM leg both re-originate TLS and verify the
// origin against the Host; on the cleartext leg neither wraps the actor's
// plaintext.
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
							if !requiresDialHost(child(r, "match")) {
								t.Errorf("chain %q sends dial=name to %q without requiring %s:%s; an answer without the name would dial the Host's port", name, clusterName, extproc.EgressMetadataNamespace, extproc.EgressDialHostKey)
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

// Each request leg dials the name its answer carries, on the port the actor
// dialed: a set_filter_state between ext_proc and the forward proxy filter
// copies the answer into the dynamic host, which the forward proxy cluster
// dials in place of the Host. Without it, the cluster dials a port in the Host
// that no rule checked. The filter resolves that same entry ahead of the
// cluster only with allow_dynamic_host_from_filter_state; without it, it
// resolves the Host as sent, into the DNS cache all actors share.
func TestEgressManifestsRequestLegsDialTheAnsweredName(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	if writers := filterStateWriters(tree, extproc.UpstreamDynamicHostFilterStateKey); len(writers) != len(requestLegs) {
		t.Errorf("%s is set by %d filters, want one per request leg", extproc.UpstreamDynamicHostFilterStateKey, len(writers))
	}
	for _, leg := range requestLegs {
		chain := byName(list(mitmListener(t, tree), "filter_chains"), leg)
		if chain == nil {
			t.Fatalf("no %q chain on mitm_listener", leg)
		}
		_, extProcAt, filters := extProcOf(chain)
		forwardProxyAt := filterIndex(filters, "envoy.filters.http.dynamic_forward_proxy")
		if forwardProxyAt < 0 {
			t.Errorf("chain %q has no dynamic_forward_proxy filter", leg)
			continue
		}
		if allow, _ := child(filters[forwardProxyAt], "typed_config")["allow_dynamic_host_from_filter_state"].(bool); !allow {
			t.Errorf("chain %q's dynamic_forward_proxy resolves the Host as sent, not %s, so every port and spelling of a Host takes a DNS cache slot", leg, extproc.UpstreamDynamicHostFilterStateKey)
		}
		var entry node
		at := -1
		for i, f := range filters {
			if str(f, "name") != setFilterStateFilter {
				continue
			}
			for _, v := range list(child(f, "typed_config"), "on_request_headers") {
				if str(v, "object_key") == extproc.UpstreamDynamicHostFilterStateKey {
					entry, at = v, i
				}
			}
		}
		if at < 0 {
			t.Errorf("chain %q does not set %s", leg, extproc.UpstreamDynamicHostFilterStateKey)
			continue
		}
		if at < extProcAt || at > forwardProxyAt {
			t.Errorf("chain %q sets %s at http_filters[%d], want it between ext_proc at [%d] and dynamic_forward_proxy at [%d]", leg, extproc.UpstreamDynamicHostFilterStateKey, at, extProcAt, forwardProxyAt)
		}
		if got := str(child(child(entry, "format_string"), "text_format_source"), "inline_string"); got != extproc.EgressDialHostFormat {
			t.Errorf("chain %q sets %s from %q, want %q", leg, extproc.UpstreamDynamicHostFilterStateKey, got, extproc.EgressDialHostFormat)
		}
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

// egressManifest is the envoy gateway, which runs the egress-policy module.
var egressManifest = egressManifests[0]

// mitmListener returns the egress manifest's inner listener.
func mitmListener(t *testing.T, tree node) node {
	t.Helper()
	l := byName(listeners(tree), "mitm_listener")
	if l == nil {
		t.Fatal("no mitm_listener in the egress manifest")
	}
	return l
}

// The matcher must select on the module's verdict and map every verdict, with
// "denied" mapping to no chain. The module runs after both inspectors, and no
// chain keeps a filter_chain_match, which Envoy ignores once a matcher is set.
func TestEgressManifestsInnerListenerSelectsOnTheModuleVerdict(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	l := mitmListener(t, tree)

	filters := list(l, "listener_filters")
	module := filterIndex(filters, "envoy.filters.listener.dynamic_modules")
	if module < 0 {
		t.Fatal("mitm_listener has no dynamic_modules listener filter; nothing would write the verdict")
	}
	for _, inspector := range []string{"envoy.filters.listener.tls_inspector", "envoy.filters.listener.http_inspector"} {
		if i := filterIndex(filters, inspector); i < 0 || i > module {
			t.Errorf("%s is at listener_filters[%d], the egress-policy module at [%d]; the module would see no transport protocol or SNI", inspector, i, module)
		}
	}

	matcherTree := child(child(l, "filter_chain_matcher"), "matcher_tree")
	if got := str(child(child(matcherTree, "input"), "typed_config"), "key"); got != extproc.EgressFilterChainFilterStateKey {
		t.Errorf("filter_chain_matcher keys on filter state %q, want %q", got, extproc.EgressFilterChainFilterStateKey)
	}
	actions := child(child(matcherTree, "exact_match_map"), "map")

	chains := map[string]bool{}
	for _, c := range list(l, "filter_chains") {
		chains[str(c, "name")] = true
		if child(c, "filter_chain_match") != nil {
			t.Errorf("chain %q has a filter_chain_match, which Envoy ignores when the listener has a filter_chain_matcher", str(c, "name"))
		}
	}
	for verdict, want := range map[string]string{
		extproc.EgressFilterChainMITM:        extproc.EgressTLSMITMFilterChainName,
		extproc.EgressFilterChainCleartext:   extproc.EgressCleartextFilterChainName,
		extproc.EgressFilterChainPassthrough: "egress_passthrough",
	} {
		got := str(child(child(child(actions, verdict), "action"), "typed_config"), "value")
		if got != want {
			t.Errorf("verdict %q selects chain %q, want %q", verdict, got, want)
		}
		if !chains[got] {
			t.Errorf("verdict %q selects chain %q, which the listener does not have; the connection would be closed", verdict, got)
		}
	}
	if action := child(child(actions, extproc.EgressFilterChainDenied), "action"); action != nil {
		if got := str(child(action, "typed_config"), "value"); chains[got] {
			t.Errorf("verdict %q selects chain %q; a denied connection must match no chain", extproc.EgressFilterChainDenied, got)
		}
	}
}

// The outer ext_proc must accept dev.ate.policy.egress, and the outer chain
// must copy it after ext_proc into shared filter state for the module.
func TestEgressManifestsConnectLegHandsTheEgressRulesToTheInnerListener(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	outer := outerChain(t, tree)
	cfg, extProcAt, filters := extProcOf(outer)
	if cfg == nil {
		t.Fatalf("chain %q has no ext_proc filter", extproc.EgressFilterChainName)
	}
	admitted := strs(child(child(cfg, "metadata_options"), "receiving_namespaces"), "untyped")
	if !slices.Contains(admitted, extproc.EgressPolicyMetadataNamespace) {
		t.Errorf("the CONNECT leg's ext_proc does not admit dynamic metadata in %q; the egress rules would be dropped and every ClientHello denied", extproc.EgressPolicyMetadataNamespace)
	}

	writers := filterStateWriters(tree, extproc.EgressPolicyMetadataNamespace)
	if len(writers) != 1 {
		t.Fatalf("%s is set by %d filters, want exactly the CONNECT leg's copy of its answer", extproc.EgressPolicyMetadataNamespace, len(writers))
	}
	entry := writers[0]
	at := -1
	for i, f := range filters {
		if str(f, "name") != setFilterStateFilter {
			continue
		}
		for _, v := range list(child(f, "typed_config"), "on_request_headers") {
			if str(v, "object_key") == extproc.EgressPolicyMetadataNamespace {
				at = i
			}
		}
	}
	if at < 0 {
		t.Fatalf("the writer of %s is not on the outer chain, where the answer is", extproc.EgressPolicyMetadataNamespace)
	}
	if at < extProcAt {
		t.Errorf("%s is set at http_filters[%d], before ext_proc at [%d]; the metadata it reads does not exist yet", extproc.EgressPolicyMetadataNamespace, at, extProcAt)
	}
	format := child(entry, "format_string")
	if got := str(child(format, "text_format_source"), "inline_string"); got != extproc.EgressPolicyMetadataFormat {
		t.Errorf("%s is set from %q, want %q", extproc.EgressPolicyMetadataNamespace, got, extproc.EgressPolicyMetadataFormat)
	}
	if got := str(entry, "factory_key"); got != "envoy.string" {
		t.Errorf("%s uses factory %q, want envoy.string, the string accessor the module reads", extproc.EgressPolicyMetadataNamespace, got)
	}
	if skip, _ := entry["skip_if_empty"].(bool); !skip {
		t.Errorf("%s is not skip_if_empty; an absent answer would be written as unparseable JSON", extproc.EgressPolicyMetadataNamespace)
	}
	if str(entry, "shared_with_upstream") == "" {
		t.Errorf("%s is not shared with upstream; the inner listener would never see it", extproc.EgressPolicyMetadataNamespace)
	}
}

// Denied connections match no chain, so the listener access log is their only
// record. It must log only NR connections and include the SNI, actor, and
// verdict.
func TestEgressManifestsInnerListenerLogsDeniedConnections(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	logs := list(mitmListener(t, tree), "access_log")
	if len(logs) == 0 {
		t.Fatal("mitm_listener has no access_log; a ClientHello the module denies would leave no record")
	}
	for i, al := range logs {
		if flags := strs(child(child(al, "filter"), "response_flag_filter"), "flags"); !slices.Contains(flags, "NR") {
			t.Errorf("access_log[%d] is not confined to response flag NR (got %v); it would log every connection the chains already log", i, flags)
		}
		format := child(child(child(al, "typed_config"), "log_format"), "json_format")
		for _, want := range []string{
			"%REQUESTED_SERVER_NAME%",
			"%FILTER_STATE(" + extproc.ActorIdentityFilterStateKey + ":PLAIN)%",
			"%FILTER_STATE(" + extproc.EgressFilterChainFilterStateKey + ":PLAIN)%",
		} {
			found := false
			for _, v := range format {
				if s, ok := v.(string); ok && strings.Contains(s, want) {
					found = true
				}
			}
			if !found {
				t.Errorf("access_log[%d] does not log %s", i, want)
			}
		}
	}
}

// The sdsmint passthrough chain relays TLS unread to the resolved SNI on the
// port the actor dialed: sni_dynamic_forward_proxy resolves the name and
// tcp_proxy dials it through a raw forward-proxy cluster. The address the
// actor dialed must have no way in, and only this chain may dial the raw
// cluster, since nothing else authorizes a by-name dial without a request leg.
func TestEgressManifestsPassthroughChainDialsTheResolvedSNI(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	chain := byName(list(mitmListener(t, tree), "filter_chains"), "egress_passthrough")
	if chain == nil {
		t.Fatal("no egress_passthrough chain on mitm_listener")
	}
	filters := list(chain, "filters")
	if len(filters) != 2 || str(filters[0], "name") != "envoy.filters.network.sni_dynamic_forward_proxy" || str(filters[1], "name") != "envoy.filters.network.tcp_proxy" {
		names := make([]string, len(filters))
		for i, f := range filters {
			names[i] = str(f, "name")
		}
		t.Fatalf("egress_passthrough filters = %v, want sni_dynamic_forward_proxy then tcp_proxy", names)
	}
	sni := child(filters[0], "typed_config")
	proxy := child(filters[1], "typed_config")
	if got := str(proxy, "cluster"); got != passthroughForwardProxyCluster {
		t.Errorf("egress_passthrough tcp_proxy dials %q, want %q", got, passthroughForwardProxyCluster)
	}
	if proxy["tunneling_config"] != nil {
		t.Error("egress_passthrough wraps the connection in a CONNECT; it relays bytes as they are")
	}

	cluster := byName(clusters(tree), passthroughForwardProxyCluster)
	if cluster == nil {
		t.Fatalf("no %s cluster", passthroughForwardProxyCluster)
	}
	clusterType := child(child(cluster, "cluster_type"), "typed_config")
	if !strings.Contains(str(clusterType, "@type"), "dynamic_forward_proxy") {
		t.Errorf("%s is not a dynamic forward proxy: %v", passthroughForwardProxyCluster, clusterType["@type"])
	}
	// Envoy rejects two differing configs of one DNS cache, so compare them whole.
	if want, got := mustJSON(t, child(sni, "dns_cache_config")), mustJSON(t, child(clusterType, "dns_cache_config")); want != got || want == "null" {
		t.Errorf("sni_dynamic_forward_proxy dns_cache_config %s differs from the cluster's %s", want, got)
	}
	if cluster["transport_socket"] != nil {
		t.Errorf("%s has a transport socket; the payload is the actor's own TLS and must not be wrapped", passthroughForwardProxyCluster)
	}
	if cluster["typed_extension_protocol_options"] != nil {
		t.Errorf("%s has HTTP protocol options; nothing on the passthrough chain is HTTP", passthroughForwardProxyCluster)
	}

	// Only the passthrough chain may dial the raw forward proxy, and it is the
	// only raw one.
	for _, lc := range allChains(tree) {
		if str(lc.chain, "name") == "egress_passthrough" {
			continue
		}
		if strings.Contains(mustJSON(t, lc.chain), passthroughForwardProxyCluster) {
			t.Errorf("chain %q references %s; only egress_passthrough may dial by name without a request leg", str(lc.chain, "name"), passthroughForwardProxyCluster)
		}
	}
	for _, c := range clusters(tree) {
		if name := str(c, "name"); name != passthroughForwardProxyCluster && strings.Contains(mustJSON(t, c), "dynamic_forward_proxy") && !strings.Contains(mustJSON(t, c), `"typed_extension_protocol_options"`) {
			t.Errorf("cluster %q is a dynamic forward proxy without HTTP protocol options; a raw by-name dial has no leg to authorize it", name)
		}
	}

	// Nothing may dial the address the actor chose.
	for _, c := range clusters(tree) {
		if str(c, "type") == "ORIGINAL_DST" && str(c, "name") == passthroughCluster {
			t.Errorf("cluster %q still exists; the passthrough chain must dial the resolved SNI, not the actor's address", passthroughCluster)
		}
	}
	if writers := filterStateWriters(tree, originalDstKey); len(writers) != 0 {
		t.Errorf("%s is written by %d filters; the gateway must not carry the address the actor dialed to the inner listener", originalDstKey, len(writers))
	}
}

// The dialed port reaches the passthrough chain as filter state the outer
// chain copies from the CONNECT leg's answer, after ext_proc and from nothing
// else. The forward proxy falls back to its configured port when the state is
// absent, so a missing port would silently redirect the connection.
func TestEgressManifestsConnectLegHandsTheDialedPortToTheInnerListener(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	writers := filterStateWriters(tree, extproc.UpstreamDynamicPortFilterStateKey)
	if len(writers) != 1 {
		t.Fatalf("%s is set by %d filters, want exactly the CONNECT leg's copy of its answer", extproc.UpstreamDynamicPortFilterStateKey, len(writers))
	}
	entry := writers[0]
	_, extProcAt, filters := extProcOf(outerChain(t, tree))
	at := -1
	for i, f := range filters {
		if str(f, "name") != setFilterStateFilter {
			continue
		}
		for _, v := range list(child(f, "typed_config"), "on_request_headers") {
			if str(v, "object_key") == extproc.UpstreamDynamicPortFilterStateKey {
				at = i
			}
		}
	}
	if at < 0 {
		t.Fatalf("the writer of %s is not on the outer chain, where the answer is", extproc.UpstreamDynamicPortFilterStateKey)
	}
	if at < extProcAt {
		t.Errorf("%s is set at http_filters[%d], before ext_proc at [%d]; the metadata it reads does not exist yet", extproc.UpstreamDynamicPortFilterStateKey, at, extProcAt)
	}
	if got := str(child(child(entry, "format_string"), "text_format_source"), "inline_string"); got != extproc.EgressDialedPortFormat {
		t.Errorf("%s is set from %q, want %q", extproc.UpstreamDynamicPortFilterStateKey, got, extproc.EgressDialedPortFormat)
	}
	if got := str(entry, "factory_key"); got != "" {
		t.Errorf("%s names factory %q; the factory of the same name is what parses the port", extproc.UpstreamDynamicPortFilterStateKey, got)
	}
	if str(entry, "shared_with_upstream") == "" {
		t.Errorf("%s is not shared with upstream; the passthrough chain would dial its fallback port", extproc.UpstreamDynamicPortFilterStateKey)
	}
}

func TestEgressManifestsRequestLegsSkipExtProcOnFilterState(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	for _, leg := range requestLegs {
		chain := byName(list(mitmListener(t, tree), "filter_chains"), leg)
		if chain == nil {
			t.Fatalf("no %q chain on mitm_listener", leg)
		}
		filters := list(hcm(chain), "http_filters")
		pepAt := filterIndex(filters, "envoy.filters.http.dynamic_modules")
		compAt := filterIndex(filters, compositeFilter)
		if compAt < 0 {
			t.Fatalf("chain %q has no %s filter", leg, compositeFilter)
		}
		if pepAt < 0 || pepAt > compAt {
			t.Errorf("chain %q runs %s at [%d] and dynamic_modules at [%d]; the PEP module must run before composite evaluates filter state", leg, compositeFilter, compAt, pepAt)
		}
		matchers := list(child(child(child(filters[compAt], "typed_config"), "matcher"), "matcher_list"), "matchers")
		if len(matchers) != 1 {
			t.Fatalf("chain %q composite filter has %d matchers, want 1", leg, len(matchers))
		}
		single := child(child(child(matchers[0], "predicate"), "not_matcher"), "single_predicate")
		if got := str(child(child(single, "input"), "typed_config"), "key"); got != "dev.ate.policy.egress.skip_callout" {
			t.Errorf("chain %q composite matcher keys on filter state %q, want %q", leg, got, "dev.ate.policy.egress.skip_callout")
		}
		if got := str(child(single, "value_match"), "exact"); got != "true" {
			t.Errorf("chain %q composite matcher value_match.exact = %q, want %q", leg, got, "true")
		}
		inner := child(child(child(child(matchers[0], "on_match"), "action"), "typed_config"), "typed_config")
		if got := str(inner, "name"); got != extProcFilter {
			t.Errorf("chain %q composite action filter = %q, want %q", leg, got, extProcFilter)
		}
	}
}

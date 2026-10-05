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

// These tests pin the egress manifest to the Go constants the handler
// dispatches on and answers with. A chain renamed on one side only fails
// closed at runtime, and nothing else in the tree would catch it.

const (
	extProcFilter                  = "envoy.filters.http.ext_proc"
	setFilterStateFilter           = "envoy.filters.http.set_filter_state"
	extProcServerCluster           = "ext_proc_server"
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
// leg, and the manifest must have exactly the legs its topology implies.
func TestEgressManifestsNameEveryExtProcChain(t *testing.T) {
	want := []string{extproc.EgressFilterChainName, extproc.EgressTLSMITMFilterChainName, extproc.EgressCleartextFilterChainName}
	var got []string
	for _, lc := range allChains(bootstrapTree(t, egressManifest)) {
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
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("ext_proc-calling filter chains = %v, want %v", got, want)
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
		extproc.EgressCleartextFilterChainName: {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.ConnectAuthorityFilterStateAttribute},
		extproc.EgressTLSMITMFilterChainName:   {extproc.FilterChainNameAttribute, extproc.ActorIdentityFilterStateAttribute, extproc.ConnectAuthorityFilterStateAttribute, extproc.RequestedServerNameAttribute},
	}
	prefixes := map[string]string{} // stat_prefix -> chain
	for _, lc := range allChains(bootstrapTree(t, egressManifest)) {
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
		// CONNECT leg's dialed port or a request leg's dial, and Envoy
		// drops metadata from a namespace the filter does not admit.
		admitted := strs(child(child(cfg, "metadata_options"), "receiving_namespaces"), "untyped")
		if !slices.Contains(admitted, extproc.EgressMetadataNamespace) {
			t.Errorf("chain %q ext_proc does not admit dynamic metadata in %q; its answer would be dropped", name, extproc.EgressMetadataNamespace)
		}
	}
}

// The identity crosses the inner hop as filter state, which internal_upstream
// copies from the options the connection pool was created with. A string
// object is not part of the pool key, so the outer HCM keys the pool per actor
// itself, and must not share that key upstream as an SNI override.
func TestEgressManifestsKeyTheInnerPoolPerActor(t *testing.T) {
	var identity, authority, poolKey node
	for _, f := range list(hcm(outerChain(t, bootstrapTree(t, egressManifest))), "http_filters") {
		if str(f, "name") != setFilterStateFilter {
			continue
		}
		for _, v := range list(child(f, "typed_config"), "on_request_headers") {
			switch str(v, "object_key") {
			case extproc.ActorIdentityFilterStateKey:
				identity = v
			case extproc.ConnectAuthorityFilterStateKey:
				authority = v
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
	// The dialed port rides along the same way; without it the request
	// legs cannot enforce a rule's ports.
	if authority == nil {
		t.Fatalf("the egress chain never sets %s", extproc.ConnectAuthorityFilterStateKey)
	}
	if got := str(authority, "shared_with_upstream"); got == "" {
		t.Errorf("%s is not shared with upstream; the inner legs would see no dialed port", extproc.ConnectAuthorityFilterStateKey)
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
}

// http_inspector steers HTTP/1.0 onto the cleartext chain; the codec has to
// accept it there, or those requests fail before any filter runs, unlogged.
func TestEgressManifestsCleartextAcceptsHTTP10(t *testing.T) {
	for _, lc := range allChains(bootstrapTree(t, egressManifest)) {
		if str(lc.chain, "name") != extproc.EgressCleartextFilterChainName {
			continue
		}
		if ok, _ := child(hcm(lc.chain), "http_protocol_options")["accept_http_10"].(bool); !ok {
			t.Errorf("%s does not accept HTTP/1.0", extproc.EgressCleartextFilterChainName)
		}
	}
}

// One tunnel per inner connection: the filter state internal_upstream copies
// belongs to the tunnel the connection was made for, and a reused connection
// would carry it into the next one.
func TestEgressManifestsNeverReuseATunnelConnection(t *testing.T) {
	for _, cluster := range clusters(bootstrapTree(t, egressManifest)) {
		if !strings.Contains(mustJSON(t, cluster), `"server_listener_name"`) {
			continue
		}
		if got := maxRequestsPerConnection(child(cluster, "typed_extension_protocol_options")); got != 1 {
			t.Errorf("internal cluster %q allows %v requests per connection, want 1", str(cluster, "name"), got)
		}
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

// A request leg's answer picks its route: every route matches dial=name and
// there is no default, so a request the sidecar did not answer for has no
// route. The route goes to a dynamic forward proxy cluster, which resolves
// the Host. On the MITM leg it re-originates TLS and verifies the origin
// against the Host; on the cleartext leg it does not wrap the actor's
// plaintext.
func TestEgressManifestsRequestLegsRouteByTheDial(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	all := clusters(tree)
	for _, lc := range allChains(tree) {
		name := str(lc.chain, "name")
		if !slices.Contains(requestLegs, name) {
			continue
		}
		routes := 0
		for _, vh := range list(child(hcm(lc.chain), "route_config"), "virtual_hosts") {
			for _, r := range list(vh, "routes") {
				clusterName := str(child(r, "route"), "cluster")
				cluster := byName(all, clusterName)
				if cluster == nil {
					t.Errorf("chain %q routes to cluster %q, which does not exist", name, clusterName)
					continue
				}
				routes++
				if dial := dialMatchOf(child(r, "match")); dial != extproc.EgressDialName {
					t.Errorf("chain %q has a route to %q matching dial=%q, want %q; a request the sidecar did not answer for would take it", name, clusterName, dial, extproc.EgressDialName)
				}
				if got := str(child(cluster, "cluster_type"), "name"); got != dfpClusterType {
					t.Errorf("chain %q sends dial=name to %q of type %q, want a dynamic forward proxy", name, clusterName, got)
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
		if routes == 0 {
			t.Errorf("chain %q has no route for dial=%s", name, extproc.EgressDialName)
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
func TestEgressManifestsConnectLegHandsTheSNIRulesToTheInnerListener(t *testing.T) {
	tree := bootstrapTree(t, egressManifest)
	outer := outerChain(t, tree)
	cfg, extProcAt, filters := extProcOf(outer)
	if cfg == nil {
		t.Fatalf("chain %q has no ext_proc filter", extproc.EgressFilterChainName)
	}
	admitted := strs(child(child(cfg, "metadata_options"), "receiving_namespaces"), "untyped")
	if !slices.Contains(admitted, extproc.EgressPolicyMetadataNamespace) {
		t.Errorf("the CONNECT leg's ext_proc does not admit dynamic metadata in %q; the SNI rules would be dropped and every ClientHello denied", extproc.EgressPolicyMetadataNamespace)
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
		if str(c, "type") == "ORIGINAL_DST" {
			t.Errorf("cluster %q is ORIGINAL_DST; the gateway dials only names it resolved, never the actor's address", str(c, "name"))
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

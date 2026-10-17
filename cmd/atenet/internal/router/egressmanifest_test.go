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
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/yaml"
)

// egressManifests is the envoy egress gateway ate-setup installs, which
// terminates and re-originates the tunneled TLS.
var egressManifests = []string{
	"../../../../manifests/ate-install/atenet-egress.yaml",
}

// TestEgressManifestsDisableTheConnectTimeout is the static-config half of
// TestBuildConnectRoutes_DisablesTimeout: Envoy applies a route's timeout to a
// CONNECT tunnel's whole lifetime rather than to its headers, so a route left
// at Envoy's 15s default caps how long any actor's outbound connection may
// exist -- streaming responses, long downloads and SSH sessions all die
// mid-stream at 15s. These manifests are what the gateway actually runs, and
// nothing else in the tree notices when one of them loses the line.
func TestEgressManifestsDisableTheConnectTimeout(t *testing.T) {
	for _, path := range egressManifests {
		t.Run(path, func(t *testing.T) {
			routes := connectRoutes(t, envoyConfig(t, path))
			if len(routes) == 0 {
				t.Fatal("found no connect_matcher route; the manifest changed shape and this test is checking nothing")
			}
			for _, r := range routes {
				if r.Route.Timeout == nil {
					t.Errorf("connect_matcher route to cluster %q sets no timeout, so it falls back to Envoy's 15s default and cuts every tunnel after 15s", r.Route.Cluster)
					continue
				}
				d, err := time.ParseDuration(*r.Route.Timeout)
				if err != nil {
					t.Errorf("connect_matcher route to cluster %q has timeout %q, which is not a duration: %v", r.Route.Cluster, *r.Route.Timeout, err)
					continue
				}
				if d != 0 {
					t.Errorf("connect_matcher route to cluster %q has timeout %s; it must be 0 to disable it", r.Route.Cluster, d)
				}
			}
		})
	}
}

// envoyRoute is the sliver of an Envoy bootstrap this test reads. Unnamed
// fields are dropped by the decoder, so the manifests stay free to grow.
type envoyRoute struct {
	Match struct {
		ConnectMatcher *struct{} `json:"connect_matcher"`
	} `json:"match"`
	Route struct {
		Cluster string  `json:"cluster"`
		Timeout *string `json:"timeout"`
	} `json:"route"`
}

type envoyBootstrap struct {
	StaticResources struct {
		Listeners []struct {
			FilterChains []struct {
				Filters []struct {
					TypedConfig struct {
						RouteConfig struct {
							VirtualHosts []struct {
								Routes []envoyRoute `json:"routes"`
							} `json:"virtual_hosts"`
						} `json:"route_config"`
					} `json:"typed_config"`
				} `json:"filters"`
			} `json:"filter_chains"`
		} `json:"listeners"`
	} `json:"static_resources"`
}

// connectRoutes is every route in the bootstrap matched by connect_matcher.
func connectRoutes(t *testing.T, raw string) []envoyRoute {
	t.Helper()
	var bootstrap envoyBootstrap
	if err := yaml.Unmarshal([]byte(raw), &bootstrap); err != nil {
		t.Fatalf("parsing envoy.yaml: %v", err)
	}
	var out []envoyRoute
	for _, l := range bootstrap.StaticResources.Listeners {
		for _, fc := range l.FilterChains {
			for _, f := range fc.Filters {
				for _, vh := range f.TypedConfig.RouteConfig.VirtualHosts {
					for _, r := range vh.Routes {
						if r.Match.ConnectMatcher != nil {
							out = append(out, r)
						}
					}
				}
			}
		}
	}
	return out
}

// envoyConfig is the envoy.yaml the atenet-egress ConfigMap in path ships.
func envoyConfig(t *testing.T, path string) string {
	t.Helper()
	return envoyConfigFrom(t, path, "atenet-egress")
}

// envoyConfigFrom is the envoy.yaml the ConfigMap named configMap in path ships.
func envoyConfigFrom(t *testing.T, path, configMap string) string {
	t.Helper()
	manifest, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	for _, doc := range strings.Split(string(manifest), "\n---\n") {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("parsing a document of %s: %v", path, err)
		}
		if obj.Kind != "ConfigMap" || obj.Metadata.Name != configMap {
			continue
		}
		envoyYaml, ok := obj.Data["envoy.yaml"]
		if !ok {
			t.Fatalf("the %s ConfigMap in %s has no envoy.yaml key", configMap, path)
		}
		return envoyYaml
	}
	t.Fatalf("%s has no ConfigMap named %s", path, configMap)
	return ""
}

// wantSampledLogFilter is the filter on the access logs of the per-request legs.
const wantSampledLogFilter = `
or_filter:
  filters:
  - status_code_filter:
      comparison:
        op: GE
        value: {default_value: 400, runtime_key: access_log.min_status}
  - response_flag_filter: {}
  - and_filter:
      filters:
      - header_filter:
          header:
            name: content-type
            string_match: {prefix: application/grpc}
      - grpc_status_filter: {exclude: true, statuses: [OK]}
  - runtime_filter:
      runtime_key: access_log.sample
      percent_sampled: {numerator: 1, denominator: HUNDRED}
      use_independent_randomness: true
`

// Every actor request lands in the mitm or cleartext access log, so both keep
// failures and a sample of the rest behind runtime keys an operator can flip.
// No other filter chain's access log may filter, since each is the only record
// of its events.
func TestEgressManifestsSampleThePerRequestAccessLogs(t *testing.T) {
	t.Parallel()
	var want node
	if err := yaml.Unmarshal([]byte(wantSampledLogFilter), &want); err != nil {
		t.Fatalf("parsing wantSampledLogFilter: %v", err)
	}
	logs := chainAccessLogsByLeg(bootstrapTree(t, egressManifest))
	sampledLegs := []string{"mitm", "cleartext"}
	for _, leg := range sampledLegs {
		t.Run(leg, func(t *testing.T) {
			t.Parallel()
			if len(logs[leg]) == 0 {
				t.Fatalf("no access log with leg %q; the manifest changed shape and this test is checking nothing", leg)
			}
			for _, al := range logs[leg] {
				if diff := cmp.Diff(want, child(al, "filter")); diff != "" {
					t.Errorf("access log %q filter mismatch (-want +got):\n%s", leg, diff)
				}
			}
		})
	}
	for leg, als := range logs {
		if slices.Contains(sampledLegs, leg) {
			continue
		}
		for _, al := range als {
			if _, ok := al["filter"]; ok {
				t.Errorf("access log %q has a filter; only %v may", leg, sampledLegs)
			}
		}
	}
}

// chainAccessLogsByLeg groups the access logs of every network filter in every
// filter chain by their json_format leg. The CONNECT log has a text format and
// no leg, so it is grouped as "connect".
func chainAccessLogsByLeg(tree node) map[string][]node {
	out := map[string][]node{}
	for _, lc := range allChains(tree) {
		for _, f := range list(lc.chain, "filters") {
			for _, al := range list(child(f, "typed_config"), "access_log") {
				leg := str(child(child(child(al, "typed_config"), "log_format"), "json_format"), "leg")
				if leg == "" {
					leg = "connect"
				}
				out[leg] = append(out[leg], al)
			}
		}
	}
	return out
}

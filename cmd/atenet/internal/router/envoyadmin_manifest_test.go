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
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"
)

const routerMonitoringManifestPath = "../../../../manifests/ate-install/atenet-router-monitoring.yaml"

// envoyAdminProxyPaths is everything the pod-IP-reachable envoy_metrics
// listener may forward to the admin API: read-only, no topology, no secrets.
var envoyAdminProxyPaths = map[string]bool{"/ready": true, "/stats/prometheus": true}

// envoyAdminManifests are the Envoy sidecars ate-setup installs, each with the
// admin path the rest of the manifest depends on through envoy_metrics.
var envoyAdminManifests = []struct {
	path, configMap, deployment, requiredPath string
}{
	{routerManifestPath, "atenet-router-envoy-config", "atenet-router", "/stats/prometheus"}, // PodMonitoring
	{egressManifests[0], "atenet-egress", "atenet-egress", "/ready"},                         // kubelet probes
}

type socketAddress struct {
	Address    string `json:"address"`
	PortValue  int32  `json:"port_value"`
	IPv4Compat bool   `json:"ipv4_compat"`
}

type envoyAdminBootstrap struct {
	Admin struct {
		Address struct {
			SocketAddress socketAddress `json:"socket_address"`
		} `json:"address"`
	} `json:"admin"`
	StaticResources struct {
		Listeners []struct {
			Name       string `json:"name"`
			StatPrefix string `json:"stat_prefix"`
			Address    struct {
				SocketAddress socketAddress `json:"socket_address"`
			} `json:"address"`
			FilterChains []struct {
				Filters []struct {
					TypedConfig struct {
						StatPrefix  string `json:"stat_prefix"`
						RouteConfig struct {
							VirtualHosts []struct {
								Routes []envoyAdminRoute `json:"routes"`
							} `json:"virtual_hosts"`
						} `json:"route_config"`
					} `json:"typed_config"`
				} `json:"filters"`
			} `json:"filter_chains"`
		} `json:"listeners"`
		Clusters []struct {
			Name           string `json:"name"`
			LoadAssignment struct {
				Endpoints []struct {
					LbEndpoints []struct {
						Endpoint struct {
							Address struct {
								SocketAddress socketAddress `json:"socket_address"`
							} `json:"address"`
						} `json:"endpoint"`
					} `json:"lb_endpoints"`
				} `json:"endpoints"`
			} `json:"load_assignment"`
		} `json:"clusters"`
	} `json:"static_resources"`
}

// envoyAdminRoute captures every path matcher Envoy accepts, so that a route
// widened from an exact path to a prefix or regex is seen rather than dropped.
type envoyAdminRoute struct {
	Match struct {
		Path                string    `json:"path"`
		Prefix              *string   `json:"prefix"`
		PathSeparatedPrefix *string   `json:"path_separated_prefix"`
		SafeRegex           *struct{} `json:"safe_regex"`
		PathMatchPolicy     *struct{} `json:"path_match_policy"`
		Headers             []struct {
			Name        string `json:"name"`
			StringMatch struct {
				Exact string `json:"exact"`
			} `json:"string_match"`
		} `json:"headers"`
	} `json:"match"`
	Route struct {
		Cluster string `json:"cluster"`
	} `json:"route"`
}

// TestEnvoyAdminIsLoopbackOnly guards #2273: the Envoy admin API is
// unauthenticated and serves /quitquitquit, /drain_listeners, /runtime_modify
// and /config_dump, so anything on the cluster network that can reach it can
// take the gateway down or read its topology. It must bind loopback, and
// nothing in the pod spec may point the kubelet (or a Service) at it.
func TestEnvoyAdminIsLoopbackOnly(t *testing.T) {
	for _, m := range envoyAdminManifests {
		t.Run(m.deployment, func(t *testing.T) {
			admin := parseEnvoyAdminBootstrap(t, envoyConfigFrom(t, m.path, m.configMap)).Admin.Address.SocketAddress
			if ip := net.ParseIP(admin.Address); ip == nil || !ip.IsLoopback() {
				t.Errorf("Envoy admin binds %q; it must be a loopback IP so it is unreachable on the pod IP", admin.Address)
			}
			if admin.PortValue == 0 {
				t.Fatal("Envoy admin sets no port_value; the manifest changed shape and this test is checking nothing")
			}

			pod := findDeployment(t, m.path, m.deployment).Spec.Template.Spec
			for _, c := range append(pod.InitContainers, pod.Containers...) {
				named := map[string]int32{}
				for _, p := range c.Ports {
					named[p.Name] = p.ContainerPort
					if p.ContainerPort == admin.PortValue {
						t.Errorf("container %q declares the admin port %d as port %q", c.Name, admin.PortValue, p.Name)
					}
				}
				for kind, probe := range map[string]*corev1.Probe{"readiness": c.ReadinessProbe, "liveness": c.LivenessProbe, "startup": c.StartupProbe} {
					if probe == nil {
						continue
					}
					var port intstr.IntOrString
					switch {
					case probe.HTTPGet != nil:
						port = probe.HTTPGet.Port
					case probe.TCPSocket != nil:
						port = probe.TCPSocket.Port
					default:
						continue
					}
					n := port.IntVal
					if port.Type == intstr.String {
						n = named[port.StrVal]
					}
					if n == admin.PortValue {
						t.Errorf("container %q's %s probe dials the admin port %d, which is loopback-only; probe envoy_metrics instead", c.Name, kind, admin.PortValue)
					}
				}
			}
		})
	}
}

// TestEnvoyAdminProxyIsAllowlisted checks the one listener allowed to reach
// the admin API from the pod IP. It may forward only exact GET paths in
// envoyAdminProxyPaths, and its stat prefixes must contain "admin" so that
// envoyDrainer.activeConnections ignores probe and scrape connections while it
// waits for downstream connections to drain to zero.
func TestEnvoyAdminProxyIsAllowlisted(t *testing.T) {
	for _, m := range envoyAdminManifests {
		t.Run(m.deployment, func(t *testing.T) {
			seen := map[string]bool{}
			for _, r := range envoyAdminProxyRoutes(parseEnvoyAdminBootstrap(t, envoyConfigFrom(t, m.path, m.configMap))) {
				seen[r.route.Match.Path] = true
				checkEnvoyAdminRoute(t, r.listener, r.route)
				if !strings.Contains(r.listenerStatPrefix, "admin") || !strings.Contains(r.hcmStatPrefix, "admin") {
					t.Errorf("listener %q forwards to the admin API but its stat prefixes (listener %q, HCM %q) lack \"admin\", so envoyDrainer counts its connections and the drain may never reach zero", r.listener, r.listenerStatPrefix, r.hcmStatPrefix)
				}
			}
			if !seen[m.requiredPath] {
				t.Errorf("no static listener forwards %s to the admin API, but the manifest depends on it", m.requiredPath)
			}
		})
	}
}

// TestEnvoyReadinessProbeAsksEnvoy checks that each gateway's envoy container
// has a readiness probe on Envoy's /ready, reached through a listener that
// forwards it to the admin API. Without that probe, the Service sends traffic
// to the pod while Envoy is still starting and refuses every connection.
func TestEnvoyReadinessProbeAsksEnvoy(t *testing.T) {
	for _, m := range envoyAdminManifests {
		t.Run(m.deployment, func(t *testing.T) {
			readyPorts := map[int32]bool{}
			for _, r := range envoyAdminProxyRoutes(parseEnvoyAdminBootstrap(t, envoyConfigFrom(t, m.path, m.configMap))) {
				if r.route.Match.Path == "/ready" && reachableOnPodIP(r.address) {
					readyPorts[r.address.PortValue] = true
				}
			}

			var envoy *corev1.Container
			pod := findDeployment(t, m.path, m.deployment).Spec.Template.Spec
			for i := range pod.Containers {
				if pod.Containers[i].Name == "envoy" {
					envoy = &pod.Containers[i]
				}
			}
			if envoy == nil {
				t.Fatalf("%s has no container named envoy", m.deployment)
			}
			probe := envoy.ReadinessProbe
			if probe == nil || probe.HTTPGet == nil {
				t.Fatal("the envoy container has no HTTP readiness probe, so the pod can be Ready before Envoy is")
			}
			if probe.HTTPGet.Path != "/ready" {
				t.Errorf("the envoy readiness probe asks %q; it must ask Envoy's /ready", probe.HTTPGet.Path)
			}
			if probe.HTTPGet.Host != "" || probe.HTTPGet.Scheme == corev1.URISchemeHTTPS {
				t.Errorf("the envoy readiness probe sets host %q and scheme %q; it must dial the pod IP over plain HTTP", probe.HTTPGet.Host, probe.HTTPGet.Scheme)
			}
			port := probe.HTTPGet.Port.IntVal
			if probe.HTTPGet.Port.Type == intstr.String {
				port = 0
				for _, p := range envoy.Ports {
					if p.Name == probe.HTTPGet.Port.StrVal {
						port = p.ContainerPort
					}
				}
			}
			if !readyPorts[port] {
				t.Errorf("the envoy readiness probe dials port %s, but no static listener reachable on the pod IP at that port forwards /ready to the admin API", probe.HTTPGet.Port.String())
			}
		})
	}
}

// reachableOnPodIP reports whether the kubelet, which probes the pod IP, can
// reach a listener bound to a. Envoy binds a bare "::" IPv6-only, so on an
// IPv4 cluster it also needs ipv4_compat.
func reachableOnPodIP(a socketAddress) bool {
	ip := net.ParseIP(a.Address)
	if ip == nil || !ip.IsUnspecified() {
		return false
	}
	return ip.To4() != nil || a.IPv4Compat
}

// envoyAdminProxyRoute is one static-listener route whose cluster is the admin
// API, with the listener details the tests above check.
type envoyAdminProxyRoute struct {
	listener, listenerStatPrefix, hcmStatPrefix string
	address                                     socketAddress
	route                                       envoyAdminRoute
}

func envoyAdminProxyRoutes(b envoyAdminBootstrap) []envoyAdminProxyRoute {
	admin := b.Admin.Address.SocketAddress
	adminClusters := map[string]bool{}
	for _, c := range b.StaticResources.Clusters {
		for _, e := range c.LoadAssignment.Endpoints {
			for _, lb := range e.LbEndpoints {
				if lb.Endpoint.Address.SocketAddress == admin {
					adminClusters[c.Name] = true
				}
			}
		}
	}

	var routes []envoyAdminProxyRoute
	for _, l := range b.StaticResources.Listeners {
		for _, fc := range l.FilterChains {
			for _, f := range fc.Filters {
				for _, vh := range f.TypedConfig.RouteConfig.VirtualHosts {
					for _, r := range vh.Routes {
						if adminClusters[r.Route.Cluster] {
							routes = append(routes, envoyAdminProxyRoute{
								listener:           l.Name,
								listenerStatPrefix: l.StatPrefix,
								hcmStatPrefix:      f.TypedConfig.StatPrefix,
								address:            l.Address.SocketAddress,
								route:              r,
							})
						}
					}
				}
			}
		}
	}
	return routes
}

func checkEnvoyAdminRoute(t *testing.T, listener string, r envoyAdminRoute) {
	t.Helper()
	m := r.Match
	if m.Prefix != nil || m.PathSeparatedPrefix != nil || m.SafeRegex != nil || m.PathMatchPolicy != nil || !envoyAdminProxyPaths[m.Path] {
		t.Errorf("listener %q forwards a route to the admin API that is not an exact match on one of %v", listener, envoyAdminProxyPaths)
	}
	for _, h := range m.Headers {
		if h.Name == ":method" && h.StringMatch.Exact == "GET" {
			return
		}
	}
	t.Errorf("listener %q forwards %q to the admin API without restricting :method to GET", listener, m.Path)
}

// TestRouterPodMonitoringAvoidsEnvoyAdmin checks that Managed Prometheus
// scrapes a port the router's envoy container actually declares, and that the
// port is not the (loopback-only, so unscrapeable) admin port.
func TestRouterPodMonitoringAvoidsEnvoyAdmin(t *testing.T) {
	admin := parseEnvoyAdminBootstrap(t, envoyConfigFrom(t, routerManifestPath, "atenet-router-envoy-config")).Admin.Address.SocketAddress
	named := map[string]int32{}
	for _, c := range findDeployment(t, routerManifestPath, "atenet-router").Spec.Template.Spec.Containers {
		if c.Name == "envoy" {
			for _, p := range c.Ports {
				named[p.Name] = p.ContainerPort
			}
		}
	}

	raw, err := os.ReadFile(routerMonitoringManifestPath)
	if err != nil {
		t.Fatalf("reading %s: %v", routerMonitoringManifestPath, err)
	}
	var mon struct {
		Spec struct {
			Endpoints []struct {
				Port string `json:"port"`
			} `json:"endpoints"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(raw, &mon); err != nil {
		t.Fatalf("parsing %s: %v", routerMonitoringManifestPath, err)
	}
	if len(mon.Spec.Endpoints) == 0 {
		t.Fatalf("%s has no endpoints; the manifest changed shape and this test is checking nothing", routerMonitoringManifestPath)
	}
	for _, ep := range mon.Spec.Endpoints {
		n, ok := named[ep.Port]
		switch {
		case !ok:
			t.Errorf("PodMonitoring scrapes port %q, which the router's envoy container does not declare", ep.Port)
		case n == admin.PortValue:
			t.Errorf("PodMonitoring scrapes port %q, the loopback-only admin port %d", ep.Port, n)
		}
	}
}

func parseEnvoyAdminBootstrap(t *testing.T, raw string) envoyAdminBootstrap {
	t.Helper()
	var b envoyAdminBootstrap
	if err := yaml.Unmarshal([]byte(raw), &b); err != nil {
		t.Fatalf("parsing envoy.yaml: %v", err)
	}
	return b
}

func findDeployment(t *testing.T, path, name string) appsv1.Deployment {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var d appsv1.Deployment
		if err := yaml.Unmarshal([]byte(doc), &d); err != nil {
			t.Fatalf("parsing a document of %s: %v", path, err)
		}
		if d.Kind == "Deployment" && d.Name == name {
			return d
		}
	}
	t.Fatalf("%s has no Deployment named %s", path, name)
	return appsv1.Deployment{}
}

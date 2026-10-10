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
	"strconv"
	"strings"
	"testing"
)

// TestXdsListenAddressIsLoopback guards #2276: the xDS server is plaintext
// and unauthenticated, and ADS/SDS hand out the whole dataplane topology and
// secret paths, so it must never bind a pod-IP-reachable address.
func TestXdsListenAddressIsLoopback(t *testing.T) {
	for _, port := range []int{0, 18000} {
		addr := xdsListenAddress(port)
		host, p, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("xdsListenAddress(%d) = %q, not host:port: %v", port, addr, err)
		}
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			t.Errorf("xdsListenAddress(%d) binds host %q; it must be a loopback IP", port, host)
		}
		if p != strconv.Itoa(port) {
			t.Errorf("xdsListenAddress(%d) uses port %q", port, p)
		}
	}

	// The kernel must agree: listen on an ephemeral port and check what was
	// actually bound, so a wildcard sneaking back in fails here.
	lis, err := net.Listen("tcp", xdsListenAddress(0))
	if err != nil {
		t.Fatalf("listening on %s: %v", xdsListenAddress(0), err)
	}
	defer lis.Close()
	if ip := lis.Addr().(*net.TCPAddr).IP; !ip.IsLoopback() {
		t.Errorf("xDS listener bound %v; it must be loopback", lis.Addr())
	}
}

// TestXdsManifestIsLoopbackOnly checks that the shipped atenet-router manifest
// still works with a loopback-only xDS server, and does not advertise it: the
// Envoy bootstrap must dial a loopback IP on the --port-xds port, and no
// container in the pod may declare that port.
func TestXdsManifestIsLoopbackOnly(t *testing.T) {
	pod := findDeployment(t, routerManifestPath, "atenet-router").Spec.Template.Spec

	xdsPort := 0
	for _, c := range pod.Containers {
		if c.Name != "atenet-router" {
			continue
		}
		for _, a := range c.Args {
			if v, ok := strings.CutPrefix(a, "--port-xds="); ok {
				n, err := strconv.Atoi(v)
				if err != nil {
					t.Fatalf("--port-xds=%q is not a number: %v", v, err)
				}
				xdsPort = n
			}
		}
	}
	if xdsPort == 0 {
		t.Fatal("the atenet-router container sets no --port-xds; the manifest changed shape and this test is checking nothing")
	}

	b := parseEnvoyAdminBootstrap(t, envoyConfigFrom(t, routerManifestPath, "atenet-router-envoy-config"))
	found := false
	for _, cl := range b.StaticResources.Clusters {
		if cl.Name != "xds_cluster" {
			continue
		}
		for _, ep := range cl.LoadAssignment.Endpoints {
			for _, lb := range ep.LbEndpoints {
				sa := lb.Endpoint.Address.SocketAddress
				found = true
				if ip := net.ParseIP(sa.Address); ip == nil || !ip.IsLoopback() {
					t.Errorf("Envoy xds_cluster dials %q; the xDS server only listens on loopback", sa.Address)
				}
				if int(sa.PortValue) != xdsPort {
					t.Errorf("Envoy xds_cluster dials port %d, but the router serves xDS on --port-xds=%d", sa.PortValue, xdsPort)
				}
			}
		}
	}
	if !found {
		t.Fatal("the Envoy bootstrap has no xds_cluster endpoint; the manifest changed shape and this test is checking nothing")
	}

	for _, c := range append(pod.InitContainers, pod.Containers...) {
		for _, p := range c.Ports {
			if int(p.ContainerPort) == xdsPort {
				t.Errorf("container %s declares port %d (%q), the loopback-only xDS port", c.Name, p.ContainerPort, p.Name)
			}
		}
	}
}

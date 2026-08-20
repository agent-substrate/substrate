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

package networking

import (
	"context"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/atenetconsts"
	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/e2e/netutil"
	"github.com/agent-substrate/substrate/internal/resources"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The actor zone is served by a CoreDNS `template` block, which answers for any
// name matching <actor>.<atespace>.<suffix> whether or not that actor exists.
// These tests therefore need no actor fixture — they are asserting the zone's
// behavior, not an actor's.
func probeActorDNSName() string {
	return resources.ActorDNSName(resources.ActorRef{Atespace: networkingAtespace, Name: "dns-probe"})
}

func mustDNSClient(t *testing.T, ctx context.Context) *netutil.DNSClient {
	t.Helper()
	dns, err := netutil.NewDNSClient(ctx, e2e.KubeConfig, e2e.KubeContext)
	if err != nil {
		t.Fatalf("NewDNSClient: %v", err)
	}
	t.Cleanup(dns.Close)
	return dns
}

// TestActorDNSZone asserts that the actor zone answers an A query with the
// router's ClusterIP, and that everything else it is asked comes back NODATA or
// NXDOMAIN rather than SERVFAIL. cmd/atenet/internal/dns/README.md explains why
// the rcode matters. Family-agnostic: it runs, not skips, on single-stack.
func TestActorDNSZone(t *testing.T) {
	ctx := context.Background()
	dns := mustDNSClient(t, ctx)

	routerSvc, err := e2e.GetClients().K8s.CoreV1().Services(atenetconsts.NamespaceATESystem).Get(ctx, atenetconsts.RouterService, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting Service %s/%s: %v", atenetconsts.NamespaceATESystem, atenetconsts.RouterService, err)
	}
	routerIPs := netutil.ClusterIPsByFamily(routerSvc)

	name := probeActorDNSName()

	t.Run("A answers with the router ClusterIP", func(t *testing.T) {
		if routerIPs.V4 == "" {
			// On a v6-only cluster emitting an A record at all would be the bug.
			t.Skip("atenet-router has no IPv4 ClusterIP")
		}
		addrs, rcode, err := dns.Lookup(ctx, "ip4", name)
		if rcode != netutil.DNSAnswered {
			t.Fatalf("A %s: %v (%v); want the router ClusterIP %s", name, rcode, err, routerIPs.V4)
		}
		if !slices.Contains(addrs, routerIPs.V4) {
			t.Fatalf("A %s = %v; want it to contain the atenet-router ClusterIP %s", name, addrs, routerIPs.V4)
		}
	})

	t.Run("AAAA is not a server failure", func(t *testing.T) {
		// The name is well-formed, so NODATA is the answer owed on a
		// single-stack cluster: it exists, it just has no AAAA.
		_, rcode, err := dns.Lookup(ctx, "ip6", name)
		if rcode == netutil.DNSFailed {
			t.Fatalf("AAAA %s: %v (%v); want NODATA — see cmd/atenet/internal/dns/README.md", name, rcode, err)
		}
	})

	t.Run("a name outside the actor pattern is not a server failure", func(t *testing.T) {
		// A regex miss needs both `fallthrough`s and the terminal catch-all
		// template to become NXDOMAIN; drop either and this goes red.
		bogus := "not-an-actor." + resources.ActorDNSSuffix
		_, rcode, err := dns.Lookup(ctx, "ip4", bogus)
		if rcode == netutil.DNSFailed {
			t.Fatalf("A %s: %v (%v); want NXDOMAIN", bogus, rcode, err)
		}
	})
}

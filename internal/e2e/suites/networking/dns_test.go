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

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
)

// The actor zone is served by a CoreDNS `template` block, which answers for any
// name matching <actor>.<atespace>.<suffix> whether or not that actor exists.
// These tests therefore need no actor fixture — they are asserting the zone's
// behavior, not an actor's.
func probeActorDNSName() string {
	return resources.ActorDNSName(resources.ActorRef{Atespace: networkingAtespace, Name: "dns-probe"})
}

func mustDNSClient(t *testing.T, ctx context.Context) *e2e.DNSClient {
	t.Helper()
	dns, err := e2e.NewDNSClient(ctx)
	if err != nil {
		t.Fatalf("NewDNSClient: %v", err)
	}
	t.Cleanup(dns.Close)
	return dns
}

// TestActorDNSZone asserts that the actor zone answers an A query with the
// router's ClusterIP, and — the part no other test covers — that everything
// else it is asked returns a *benign* rcode rather than SERVFAIL.
//
// The rcode matters more than the missing record. NODATA and NXDOMAIN are what
// every stub resolver expects for "there is no address here"; SERVFAIL is a
// transport fault, and resolvers disagree about it. musl maps it to EAI_AGAIN
// and abandons the whole getaddrinfo — and because musl issues the A and AAAA
// queries in parallel, one SERVFAIL sinks the other with it, so an Alpine-based
// client cannot resolve an actor name at all, not even its A record. glibc
// retries and pays the resolver timeout instead. And unlike NODATA and
// NXDOMAIN, SERVFAIL carries no SOA to cache negatively against, so every
// request re-pays that cost. Go's resolver masks all of this, which is why no
// test in this repo caught it before these.
//
// The zone gets the rcodes right with three `template` blocks in
// cmd/atenet/internal/dns/corefile.go: the `IN A` block that answers actor
// names, a regex-matched `template ANY ANY` returning NOERROR plus an SOA
// authority (NODATA) for other qtypes on a well-formed actor name, and a
// terminal `template ANY ANY` returning NXDOMAIN plus an SOA authority for
// everything else in the zone. The first two carry a bare `fallthrough`, which
// is load-bearing: the plugin walks past a class or qtype mismatch by itself,
// but a *regex* miss returns SERVFAIL immediately unless the block declares it.
// The two subtests below are what keeps that from being collapsed back into a
// single block.
//
// This test is family-agnostic and is expected to run, not skip, on a
// single-stack cluster.
func TestActorDNSZone(t *testing.T) {
	ctx := context.Background()
	dns := mustDNSClient(t, ctx)

	routerV4, _, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}

	name := probeActorDNSName()

	t.Run("A answers with the router ClusterIP", func(t *testing.T) {
		if routerV4 == "" {
			// A v6-only cluster: there is no IPv4 ClusterIP to answer with, and
			// emitting an A record at all would be the bug.
			t.Skip("atenet-router has no IPv4 ClusterIP")
		}
		addrs, rcode, err := dns.Lookup(ctx, "ip4", name)
		if rcode != e2e.DNSAnswered {
			t.Fatalf("A %s: %v (%v); want the router ClusterIP %s", name, rcode, err, routerV4)
		}
		if !slices.Contains(addrs, routerV4) {
			t.Fatalf("A %s = %v; want it to contain the atenet-router ClusterIP %s", name, addrs, routerV4)
		}
	})

	t.Run("AAAA is not a server failure", func(t *testing.T) {
		// The name is well-formed, so NODATA is the answer owed here on a
		// single-stack cluster: it exists, it just has no address in this
		// family. That needs a block that matches the qtype. Were the `IN A`
		// template the zone's only one, a qtype mismatch would fall through to
		// a plugin chain with nothing after it, and plugin.NextOrFailure with a
		// nil Next returns SERVFAIL.
		_, rcode, err := dns.Lookup(ctx, "ip6", name)
		if rcode == e2e.DNSFailed {
			t.Fatalf("AAAA %s: %v (%v); want NODATA. A SERVFAIL on a non-A qtype in this "+
				"zone breaks musl-based clients on IPv4-only clusters too, because it takes "+
				"their parallel A query down with it", name, rcode, err)
		}
	})

	t.Run("a name outside the actor pattern is not a server failure", func(t *testing.T) {
		// A single-label name inside the zone: the zone matches, the qtype
		// matches, the actor regex does not. This is the case that depends on
		// both halves of the corefile fix at once. A regex miss is the one kind
		// of non-match the template plugin does not walk past on its own -- it
		// consults fall.Through() and, absent a bare `fallthrough`, answers
		// SERVFAIL without evaluating any later block. So the two regex-matched
		// templates each need `fallthrough` to decline the name, and the
		// terminal catch-all `template ANY ANY` is what turns it into NXDOMAIN.
		// Drop either piece and this subtest goes red.
		bogus := "not-an-actor." + resources.ActorDNSSuffix
		_, rcode, err := dns.Lookup(ctx, "ip4", bogus)
		if rcode == e2e.DNSFailed {
			t.Fatalf("A %s: %v (%v); want NXDOMAIN", bogus, rcode, err)
		}
	})
}

// TestActorDNSAAAA asserts the zone publishes the router's IPv6 ClusterIP.
//
// Skipped unless the atenet-router Service actually has one, which is the
// steady state on every single-stack cluster: a Service with no
// ipFamilyPolicy is SingleStack and never gets a second ClusterIP, so there is
// nothing an AAAA could correctly point at.
//
// Deliberately separate from TestActorDNSZone: it is the only assertion here
// whose expected result changes when the cluster becomes dual-stack, so keeping
// it its own function lets a dual-stack CI job exclude it while the AAAA
// generator is still in flight.
func TestActorDNSAAAA(t *testing.T) {
	ctx := context.Background()

	_, routerV6, err := e2e.RouterClusterIPs(ctx)
	if err != nil {
		t.Fatalf("reading atenet-router ClusterIPs: %v", err)
	}
	if routerV6 == "" {
		t.Skip("atenet-router has no IPv6 ClusterIP; single-stack cluster, nothing to publish")
	}

	dns := mustDNSClient(t, ctx)
	name := probeActorDNSName()

	addrs, rcode, err := dns.Lookup(ctx, "ip6", name)
	if rcode != e2e.DNSAnswered {
		t.Fatalf("AAAA %s: %v (%v); want the router IPv6 ClusterIP %s", name, rcode, err, routerV6)
	}
	if !slices.Contains(addrs, routerV6) {
		t.Fatalf("AAAA %s = %v; want it to contain the atenet-router IPv6 ClusterIP %s", name, addrs, routerV6)
	}
}

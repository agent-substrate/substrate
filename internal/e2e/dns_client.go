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

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/portforward"
	"k8s.io/client-go/kubernetes"
)

const (
	dnsNamespace = "ate-system"
	dnsService   = "dns"
	// dnsServicePort is the Service port; the Service exposes 53 twice, once
	// UDP and once TCP, and the port-forward tunnel is TCP either way.
	dnsServicePort = 53
)

// DNSRcode is how the server answered, at the granularity net.Resolver exposes.
type DNSRcode int

const (
	// DNSAnswered is NOERROR with at least one address of the queried family.
	DNSAnswered DNSRcode = iota
	// DNSEmpty is "this name has no address in this family": NODATA (NOERROR
	// with an empty answer section) or NXDOMAIN. net.Resolver reports both as
	// DNSError.IsNotFound and the standard library offers no way to tell them
	// apart, which is fine here — both are benign to every stub resolver, and
	// that benign-ness is the property under test.
	DNSEmpty
	// DNSFailed is SERVFAIL, REFUSED, a timeout, or a malformed reply: anything
	// net.Resolver classifies as the server misbehaving. No query into the actor
	// zone should ever produce it — not a non-A qtype, not a name that fails the
	// actor regex — and that is the regression these tests exist to catch.
	DNSFailed
)

func (r DNSRcode) String() string {
	switch r {
	case DNSAnswered:
		return "answered"
	case DNSEmpty:
		return "no-such-host (NODATA or NXDOMAIN)"
	case DNSFailed:
		return "server failure (SERVFAIL/REFUSED/timeout)"
	default:
		return "unknown"
	}
}

// DNSClient resolves names against the ate-system/dns CoreDNS Service over a
// port-forward.
//
// Querying that Service directly, rather than going through the cluster's own
// resolver, is deliberate: the delegation that would make actor names resolvable
// cluster-wide is a patch to the kube-system/kube-dns ConfigMap, which only
// exists on GKE (cmd/atenet/internal/dns/dns.go reconcileKubeDNSConfig hits the
// IsNotFound branch on kind and upstream Kubernetes). Pointing at the Service is
// the only way to assert the zone's behavior on every cluster we test on.
type DNSClient struct {
	resolver *net.Resolver
	stop     func()
}

// NewDNSClient establishes a port-forward to the atenet DNS Service. Call Close
// to tear it down.
func NewDNSClient(ctx context.Context) (*DNSClient, error) {
	config, err := ateclient.LoadConfig(KubeConfig, KubeContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, dnsNamespace, dnsService, dnsServicePort)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(localPort))

	return &DNSClient{
		stop: stop,
		resolver: &net.Resolver{
			// PreferGo keeps us on Go's own resolver on every platform. cgo's
			// would ignore Dial entirely and query the host's nameservers.
			PreferGo: true,
			// Surface a per-family failure instead of hiding it behind the
			// other family's success.
			StrictErrors: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				// The port-forward is a TCP tunnel, so every query goes over TCP
				// whatever network the resolver asked for. net.Resolver selects
				// stream framing for any conn that is not a net.PacketConn, so
				// returning a TCP conn here is transparent to it. The requested
				// server address is ignored: there is exactly one server.
				var d net.Dialer
				return d.DialContext(ctx, "tcp", addr)
			},
		},
	}, nil
}

// Close tears down the port-forward.
func (c *DNSClient) Close() {
	if c.stop != nil {
		c.stop()
	}
}

// Lookup resolves name in a single address family — network is "ip4" for an A
// query or "ip6" for a AAAA query — and reports the addresses alongside how the
// server answered. A DNSFailed result is returned with the underlying error for
// the failure message; DNSEmpty is returned with a nil error because it is a
// valid answer, not a fault.
func (c *DNSClient) Lookup(ctx context.Context, network, name string) ([]string, DNSRcode, error) {
	// Root the name so the resolver skips the host's search list and ndots
	// handling, which would otherwise make the query depend on where the test
	// runs.
	if !strings.HasSuffix(name, ".") {
		name += "."
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	addrs, err := c.resolver.LookupNetIP(lookupCtx, network, name)
	if err == nil {
		ips := make([]string, 0, len(addrs))
		for _, a := range addrs {
			ips = append(ips, a.Unmap().String())
		}
		return ips, DNSAnswered, nil
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return nil, DNSEmpty, nil
	}
	return nil, DNSFailed, fmt.Errorf("%s query for %q: %w", network, name, err)
}

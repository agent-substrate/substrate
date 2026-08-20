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

package netutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/atenetconsts"
	"github.com/agent-substrate/substrate/internal/portforward"
	"k8s.io/client-go/kubernetes"
)

// DNSRcode is how the server answered, at the granularity net.Resolver exposes.
type DNSRcode int

const (
	// DNSAnswered is NOERROR with at least one address of the queried family.
	DNSAnswered DNSRcode = iota
	// DNSEmpty is NODATA or NXDOMAIN: the name has no address in this family.
	DNSEmpty
	// DNSFailed is SERVFAIL, REFUSED, a timeout, or a malformed reply.
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
// port-forward, rather than through the cluster resolver, because the kube-dns
// delegation only exists on GKE.
type DNSClient struct {
	resolver *net.Resolver
	stop     func()
}

// NewDNSClient establishes a port-forward to the atenet DNS Service. Call Close
// to tear it down.
func NewDNSClient(ctx context.Context, kubeconfig, kubecontext string) (*DNSClient, error) {
	config, err := ateclient.LoadConfig(kubeconfig, kubecontext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, atenetconsts.NamespaceATESystem, atenetconsts.DNSService, 53)
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(localPort))

	return &DNSClient{
		stop: stop,
		resolver: &net.Resolver{
			// cgo's resolver would ignore Dial and query the host's nameservers.
			PreferGo: true,
			// Surface a per-family failure instead of hiding it behind the
			// other family's success.
			StrictErrors: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				// net.Resolver uses stream framing for any conn that is not a
				// net.PacketConn, so the TCP tunnel is transparent to it.
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
// query, "ip6" for AAAA. DNSFailed carries the underlying error; DNSEmpty does
// not, because it is a valid answer.
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

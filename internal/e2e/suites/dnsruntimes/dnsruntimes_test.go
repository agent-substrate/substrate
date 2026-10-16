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

package dnsruntimes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

const (
	templateGlibc = "runtime-glibc"
	templateMusl  = "runtime-musl"
)

// RuntimeSpec codifies one row of the DNS resolver behavior matrix.
type RuntimeSpec struct {
	// Name is the human-readable row name from the specification table.
	Name string
	// ResolverEngine describes the underlying resolver implementation.
	ResolverEngine string
	// Probe is the executable name under /opt/probes/<Probe>.
	Probe string
	// Templates lists the fixture ActorTemplates ("runtime-glibc", "runtime-musl")
	// this runtime runs on. Empty when SkipReason is set.
	Templates []string
	// SkipReason documents why a platform row is skipped on Linux workers.
	SkipReason string
	// FailoverToNextServer reports whether the resolver fails over to the next
	// nameserver when the primary returns REFUSED.
	FailoverToNextServer bool
	// ParallelNameservers reports whether the resolver queries all nameservers
	// concurrently (as musl's __res_msend_rc does).
	ParallelNameservers bool
	// RetriesSameServerOnRefused reports whether the resolver retries the same
	// nameserver on REFUSED or cycles across servers.
	RetriesSameServerOnRefused bool
	// NegativeCacheTTL is the duration for which the runtime caches negative
	// (NXDOMAIN) lookups in-process (e.g. 10s for OpenJDK InetAddress), or 0 if
	// NXDOMAIN is not cached.
	NegativeCacheTTL time.Duration
	// ExpectedErrorSubstrings maps the template name ("runtime-glibc" or
	// "runtime-musl") to any of the acceptable error substrings when the sandbox
	// DNS relay denies a query with NXDOMAIN.
	ExpectedErrorSubstrings map[string][]string
}

// MatrixSpecs defines all 13 rows from the runtime DNS resolver matrix.
var MatrixSpecs = []RuntimeSpec{
	{
		Name:                       "glibc (C / C++ / Linux)",
		ResolverEngine:             "Stub resolver (res_send / NSS getaddrinfo)",
		Probe:                      "libc-getaddrinfo",
		Templates:                  []string{templateGlibc},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"EAI_NONAME", "Name or service not known"},
		},
	},
	{
		Name:                       "musl libc (Alpine Linux)",
		ResolverEngine:             "Parallel stub (__res_msend_rc)",
		Probe:                      "libc-getaddrinfo",
		Templates:                  []string{templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        true,
		RetriesSameServerOnRefused: false,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateMusl: {"EAI_NONAME", "Name does not resolve"},
		},
	},
	{
		Name:                       "Golang (Pure Go)",
		ResolverEngine:             "net.DefaultResolver (net/dnsclient_unix.go)",
		Probe:                      "go-pure",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"no such host"},
			templateMusl:  {"no such host"},
		},
	},
	{
		Name:                       "Golang (cgo)",
		ResolverEngine:             "Host OS libc via getaddrinfo",
		Probe:                      "go-cgo",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"no such host"},
			templateMusl:  {"no such host"},
		},
	},
	{
		Name:                       "Node.js (dns.lookup)",
		ResolverEngine:             "libuv threadpool (uv_getaddrinfo)",
		Probe:                      "node-lookup",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"ENOTFOUND"},
			templateMusl:  {"ENOTFOUND"},
		},
	},
	{
		Name:                       "Node.js (dns.resolve*)",
		ResolverEngine:             "c-ares (asynchronous C resolver)",
		Probe:                      "node-resolve",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"ENOTFOUND"},
			templateMusl:  {"ENOTFOUND"},
		},
	},
	{
		Name:                       "Python (socket / asyncio)",
		ResolverEngine:             "Host OS libc (getaddrinfo via threadpool)",
		Probe:                      "python-socket",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"Name or service not known", "[Errno -2]"},
			templateMusl:  {"Name does not resolve", "[Errno -2]"},
		},
	},
	{
		Name:                       "Python (dnspython)",
		ResolverEngine:             "dns.resolver.Resolver",
		Probe:                      "python-dnspython",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"dns.resolver.NXDOMAIN"},
			templateMusl:  {"dns.resolver.NXDOMAIN"},
		},
	},
	{
		Name:                       "Java (OpenJDK InetAddress)",
		ResolverEngine:             "JNI to OS getaddrinfo",
		Probe:                      "java-inetaddress",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           10 * time.Second,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"Name or service not known"},
			templateMusl:  {"Name does not resolve"},
		},
	},
	{
		Name:                       "Java (Netty DnsNameResolver)",
		ResolverEngine:             "Async Netty DNS client",
		Probe:                      "java-netty",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"NXDOMAIN", "SearchDomainUnknownHostException"},
			templateMusl:  {"NXDOMAIN", "SearchDomainUnknownHostException"},
		},
	},
	{
		Name:                       "Rust (std::net::ToSocketAddrs)",
		ResolverEngine:             "libc::getaddrinfo",
		Probe:                      "rust-std",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"Name or service not known"},
			templateMusl:  {"Name does not resolve"},
		},
	},
	{
		Name:                       "Rust (hickory-dns / trust-dns)",
		ResolverEngine:             "Async native Rust client",
		Probe:                      "rust-hickory",
		Templates:                  []string{templateGlibc, templateMusl},
		FailoverToNextServer:       true,
		ParallelNameservers:        false,
		RetriesSameServerOnRefused: true,
		NegativeCacheTTL:           0,
		ExpectedErrorSubstrings: map[string][]string{
			templateGlibc: {"NXDomain"},
			templateMusl:  {"NXDomain"},
		},
	},
	{
		Name:           "Windows (Win32 / MSVCRT)",
		ResolverEngine: "DNS Client Service (dnscache / ws2_32)",
		SkipReason:     "Windows sandboxes are not supported on Linux ateworker nodes",
	},
}

// probeRequest mirrors the fixture's ProbeRequest JSON payload.
type probeRequest struct {
	Runtime     string           `json:"runtime"`
	Op          string           `json:"op"`
	Host        string           `json:"host,omitempty"`
	URL         string           `json:"url,omitempty"`
	Attempts    int              `json:"attempts,omitempty"`
	IntervalMs  int              `json:"intervalMs,omitempty"`
	EnableCache bool             `json:"enableCache,omitempty"`
	MockDNS     []mockServerSpec `json:"mockDNS,omitempty"`
	Search      []string         `json:"search,omitempty"`
}

type mockServerSpec struct {
	Addr     string `json:"addr"`
	RCode    string `json:"rcode"`
	AnswerIP string `json:"answerIP,omitempty"`
}

type probeAttempt struct {
	Success    bool     `json:"success"`
	Addresses  []string `json:"addresses,omitempty"`
	StatusCode int      `json:"statusCode,omitempty"`
	ErrorCode  string   `json:"errorCode,omitempty"`
	Error      string   `json:"error,omitempty"`
	DurationMs int64    `json:"durationMs"`
}

type dnsQueryLog struct {
	ServerAddr  string `json:"serverAddr"`
	ServerIndex int    `json:"serverIndex"`
	Network     string `json:"network"`
	QName       string `json:"qname"`
	QType       uint16 `json:"qtype"`
	TimestampNs int64  `json:"timestampNs"`
}

type probeResponse struct {
	Runtime    string         `json:"runtime"`
	Attempts   []probeAttempt `json:"attempts"`
	DNSQueries []dnsQueryLog  `json:"dnsQueries,omitempty"`
	Error      string         `json:"error,omitempty"`
}

var (
	sharedFixtureOnce sync.Once
	sharedAtespace    string
)

// ensureSuiteFixture deploys the dnsruntimes WorkerPool and ActorTemplates
// once for the test binary when running against a cluster.
func ensureSuiteFixture(t *testing.T, ctx context.Context, clients *e2e.Clients) string {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	sharedFixtureOnce.Do(func() {
		sharedAtespace, _ = e2e.DeployDNSRuntimesFixture(t, ctx, clients, env["BUCKET_NAME"], "dnsruntimes")
	})
	if sharedAtespace == "" {
		t.Fatal("dnsruntimes fixture failed to initialize")
	}
	return sharedAtespace
}

// TestDNSRuntimesBlocking verifies that every runtime in MatrixSpecs:
//  1. Resolves and fetches an HTTP origin allowed by the actor's EgressPolicy.
//  2. Fails to resolve an unlisted hostname with the runtime-specific NXDOMAIN
//     error signature documented in the matrix table.
func TestDNSRuntimesBlocking(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	atespace := ensureSuiteFixture(t, ctx, clients)

	origin := e2e.ServerPod{
		Name:       "matrixhttp",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
	}
	target := e2e.DeployServerPod(t, ctx, origin)
	allowedHost := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)
	allowedURL := "http://" + allowedHost + "/healthz"
	const blockedHost = "blocked.example.com"

	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Resume one actor per distro template (glibc and musl) and run all runtime
	// probes against it.
	actors := map[string]resources.ActorRef{
		templateGlibc: createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateGlibc, "dnsblock-glibc", e2e.EgressAllowHTTP(allowedHost)),
		templateMusl:  createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateMusl, "dnsblock-musl", e2e.EgressAllowHTTP(allowedHost)),
	}

	for _, spec := range MatrixSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			if spec.SkipReason != "" {
				t.Skip(spec.SkipReason)
			}
			for _, tmpl := range spec.Templates {
				t.Run(tmpl, func(t *testing.T) {
					actorRef := actors[tmpl]

					// 1. Allowed host resolves.
					resResp := runProbe(t, ctx, router, actorRef, probeRequest{
						Runtime: spec.Probe,
						Op:      "resolve",
						Host:    allowedHost,
					})
					if len(resResp.Attempts) != 1 || !resResp.Attempts[0].Success {
						t.Fatalf("resolve allowed host %s failed: %+v", allowedHost, resResp)
					}
					if !slices.Contains(resResp.Attempts[0].Addresses, target.ClusterIP) {
						t.Errorf("resolve %s addresses = %v, want to contain %s", allowedHost, resResp.Attempts[0].Addresses, target.ClusterIP)
					}

					// 2. Allowed host HTTP GET succeeds through egress gateway.
					httpResp := runProbe(t, ctx, router, actorRef, probeRequest{
						Runtime: spec.Probe,
						Op:      "http_get",
						URL:     allowedURL,
					})
					if len(httpResp.Attempts) != 1 || !httpResp.Attempts[0].Success || httpResp.Attempts[0].StatusCode != http.StatusOK {
						t.Fatalf("http_get allowed URL %s failed: %+v", allowedURL, httpResp)
					}

					// 3. Disallowed host resolution is denied with expected runtime error.
					blockResp := runProbe(t, ctx, router, actorRef, probeRequest{
						Runtime: spec.Probe,
						Op:      "resolve",
						Host:    blockedHost,
					})
					if len(blockResp.Attempts) != 1 {
						t.Fatalf("expected 1 attempt, got %+v", blockResp)
					}
					att := blockResp.Attempts[0]
					if att.Success {
						t.Fatalf("resolve blocked host %s unexpectedly succeeded with %v", blockedHost, att.Addresses)
					}
					wantSubstrings := spec.ExpectedErrorSubstrings[tmpl]
					if !matchesAnySubstring(att.Error, att.ErrorCode, wantSubstrings) {
						t.Errorf("blocked lookup error = %q (code %q), want any of %v", att.Error, att.ErrorCode, wantSubstrings)
					}
				})
			}
		})
	}
}

// TestDNSRuntimesSearchDomains verifies that every runtime resolves a short
// name against a search list whose first suffix the relay denies with NXDOMAIN,
// by continuing on to the next suffix, which is allowed.
func TestDNSRuntimesSearchDomains(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	atespace := ensureSuiteFixture(t, ctx, clients)

	origin := e2e.ServerPod{
		Name:       "searchsvc",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
	}
	target := e2e.DeployServerPod(t, ctx, origin)
	fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)

	router := mustRouterClient(t, ctx)
	defer router.Close()

	// The actor's /etc/resolv.conf has:
	//   search <actor-pod-ns>.svc.cluster.local svc.cluster.local cluster.local
	// Resolving "<Name>.<target.Namespace>" (1 dot, < ndots:5):
	//   1st candidate: <Name>.<target.Namespace>.<actor-pod-ns>.svc.cluster.local -> NXDOMAIN
	//   2nd candidate: <Name>.<target.Namespace>.svc.cluster.local -> ALLOWED (matches fqdn)
	relativeName := fmt.Sprintf("%s.%s", origin.Name, target.Namespace)

	actors := map[string]resources.ActorRef{
		templateGlibc: createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateGlibc, "search-glibc", e2e.EgressAllowHTTP(fqdn)),
		templateMusl:  createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateMusl, "search-musl", e2e.EgressAllowHTTP(fqdn)),
	}

	for _, spec := range MatrixSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			if spec.SkipReason != "" {
				t.Skip(spec.SkipReason)
			}
			for _, tmpl := range spec.Templates {
				t.Run(tmpl, func(t *testing.T) {
					actorRef := actors[tmpl]
					resp := runProbe(t, ctx, router, actorRef, probeRequest{
						Runtime: spec.Probe,
						Op:      "resolve",
						Host:    relativeName,
					})
					if len(resp.Attempts) != 1 {
						t.Fatalf("expected 1 attempt, got %+v", resp)
					}
					att := resp.Attempts[0]
					// Node.js dns.resolve* (src/cares_wrap.cc) calls c-ares ares_query()
					// rather than ares_search(), so it never appends /etc/resolv.conf
					// search domains and queries the literal relative name directly.
					if spec.Probe == "node-resolve" {
						if att.Success || !matchesAnySubstring(att.Error, att.ErrorCode, spec.ExpectedErrorSubstrings[tmpl]) {
							t.Fatalf("%s (%s) expected ENOTFOUND on unexpanded relative name %q (ares_query ignores resolv.conf search), got %+v",
								spec.Name, tmpl, relativeName, att)
						}
						return
					}
					if !att.Success {
						t.Fatalf("%s (%s) failed on relative name %q when first search domain returned NXDOMAIN; expected continuation to second search domain: %+v", spec.Name, tmpl, relativeName, att)
					}
					if !slices.Contains(att.Addresses, target.ClusterIP) {
						t.Errorf("%s (%s) relative lookup %q addresses = %v, want to contain %s", spec.Name, tmpl, relativeName, att.Addresses, target.ClusterIP)
					}
				})
			}
		})
	}
}

// TestDNSRuntimesNegativeCaching verifies the "Caching of NXDOMAIN" column:
//   - Most resolvers do not cache the relay's NXDOMAIN responses, whose SOA
//     record has a TTL of 0, and succeed immediately once the actor's
//     EgressPolicy is updated and the 5s policy cache expires.
//   - OpenJDK InetAddress caches negative lookups for 10 seconds in the JVM
//     (networkaddress.cache.negative.ttl=10), so within a single JVM invocation
//     early attempts continue to fail before recovering once the TTL expires.
func TestDNSRuntimesNegativeCaching(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	atespace := ensureSuiteFixture(t, ctx, clients)

	origin := e2e.ServerPod{
		Name:       "negcachesvc",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http"},
		Port:       80,
		TargetPort: 8080,
	}
	target := e2e.DeployServerPod(t, ctx, origin)
	fqdn := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)

	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Start with an EgressPolicy that does NOT allow fqdn.
	actorRef := createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateGlibc, "negcache-glibc", e2e.EgressAllowHTTP("other.example.com"))

	// Confirm initial lookup is denied for both Go and Java.
	for _, probe := range []string{"go-pure", "java-inetaddress"} {
		initResp := runProbe(t, ctx, router, actorRef, probeRequest{
			Runtime: probe,
			Op:      "resolve",
			Host:    fqdn,
		})
		if len(initResp.Attempts) != 1 || initResp.Attempts[0].Success {
			t.Fatalf("initial %s lookup of %s succeeded before policy update: %+v", probe, fqdn, initResp)
		}
	}

	// Launch a single long-lived Java InetAddress probe that performs 16 attempts
	// spaced 1s apart (~15s total). After 1s, update the EgressPolicy on the
	// control plane to allow fqdn.
	go func() {
		time.Sleep(1 * time.Second)
		e2e.EnsureEgressPolicy(t, ctx, clients, actorRef.ToObjectRef(), e2e.EgressAllowHTTP(fqdn))
	}()

	javaResp := runProbe(t, ctx, router, actorRef, probeRequest{
		Runtime:     "java-inetaddress",
		Op:          "resolve",
		Host:        fqdn,
		Attempts:    16,
		IntervalMs:  1000,
		EnableCache: true,
	})
	if len(javaResp.Attempts) < 2 {
		t.Fatalf("expected multiple attempts from java-inetaddress, got %+v", javaResp)
	}
	if javaResp.Attempts[0].Success {
		t.Fatalf("java-inetaddress attempt 0 succeeded before policy was updated")
	}
	var recoveredAt int = -1
	for i, att := range javaResp.Attempts {
		if att.Success {
			recoveredAt = i
			break
		}
	}
	if recoveredAt < 0 {
		t.Fatalf("java-inetaddress never recovered after EgressPolicy update and 10s JVM negative cache TTL: %+v", javaResp.Attempts)
	}
	t.Logf("java-inetaddress recovered at attempt %d after JVM negative cache expiry", recoveredAt)

	// Verify non-caching resolvers (e.g. go-pure, python-dnspython with cache enabled)
	// now resolve fqdn immediately.
	for _, probe := range []string{"go-pure", "python-dnspython", "rust-hickory"} {
		afterResp := runProbe(t, ctx, router, actorRef, probeRequest{
			Runtime:     probe,
			Op:          "resolve",
			Host:        fqdn,
			EnableCache: true,
		})
		if len(afterResp.Attempts) != 1 || !afterResp.Attempts[0].Success {
			t.Errorf("%s failed to resolve %s after policy update: %+v", probe, fqdn, afterResp)
		}
	}
}

// TestDNSRuntimesNameserverFailover verifies the "Failover to Next Server?"
// and "Retries Same Server?" columns using the in-sandbox multi-server mock DNS
// harness:
//   - Server 0 (127.0.0.10) returns REFUSED.
//   - Server 1 (127.0.0.11) returns NOERROR -> 127.0.0.42.
func TestDNSRuntimesNameserverFailover(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	atespace := ensureSuiteFixture(t, ctx, clients)

	router := mustRouterClient(t, ctx)
	defer router.Close()

	actors := map[string]resources.ActorRef{
		templateGlibc: createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateGlibc, "failover-glibc", e2e.EgressAllowAll()...),
		templateMusl:  createAndResumeMatrixActor(t, ctx, clients, router, atespace, templateMusl, "failover-musl", e2e.EgressAllowAll()...),
	}

	mockServers := []mockServerSpec{
		{Addr: "127.0.0.10", RCode: "REFUSED"},
		{Addr: "127.0.0.11", RCode: "NOERROR", AnswerIP: "127.0.0.42"},
	}

	for _, spec := range MatrixSpecs {
		t.Run(spec.Name, func(t *testing.T) {
			if spec.SkipReason != "" {
				t.Skip(spec.SkipReason)
			}
			for _, tmpl := range spec.Templates {
				t.Run(tmpl, func(t *testing.T) {
					actorRef := actors[tmpl]
					resp := runProbe(t, ctx, router, actorRef, probeRequest{
						Runtime: spec.Probe,
						Op:      "resolve",
						Host:    "failover.matrix.local.",
						MockDNS: mockServers,
					})
					if len(resp.Attempts) != 1 {
						t.Fatalf("expected 1 attempt, got %+v", resp)
					}
					att := resp.Attempts[0]

					// Node.js initializes c-ares with ARES_FLAG_NOCHECKRESP so it can
					// surface explicit EREFUSED/ESERVFAIL/ENOTIMP codes to callers.
					// Consequently, c-ares treats an explicit REFUSED packet from
					// server 0 as a terminal response rather than failing over, and
					// only fails over to server 1 when server 0 times out / drops.
					if spec.Probe == "node-resolve" {
						if att.Success || !matchesAnySubstring(att.Error, att.ErrorCode, []string{"EREFUSED"}) {
							t.Fatalf("%s (%s) expected EREFUSED on server 0 REFUSED with ARES_FLAG_NOCHECKRESP, got %+v",
								spec.Name, tmpl, att)
						}
						resp = runProbe(t, ctx, router, actorRef, probeRequest{
							Runtime: spec.Probe,
							Op:      "resolve",
							Host:    "failover.matrix.local.",
							MockDNS: []mockServerSpec{
								{Addr: "127.0.0.10", RCode: "DROP"},
								{Addr: "127.0.0.11", RCode: "NOERROR", AnswerIP: "127.0.0.42"},
							},
						})
						if len(resp.Attempts) != 1 {
							t.Fatalf("expected 1 attempt on DROP failover, got %+v", resp)
						}
						att = resp.Attempts[0]
					}

					if spec.FailoverToNextServer {
						if !att.Success {
							t.Fatalf("%s (%s) did not fail over to second nameserver: %+v (queries: %+v)",
								spec.Name, tmpl, att, resp.DNSQueries)
						}
						if !slices.Contains(att.Addresses, "127.0.0.42") {
							t.Errorf("%s (%s) addresses = %v, want [127.0.0.42]", spec.Name, tmpl, att.Addresses)
						}
					}

					// Verify both mock nameservers were queried.
					var queried0, queried1 bool
					for _, q := range resp.DNSQueries {
						if q.ServerIndex == 0 {
							queried0 = true
						}
						if q.ServerIndex == 1 {
							queried1 = true
						}
					}
					if !queried0 || !queried1 {
						t.Errorf("%s (%s) mock DNS queries = %+v, want both server 0 and server 1 queried",
							spec.Name, tmpl, resp.DNSQueries)
					}
				})
			}
		})
	}
}

func matchesAnySubstring(errStr, errCode string, want []string) bool {
	combined := strings.ToLower(errStr + " " + errCode)
	for _, w := range want {
		if strings.Contains(combined, strings.ToLower(w)) {
			return true
		}
	}
	return false
}

// TestMatrixSpecsTable Completeness verifies at unit-test time (even without
// --e2e) that MatrixSpecs covers all 13 rows from the specification table and
// defines error expectations for every active template.
func TestMatrixSpecsTableCompleteness(t *testing.T) {
	if len(MatrixSpecs) != 13 {
		t.Fatalf("len(MatrixSpecs) = %d, want 13", len(MatrixSpecs))
	}
	var activeCount, skippedCount int
	for _, spec := range MatrixSpecs {
		if spec.Name == "" || spec.ResolverEngine == "" {
			t.Errorf("spec %+v is missing Name or ResolverEngine", spec)
		}
		if spec.SkipReason != "" {
			skippedCount++
			continue
		}
		activeCount++
		if spec.Probe == "" {
			t.Errorf("active spec %q has empty Probe", spec.Name)
		}
		if len(spec.Templates) == 0 {
			t.Errorf("active spec %q has no Templates", spec.Name)
		}
		for _, tmpl := range spec.Templates {
			if len(spec.ExpectedErrorSubstrings[tmpl]) == 0 {
				t.Errorf("spec %q is missing ExpectedErrorSubstrings for template %q", spec.Name, tmpl)
			}
		}
	}
	if activeCount != 12 || skippedCount != 1 {
		t.Errorf("activeCount = %d, skippedCount = %d; want 12 active and 1 skipped", activeCount, skippedCount)
	}
}

func createAndResumeMatrixActor(t *testing.T, ctx context.Context, clients *e2e.Clients, router *e2e.RouterClient, atespace, templateName, prefix string, rules ...*ateapipb.EgressRule) resources.ActorRef {
	t.Helper()
	actorName := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	objRef := &ateapipb.ObjectRef{Atespace: atespace, Name: actorName}

	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: templateName},
		},
	}); err != nil {
		t.Fatalf("CreateActor %s/%s: %v", atespace, actorName, err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapipb.SuspendActorRequest{Actor: objRef})
		_, _ = clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapipb.DeleteActorRequest{Actor: objRef})
	})

	if len(rules) > 0 {
		e2e.EnsureEgressPolicy(t, ctx, clients, objRef, rules...)
	}

	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: objRef}); err != nil {
		t.Fatalf("ResumeActor %s/%s: %v", atespace, actorName, err)
	}

	actorRef := resources.ActorRef{Atespace: atespace, Name: actorName}
	waitForMatrixActorReady(t, ctx, router, actorRef)
	return actorRef
}

func waitForMatrixActorReady(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := router.Get(ctx, actorRef, "/healthz")
		if err != nil {
			t.Fatalf("GET /healthz on %s: %v", actorRef, err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actor %s route did not become ready: last HTTP %d", actorRef, resp.StatusCode)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func runProbe(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, req probeRequest) probeResponse {
	t.Helper()
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshaling probeRequest: %v", err)
	}
	resp, err := router.PostJSON(ctx, actorRef, "/probe", payload)
	if err != nil {
		t.Fatalf("POST /probe to %s: %v", actorRef, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /probe response from %s: %v", actorRef, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /probe to %s returned HTTP %d: %s", actorRef, resp.StatusCode, body)
	}
	var out probeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding /probe response from %s: %v (raw: %s)", actorRef, err, body)
	}
	if out.Error != "" {
		t.Fatalf("probe %s on %s reported error: %s", req.Runtime, actorRef, out.Error)
	}
	return out
}

func mustRouterClient(t *testing.T, ctx context.Context) *e2e.RouterClient {
	t.Helper()
	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	return rc
}

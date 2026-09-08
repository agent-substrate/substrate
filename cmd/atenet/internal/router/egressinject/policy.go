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

package egressinject

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"

	"google.golang.org/grpc"

	"github.com/agent-substrate/substrate/internal/actorspiffe"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// credentialURIScheme is the only scheme a credential URI may carry.
const credentialURIScheme = "substrate-secret"

// policyClient is the subset of ateapipb.ControlClient the injector calls: it
// fetches a single actor's egress policy on demand. *ateapipb.NewControlClient
// satisfies it; tests supply a fake.
type policyClient interface {
	GetActorEgressPolicy(ctx context.Context, in *ateapipb.GetActorEgressPolicyRequest, opts ...grpc.CallOption) (*ateapipb.EgressPolicy, error)
}

// evaluate applies an actor's egress policy to a request bound for host,
// mirroring the control-plane's semantics: rules are evaluated in order and the
// first matching rule wins — only its effects apply and evaluation stops even if
// a later rule would also match. It reports whether a rule matched (which
// authorizes the request) separately from the injections to apply, because a
// matching rule may carry no injection.
func evaluate(policy *ateapipb.EgressPolicy, host string) (inject []*ateapipb.CredentialHeaderInjection, matched bool) {
	for _, r := range policy.GetRules() {
		if ruleMatches(r, host) {
			return r.GetHostnames().GetEffects().GetInjectStaticHeaders(), true
		}
	}
	return nil, false
}

// ruleMatches reports whether rule matches a request bound for host. The MITM
// leg matches on hostname, so an ip_blocks rule — which matches on the original
// destination IP, not available on this leg — never matches here.
func ruleMatches(rule *ateapipb.EgressRule, host string) bool {
	switch {
	case rule.GetAll() != nil:
		return true
	case rule.GetHostnames() != nil:
		for _, p := range rule.GetHostnames().GetPatterns() {
			if hostnameMatches(p, host) {
				return true
			}
		}
	}
	return false
}

// credentialURIClass returns the provider class of a substrate-secret:// URI —
// the URI host, e.g. "kubernetes.io" in
// substrate-secret://kubernetes.io/<provider>/<namespace>/<secret>. The injector
// uses it to confirm a URI targets the provider class it is configured to serve.
func credentialURIClass(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parsing credential URI %q: %w", raw, err)
	}
	if u.Scheme != credentialURIScheme {
		return "", fmt.Errorf("credential URI %q: scheme is %q, want %q", raw, u.Scheme, credentialURIScheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("credential URI %q: missing provider class", raw)
	}
	return u.Host, nil
}

// validateInjectHeader rejects header names the egress gateway's ext_proc
// mutation rules (disallow_system + disallow_is_error) would turn into a hard
// request failure: the HTTP/2 pseudo-headers and Host. The egress-policy API
// validates header format on write, so this is a defensive backstop.
func validateInjectHeader(name string) error {
	if name == "" {
		return fmt.Errorf("credential injection header is required")
	}
	if strings.HasPrefix(name, ":") || strings.EqualFold(name, "host") {
		return fmt.Errorf("credential injection header %q is a system header the gateway forbids mutating", name)
	}
	return nil
}

// hostnameMatches reports whether host matches pattern, following the egress
// policy API's HostnameRule semantics. A wildcard replaces the complete leftmost
// label: "*.example.com" matches exactly one non-empty label (e.g.
// "api.example.com") but not "example.com" or "a.b.example.com". Any other
// pattern is an exact, case-insensitive match.
func hostnameMatches(pattern, host string) bool {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	pattern = strings.ToLower(pattern)
	if parent, ok := strings.CutPrefix(pattern, "*."); ok {
		suffix := "." + parent
		label, matched := strings.CutSuffix(host, suffix)
		return matched && label != "" && !strings.Contains(label, ".")
	}
	return host == pattern
}

// hostFromAuthority strips any port from an :authority/Host value, returning the
// bare hostname the policy matches on.
func hostFromAuthority(authority string) string {
	if authority == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(authority); err == nil {
		return host
	}
	return authority
}

// parseActorURI extracts the atespace and actor name from an actor SPIFFE URI of
// the form spiffe://substrate-actor.local/atespace/<space>/actor/<name>.
func parseActorURI(raw string) (atespace, name string, err error) {
	return actorspiffe.Parse(raw)
}

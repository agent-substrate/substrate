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

// Package egresspolicy evaluates an Actor's EgressPolicy against a
// destination. ateapi validates patterns and ports with the same parsers the
// gateway matches with, so the two cannot drift.
//
// The gateway does not decide at the ClientHello yet: every TLS connection is
// intercepted, and a tls_passthrough rule matches nothing until it does. The
// rules are decided per request, on the authority, plus the SNI of the
// connection for https.
//
// The package is pure: no I/O, no logging.
package egresspolicy

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// Destination is what a request is going to, as far as the leg evaluating it
// can tell: the Hostname it named, or the IP when it named a literal, and the
// port the actor dialed.
type Destination struct {
	// Hostname is the normalized DNS name: lowercase ASCII, no trailing dot.
	// Empty when the destination was not named by a hostname.
	Hostname string
	// IP is the address named instead of a hostname, when it was. Only the
	// "*" pattern matches it.
	IP netip.Addr
	// Port is the port the actor dialed, when the leg knows it. Zero when it
	// does not, in which case a rule's ports are not enforced. A port in a
	// request's authority is not this.
	Port uint16
}

// Decision is the outcome of evaluating a policy against a Destination.
type Decision struct {
	// Allowed reports whether some rule authorized the destination.
	Allowed bool
	// RuleIndex is the index of the deciding rule in the policy, or -1 when
	// nothing matched.
	RuleIndex int
	// Effects are the effects of the deciding rule, when it declares any.
	// Nil otherwise.
	Effects *ateapipb.HttpRuleEffects
}

// Policy is an EgressPolicy with its patterns and ports parsed once, ready to
// be evaluated many times.
type Policy struct {
	rules []compiledRule
}

type protocol int

const (
	protocolHTTP protocol = iota + 1
	protocolHTTPS
	protocolTLSPassthrough
)

type compiledRule struct {
	protocol protocol
	patterns []HostnamePattern
	// ports the rule names; anyPort when it names "*".
	ports   []uint16
	anyPort bool
	effects *ateapipb.HttpRuleEffects
}

// Default ports of the rules that have one. A tls_passthrough rule must name
// its ports.
const (
	defaultHTTPPort  uint16 = 80
	defaultHTTPSPort uint16 = 443
)

// Compile parses every pattern and port in policy. ateapi validates with the
// same parsers, so nothing should fail here; an entry that does (an older
// ateapi accepted it) is dropped and reported, which fails closed because it
// only narrows an allow rule. The Policy is always usable.
func Compile(policy *ateapipb.EgressPolicy) (*Policy, []error) {
	var errs []error
	compiled := &Policy{}
	for i, rule := range policy.GetRules() {
		var (
			cr          compiledRule
			member      string
			patterns    string
			raw, ports  []string
			defaultPort uint16
		)
		switch {
		case rule.GetHttp() != nil:
			cr.protocol, cr.effects = protocolHTTP, rule.GetHttp().GetEffects()
			member, patterns = "http", "host_patterns"
			raw, ports, defaultPort = rule.GetHttp().GetHostPatterns(), rule.GetHttp().GetPorts(), defaultHTTPPort
		case rule.GetHttps() != nil:
			cr.protocol, cr.effects = protocolHTTPS, rule.GetHttps().GetEffects()
			member, patterns = "https", "host_patterns"
			raw, ports, defaultPort = rule.GetHttps().GetHostPatterns(), rule.GetHttps().GetPorts(), defaultHTTPSPort
		case rule.GetTlsPassthrough() != nil:
			cr.protocol = protocolTLSPassthrough
			member, patterns = "tls_passthrough", "sni_patterns"
			raw, ports = rule.GetTlsPassthrough().GetSniPatterns(), rule.GetTlsPassthrough().GetPorts()
		default:
			compiled.rules = append(compiled.rules, cr)
			continue
		}
		for _, entry := range raw {
			pattern, err := ParseHostnamePattern(entry)
			if err != nil {
				errs = append(errs, fmt.Errorf("rules[%d].%s.%s: %w", i, member, patterns, err))
				continue
			}
			cr.patterns = append(cr.patterns, pattern)
		}
		if len(ports) == 0 {
			if defaultPort == 0 {
				errs = append(errs, fmt.Errorf("rules[%d].%s.ports: required", i, member))
			} else {
				cr.ports = []uint16{defaultPort}
			}
		}
		for _, entry := range ports {
			if entry == "*" {
				cr.anyPort = true
				continue
			}
			port, err := ParsePort(entry)
			if err != nil {
				errs = append(errs, fmt.Errorf("rules[%d].%s.ports: %w", i, member, err))
				continue
			}
			cr.ports = append(cr.ports, port)
		}
		compiled.rules = append(compiled.rules, cr)
	}
	return compiled, errs
}

// RuleCount is the number of rules in the policy, dropped entries included.
// A policy with no rules can authorize nothing.
func (p *Policy) RuleCount() int { return len(p.rules) }

func (r compiledRule) matchesPort(port uint16) bool {
	return r.anyPort || slices.Contains(r.ports, port)
}

// EvaluateRequest decides one request the gateway can read, on the name or
// address in its authority and the port the actor dialed. decrypted selects
// the https rules, for a request the gateway terminated TLS for; otherwise
// the http rules apply. A tls_passthrough rule never decides a request.
func (p *Policy) EvaluateRequest(dest Destination, decrypted bool) Decision {
	want := protocolHTTP
	if decrypted {
		want = protocolHTTPS
	}
	best, bestRank := Decision{RuleIndex: -1}, matchRank{}
	for i, rule := range p.rules {
		if rule.protocol != want || (dest.Port != 0 && !rule.matchesPort(dest.Port)) {
			continue
		}
		name, ok := rule.matchName(dest)
		if !ok {
			continue
		}
		rank := matchRank{name: name, port: rule.portRank()}
		if best.RuleIndex == -1 || rank.beats(bestRank) {
			best, bestRank = Decision{Allowed: true, RuleIndex: i, Effects: rule.effects}, rank
		}
	}
	return best
}

// matchName reports whether any pattern matches dest, and how specific the
// best one is. A destination named by an IP literal is matched by "*" alone.
func (r compiledRule) matchName(dest Destination) (int, bool) {
	best, found := 0, false
	for _, pattern := range r.patterns {
		if !pattern.Matches(dest.Hostname) && !(pattern.any && dest.IP.IsValid()) {
			continue
		}
		if rank := pattern.rank(); !found || rank < best {
			best, found = rank, true
		}
	}
	return best, found
}

// Pattern ranks, most specific first. The API only distinguishes a pattern
// with a wildcard from one without; "*" ranks below a labeled wildcard so the
// two never tie.
const (
	rankExact = iota
	rankWildcard
	rankAny
)

// portRank is 0 for a rule that names its ports and 1 for "*".
func (r compiledRule) portRank() int {
	if r.anyPort {
		return 1
	}
	return 0
}

// matchRank orders matches as the API describes: the name first, then the
// port, lower is more specific. Rules that tie keep policy order.
type matchRank struct{ name, port int }

func (m matchRank) beats(o matchRank) bool {
	if m.name != o.name {
		return m.name < o.name
	}
	return m.port < o.port
}

// HostnamePattern is one parsed host or SNI pattern: an exact name, a
// wildcard standing in for the whole leftmost label, or "*" for every name
// and every address.
type HostnamePattern struct {
	// name is the exact name, or the suffix after "*." for a wildcard.
	name     string
	wildcard bool
	any      bool
	// dotSuffix is "." + name, built once so Matches does not allocate per call.
	dotSuffix string
}

// ParseHostnamePattern parses a host or SNI pattern. A pattern is a lowercase
// DNS-1123 subdomain, optionally prefixed with "*." to match exactly one
// non-empty leftmost label, or "*" alone to match every name. IP literals,
// ports, URLs, trailing dots, and any other placement of "*" are rejected.
func ParseHostnamePattern(raw string) (HostnamePattern, error) {
	if raw == "*" {
		return HostnamePattern{any: true}, nil
	}
	name, wildcard := strings.CutPrefix(raw, "*.")
	if !isHostname(name) {
		return HostnamePattern{}, fmt.Errorf("%q is not a valid hostname pattern", raw)
	}
	pattern := HostnamePattern{name: name, wildcard: wildcard}
	if wildcard {
		pattern.dotSuffix = "." + name
	}
	return pattern, nil
}

// String returns the pattern in the form it was written.
func (p HostnamePattern) String() string {
	switch {
	case p.any:
		return "*"
	case p.wildcard:
		return "*." + p.name
	}
	return p.name
}

// Matches reports whether hostname, already normalized as by
// NormalizeAuthority, matches the pattern. "*.example.com" matches
// "api.example.com" but neither "example.com" nor "a.b.example.com"; "*"
// matches every name.
func (p HostnamePattern) Matches(hostname string) bool {
	switch {
	case hostname == "":
		return false
	case p.any:
		return true
	case !p.wildcard:
		return hostname == p.name
	}
	label, found := strings.CutSuffix(hostname, p.dotSuffix)
	return found && label != "" && !strings.Contains(label, ".")
}

func (p HostnamePattern) rank() int {
	switch {
	case p.any:
		return rankAny
	case p.wildcard:
		return rankWildcard
	}
	return rankExact
}

// ParsePort parses one ports entry other than "*": a decimal port number from
// 1 to 65535, with no sign, leading zeros, or surrounding space.
func ParsePort(raw string) (uint16, error) {
	n, err := strconv.ParseUint(raw, 10, 16)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != raw {
		return 0, fmt.Errorf("%q is not a port number", raw)
	}
	return uint16(n), nil
}

// NormalizeAuthority turns an :authority or Host value into a Destination:
// port split off, IP literal to IP, DNS name lowercased with one trailing dot
// removed and checked as a DNS-1123 subdomain. Anything else is an error, and
// the caller should deny.
func NormalizeAuthority(authority string) (Destination, error) {
	if authority == "" {
		return Destination{}, errors.New("authority is empty")
	}
	host := authority
	var port uint16
	if h, p, err := net.SplitHostPort(authority); err == nil {
		n, err := strconv.ParseUint(p, 10, 16)
		if err != nil || n == 0 {
			return Destination{}, fmt.Errorf("authority %q has an invalid port", authority)
		}
		host, port = h, uint16(n)
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	if addr, err := netip.ParseAddr(host); err == nil {
		if addr.Zone() != "" {
			return Destination{}, fmt.Errorf("authority %q has an IPv6 zone", authority)
		}
		return Destination{IP: addr.Unmap(), Port: port}, nil
	}

	name := lowerASCII(strings.TrimSuffix(host, "."))
	if !isHostname(name) {
		return Destination{}, fmt.Errorf("authority %q is neither a DNS hostname nor an IP literal", authority)
	}
	return Destination{Hostname: name, Port: port}, nil
}

// isHostname reports whether name is a lowercase DNS-1123 subdomain whose last
// label is not all digits. RFC 1123 section 2.1 requires that, and it is what
// keeps a dotted-decimal address from passing as a name, including spellings
// like "01.2.3.4" that netip rejects but resolvers accept. IPv6 literals fail
// the subdomain check on their own.
func isHostname(name string) bool {
	if len(content.IsDNS1123Subdomain(name)) != 0 {
		return false
	}
	return !allDigits(name[strings.LastIndexByte(name, '.')+1:])
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// lowerASCII folds A-Z only. strings.ToLower would also fold non-ASCII onto
// ASCII letters (U+212A KELVIN SIGN onto "k") and let a non-ASCII spelling
// match a pattern for a different name.
func lowerASCII(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

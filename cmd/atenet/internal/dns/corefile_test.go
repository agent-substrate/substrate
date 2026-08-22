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

package dns

import (
	"strings"
	"testing"
)

// actorMatchDirective is the match line every template that scopes itself to
// real actor names carries; soaAuthorityDirective is the record that makes the
// negative answers cacheable. Both are spelled out rather than built from
// resources.ResourceNameRegexPattern and ActorDNSSuffix: the rendered zone is a
// wire contract, so a change to either constant should fail here instead of
// being tracked silently.
const (
	actorMatchDirective   = `match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\.actors\.resources\.substrate\.ate\.dev\.$"`
	soaAuthorityDirective = `authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"`
)

// The zones below are compared whole rather than by substring because every
// part of them is behavior: templates are evaluated in Corefile order, every
// block carrying a "match" needs a "fallthrough" to reach the blocks after it,
// the catch-all must be last and must not declare one, and the indentation has
// to parse. See README.md for what the template plugin does with each.
//
// The head and tail are shared to keep the four goldens readable. That does not
// weaken the ordering assertion: each golden is still the whole expected file,
// with the address blocks spelled out between them.
const (
	wantZoneHead = `actors.resources.substrate.ate.dev:53 {
  log
  errors
  health :8080
  ready :8181
  reload
`
	wantZoneTail = `  template ANY ANY actors.resources.substrate.ate.dev {
    ` + actorMatchDirective + `
    rcode NOERROR
    ` + soaAuthorityDirective + `
    fallthrough
  }
  template ANY ANY actors.resources.substrate.ate.dev {
    rcode NXDOMAIN
    ` + soaAuthorityDirective + `
  }
}
`
)

const (
	wantZoneIPv4 = wantZoneHead + `  template IN A actors.resources.substrate.ate.dev {
    ` + actorMatchDirective + `
    answer "{{ .Name }} 60 IN A 10.240.0.10"
    fallthrough
  }
` + wantZoneTail

	wantZoneIPv6 = wantZoneHead + `  template IN AAAA actors.resources.substrate.ate.dev {
    ` + actorMatchDirective + `
    answer "{{ .Name }} 60 IN AAAA fd00:10:96::8857"
    fallthrough
  }
` + wantZoneTail

	wantZoneDualStack = wantZoneHead + `  template IN A actors.resources.substrate.ate.dev {
    ` + actorMatchDirective + `
    answer "{{ .Name }} 60 IN A 10.96.233.69"
    fallthrough
  }
  template IN AAAA actors.resources.substrate.ate.dev {
    ` + actorMatchDirective + `
    answer "{{ .Name }} 60 IN AAAA fd00:10:96::7373"
    fallthrough
  }
` + wantZoneTail

	wantZoneNoAddresses = wantZoneHead + wantZoneTail
)

// zoneBody strips the "# Generated at <timestamp>" header.
func zoneBody(t *testing.T, corefile string) string {
	t.Helper()
	header, body, ok := strings.Cut(corefile, "\n")
	if !ok || !strings.HasPrefix(header, "# Generated at ") {
		t.Fatalf("makeCoreFile() has no generated-at header, got first line %q", header)
	}
	return body
}

func TestMakeCoreFile(t *testing.T) {
	tests := []struct {
		name     string
		routerV4 string
		routerV6 string
		want     string
	}{
		{
			// AAAA is left to the NODATA template in the tail, which is the
			// right answer for a name with no address of that type. Publishing
			// the v4 ClusterIP as an AAAA instead would render a literal RR that
			// loads clean and then fails dns.NewRR on every query.
			name:     "IPv4 only",
			routerV4: "10.240.0.10",
			want:     wantZoneIPv4,
		},
		{
			// The bug this change exists for: a v6-only cluster's sole ClusterIP
			// used to be published as an A record, SERVFAILing the whole zone.
			name:     "IPv6 only",
			routerV6: "fd00:10:96::8857",
			want:     wantZoneIPv6,
		},
		{
			name:     "dual stack",
			routerV4: "10.96.233.69",
			routerV6: "fd00:10:96::7373",
			want:     wantZoneDualStack,
		},
		{
			// The controller does not call makeCoreFile in this state, but the
			// zone still has to be a loadable Corefile if it ever does: negative
			// answers only, never a template with an empty address in it.
			name: "no addresses",
			want: wantZoneNoAddresses,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := zoneBody(t, makeCoreFile(tc.routerV4, tc.routerV6))
			if got != tc.want {
				t.Errorf("makeCoreFile(%q, %q) rendered an unexpected Corefile\nGot:\n%s\nWant:\n%s", tc.routerV4, tc.routerV6, got, tc.want)
			}
		})
	}
}

// TestMakeCoreFileStable pins the property that keeps the reconcile loop quiet:
// the render depends only on its arguments. reconcileCoreDNSConfig rewrites the
// Corefile and signals CoreDNS whenever the render differs from what is on
// disk, so anything time-varying in the output -- the "Generated at" stamp, in
// particular -- would reload the DNS server on every tick.
func TestMakeCoreFileStable(t *testing.T) {
	first := makeCoreFile("10.240.0.10", "fd00:10:96::8857")
	second := makeCoreFile("10.240.0.10", "fd00:10:96::8857")
	if first != second {
		t.Errorf("makeCoreFile() is not stable across calls; the reconcile loop would rewrite and reload every tick\nFirst:\n%s\nSecond:\n%s", first, second)
	}
}

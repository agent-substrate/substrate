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
	"fmt"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	fallthroughDirective = "  fallthrough"
	soaDirective         = `  authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"`
)

// generatedAt stamps the rendered Corefile once per process, and must not be
// recomputed per call: reconcileCoreDNSConfig decides whether to rewrite the
// file and signal CoreDNS by comparing the render against what is on disk, so a
// moving timestamp would reload the server on every tick of the reconcile loop.
var generatedAt = time.Now()

// makeCoreFile renders the actor zone for the router Service's ClusterIPs.
//
// A family gets an address template only when the router actually has an
// address in it. That is not an optimization: an address template is a literal
// RR, so emitting `IN A <v6 address>` on a v6-only cluster produces a Corefile
// that loads clean and then fails dns.NewRR on every query, turning the whole
// zone into SERVFAIL. Omitting the block instead leaves the family to the
// NODATA template below, which is the correct answer for a name with no address
// of that type.
//
// Either argument may be empty, and on any cluster where ipFamilyPolicy is
// unset exactly one of them will be.
func makeCoreFile(routerV4, routerV6 string) string {
	// Build up the Corefile programmatically to make it easier to understand.
	var directives []string
	// Plugins to enable.
	directives = append(directives, "log")
	directives = append(directives, "errors")
	directives = append(directives, "health :8080")
	directives = append(directives, "ready :8181")
	directives = append(directives, "reload")

	// Construct match pattern for <ActorName>.<atespace>.<dnsDomain>. Both the
	// actor name and the atespace are DNS-1123 labels (same regex).
	// Escape the suffix's dots so they match literally; the final \. matches the FQDN's trailing dot.
	escapedSuffix := strings.ReplaceAll(resources.ActorDNSSuffix, ".", `\.`)
	actorMatch := fmt.Sprintf(`  match "^%s\.%s\.%s\.$"`, resources.ResourceNameRegexPattern, resources.ResourceNameRegexPattern, escapedSuffix)

	if routerV4 != "" {
		directives = append(directives, addressTemplate("A", routerV4, actorMatch)...)
	}
	if routerV6 != "" {
		directives = append(directives, addressTemplate("AAAA", routerV6, actorMatch)...)
	}

	// Valid actor names return NOERROR (NODATA) for the qtypes not answered
	// above, which includes the family the router has no address in.
	directives = append(directives, fmt.Sprintf("template ANY ANY %s {", resources.ActorDNSSuffix))
	directives = append(directives, actorMatch)
	directives = append(directives, "  rcode NOERROR")
	directives = append(directives, soaDirective)
	directives = append(directives, fallthroughDirective)
	directives = append(directives, "}")

	// Returns rcode NXDOMAIN (Non-Existent Domain) for any query that did not
	// match the valid actor regex in the previous blocks.
	// TODO(#922): answer empty non-terminals with NODATA.
	directives = append(directives, fmt.Sprintf("template ANY ANY %s {", resources.ActorDNSSuffix))
	directives = append(directives, "  rcode NXDOMAIN")
	directives = append(directives, soaDirective)
	directives = append(directives, "}")

	// Generate the Corefile.
	b := strings.Builder{}
	fmt.Fprintf(&b, "# Generated at %s\n", generatedAt)
	fmt.Fprintf(&b, "%s:53 {\n  ", resources.ActorDNSSuffix)
	fmt.Fprint(&b, strings.Join(directives, "\n  "))
	fmt.Fprint(&b, "\n}\n")

	return b.String()
}

// addressTemplate returns the template block that answers qtype ("A" or "AAAA")
// for actor names with addr. addr is interpolated into an RR verbatim, so it
// must already be known to be an address of that family -- see
// ipfamily.ClusterIPsByFamily, which is where callers get it.
func addressTemplate(qtype, addr, actorMatch string) []string {
	return []string{
		fmt.Sprintf("template IN %s %s {", qtype, resources.ActorDNSSuffix),
		actorMatch,
		fmt.Sprintf(`  answer "{{ .Name }} 60 IN %s %s"`, qtype, addr),
		fallthroughDirective,
		"}",
	}
}

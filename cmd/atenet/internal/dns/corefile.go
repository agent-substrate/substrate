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

// corefileTemplate is a Sprintf template for the CoreDNS configuration.
var corefileTemplate string

func init() {
	corefileTemplate = buildTemplate()
}

func buildTemplate() string {
	const (
		fallthroughDirective = "  fallthrough"
		soaDirective         = `  authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"`
	)

	// Build up the corefileTemplate programmatically to make it easier to understand.
	var directives []string
	// Plugins to enable.
	directives = append(directives, "log")
	directives = append(directives, "errors")
	directives = append(directives, "health :8080")
	directives = append(directives, "ready :8181")
	directives = append(directives, "reload")

	// Construct match pattern for <ActorName>.<atespace>.<dnsDomain>. Both the
	// actor name and the atespace are DNS-1123 labels (same regex).
	directives = append(directives, fmt.Sprintf("template IN A %s {", resources.ActorDNSSuffix))
	// Escape the suffix's dots so they match literally; the final \. matches the FQDN's trailing dot.
	escapedSuffix := strings.ReplaceAll(resources.ActorDNSSuffix, ".", `\.`)
	actorMatch := fmt.Sprintf(`  match "^%s\.%s\.%s\.$"`, resources.ResourceNameRegexPattern, resources.ResourceNameRegexPattern, escapedSuffix)
	directives = append(directives, actorMatch)
	// Note the %s -- this will be filled with the router IP.
	directives = append(directives, `  answer "{{ .Name }} 60 IN A %s"`)
	directives = append(directives, fallthroughDirective)
	directives = append(directives, "}")

	// Valid actor names return NOERROR (NODATA) for non-A queries.
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

	// Generate the template.
	b := strings.Builder{}
	fmt.Fprintf(&b, "# Generated at %s\n", time.Now())
	fmt.Fprintf(&b, "%s:53 {\n  ", resources.ActorDNSSuffix)
	fmt.Fprint(&b, strings.Join(directives, "\n  "))
	fmt.Fprint(&b, "\n}\n")

	return b.String()
}

func makeCoreFile(routerIP string) string {
	return fmt.Sprintf(corefileTemplate, routerIP)
}

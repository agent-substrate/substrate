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
	// Escape the suffix's dots so they match literally; the final \\. matches the FQDN's trailing dot.
	escapedSuffix := strings.ReplaceAll(resources.ActorDNSSuffix, ".", `\.`)
	directives = append(directives, fmt.Sprintf(`  match "^%s\.%s\.%s\.$"`, resources.ResourceNameRegexPattern, resources.ResourceNameRegexPattern, escapedSuffix))
	// Note the %s -- this will be filled with the router IP.
	directives = append(directives, `  answer "{{ .Name }} 60 IN A %s"`)
	directives = append(directives, "}")

	// Non-A queries for valid actor names must return NODATA (NOERROR with an
	// empty answer section), not SERVFAIL. CoreDNS's template plugin returns
	// SERVFAIL when a template's zone matches a query name but no template
	// block matches the query type (see plugin/template README: "Without
	// fallthrough, when the template's ZONE matches a query but no regex match
	// then a SERVFAIL response is returned"). Strict POSIX resolvers query A
	// and AAAA in parallel and treat SERVFAIL as a hard failure, breaking
	// intra-cluster resolution for actor images that don't have AAAA records.
	//
	// template IN ANY matches every type, and specifying no answer results in
	// a response with an empty answer section and the default rcode NOERROR —
	// exactly the NODATA response RFC 8020 expects for a name that exists but
	// has no records of the requested type.
	directives = append(directives, fmt.Sprintf("template IN ANY %s {", resources.ActorDNSSuffix))
	directives = append(directives, fmt.Sprintf(`  match "^%s\.%s\.%s\.$"`, resources.ResourceNameRegexPattern, resources.ResourceNameRegexPattern, escapedSuffix))
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

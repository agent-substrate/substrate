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

	// Construct match pattern for <ActorID>.<dnsDomain>.
	directives = append(directives, fmt.Sprintf("template IN A %s {", resources.ActorDNSSuffix))
	dnsDomainParts := strings.Split("."+resources.ActorDNSSuffix+".", ".")
	dnsDomainRef := strings.Join(dnsDomainParts, `\.`)
	directives = append(directives, fmt.Sprintf(`  match "^%s%s$"`, resources.ActorIDRegexPattern, dnsDomainRef))
	// Note the %s -- this will be filled with the router IP.
	directives = append(directives, `  answer "{{ .Name }} 60 IN A %s"`)
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

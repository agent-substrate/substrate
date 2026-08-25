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
	"testing"
)

// Spelled out rather than built from resources.ResourceNameRegexPattern and
// ActorDNSSuffix: the rendered zone is a wire contract, so a change to either
// constant should fail here instead of being tracked silently.
const wantCorefileFmt = `actors.resources.substrate.ate.dev:53 {
  log
  errors
  health :8080
  ready :8181
  reload
  template IN A actors.resources.substrate.ate.dev {
    match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\.actors\.resources\.substrate\.ate\.dev\.$"
    answer "{{ .Name }} 60 IN A %s"
    fallthrough
  }
  template ANY ANY actors.resources.substrate.ate.dev {
    match "^[a-z0-9]([-a-z0-9]*[a-z0-9])?\.[a-z0-9]([-a-z0-9]*[a-z0-9])?\.actors\.resources\.substrate\.ate\.dev\.$"
    rcode NOERROR
    authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"
    fallthrough
  }
  template ANY ANY actors.resources.substrate.ate.dev {
    rcode NXDOMAIN
    authority "{{ .Zone }} 60 IN SOA ns.dns.{{ .Zone }} hostmaster.{{ .Zone }} (1 60 60 60 60)"
  }
}
`

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
		routerIP string
	}{
		{name: "cluster IP", routerIP: "10.240.0.10"},
		{name: "different cluster IP", routerIP: "192.168.1.1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := zoneBody(t, makeCoreFile(tc.routerIP))
			want := fmt.Sprintf(wantCorefileFmt, tc.routerIP)
			if got != want {
				t.Errorf("makeCoreFile(%q) rendered an unexpected Corefile\nGot:\n%s\nWant:\n%s", tc.routerIP, got, want)
			}
		})
	}
}

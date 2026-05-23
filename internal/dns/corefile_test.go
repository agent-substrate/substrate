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

	"github.com/agent-substrate/substrate/internal/resources"
)

func TestMakeCoreFile(t *testing.T) {
	tests := []struct {
		name     string
		routerIP string
		expected []string
	}{
		{
			name:     "standard local IP",
			routerIP: "10.240.0.10",
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				"log",
				"errors",
				"health :8080",
				"ready :8181",
				"reload",
				"template IN A actors.resources.substrate.ate.dev {",
				`match "^` + resources.ActorIDRegexPattern + `\.actors\.resources\.substrate\.ate\.dev\.$"`,
				`answer "{{ .Name }} 60 IN A 10.240.0.10"`,
			},
		},
		{
			name:     "different IP",
			routerIP: "192.168.1.1",
			expected: []string{
				"actors.resources.substrate.ate.dev:53 {",
				`answer "{{ .Name }} 60 IN A 192.168.1.1"`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := makeCoreFile(tc.routerIP)
			for _, exp := range tc.expected {
				if !strings.Contains(got, exp) {
					t.Errorf("makeCoreFile(%q) missing expected substring %q\nGot:\n%s", tc.routerIP, exp, got)
				}
			}
		})
	}
}

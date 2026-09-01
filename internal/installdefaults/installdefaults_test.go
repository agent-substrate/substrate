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

package installdefaults

import "testing"

func TestAteletSPIFFEID(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		want      string
	}{
		{
			// The canonical install. Peers reject any other string, so this
			// value is effectively wire format: changing it breaks the atelet
			// mTLS handshake for every existing deployment.
			name:      "default namespace",
			namespace: SystemNamespace,
			want:      "spiffe://cluster.local/ns/ate-system/sa/atelet",
		},
		{
			name:      "namespace the install was relocated to",
			namespace: "team-a-substrate",
			want:      "spiffe://cluster.local/ns/team-a-substrate/sa/atelet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AteletSPIFFEID(tt.namespace); got != tt.want {
				t.Errorf("AteletSPIFFEID(%q) = %q, want %q", tt.namespace, got, tt.want)
			}
		})
	}
}

func TestRouterSPIFFEID(t *testing.T) {
	// Matches the --atunnel-client-identity default the ateom binaries ship
	// with, which is what actor ingress authenticates the router against.
	const want = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
	if got := RouterSPIFFEID(SystemNamespace); got != want {
		t.Errorf("RouterSPIFFEID(%q) = %q, want %q", SystemNamespace, got, want)
	}
}

func TestNamespaceFromPodEnv(t *testing.T) {
	t.Run("falls back to the install default when unset", func(t *testing.T) {
		t.Setenv(PodNamespaceEnv, "")
		if got := NamespaceFromPodEnv(); got != SystemNamespace {
			t.Errorf("NamespaceFromPodEnv() = %q, want %q", got, SystemNamespace)
		}
	})

	t.Run("prefers the downward API value", func(t *testing.T) {
		t.Setenv(PodNamespaceEnv, "team-a-substrate")
		if got := NamespaceFromPodEnv(); got != "team-a-substrate" {
			t.Errorf("NamespaceFromPodEnv() = %q, want %q", got, "team-a-substrate")
		}
	})
}

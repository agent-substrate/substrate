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

package extproc

import (
	"strings"
	"testing"
)

func TestAttributeKeys(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"actor name filter state", ActorNameFilterStateKey, "dev.ate.actor.name"},
		{"actor name attribute", ActorNameFilterStateAttribute, "filter_state['dev.ate.actor.name']"},
		{"atespace filter state", AtespaceFilterStateKey, "dev.ate.actor.atespace"},
		{"atespace attribute", AtespaceFilterStateAttribute, "filter_state['dev.ate.actor.atespace']"},
		{"CONNECT authority filter state", ConnectAuthorityFilterStateKey, "dev.ate.connect.authority"},
		{"CONNECT authority attribute", ConnectAuthorityFilterStateAttribute, "filter_state['dev.ate.connect.authority']"},
		{"actor identity filter state", ActorIdentityFilterStateKey, "dev.ate.actor.identity"},
		{"direction attribute", directionAttribute, "dev.ate.extproc.direction"},
		{"filter chain name attribute", FilterChainNameAttribute, "xds.filter_chain_name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("key = %q, want %q; update dataplane configuration to match", tt.got, tt.want)
			}
		})
	}
}

func TestSubstrateKeysShareOnePrefix(t *testing.T) {
	const prefix = "dev.ate."
	for _, key := range []string{
		ActorNameFilterStateKey,
		AtespaceFilterStateKey,
		ConnectAuthorityFilterStateKey,
		ActorIdentityFilterStateKey,
		directionAttribute,
	} {
		if !strings.HasPrefix(key, prefix) {
			t.Errorf("key %q is not rooted at %q", key, prefix)
		}
		if strings.Contains(key, "/") {
			t.Errorf("key %q uses the ate.dev/ Kubernetes label form", key)
		}
	}
}

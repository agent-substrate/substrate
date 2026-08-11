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

package ingress

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
)

func TestParseActorRef(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		want    resources.ActorRef
		wantErr bool
	}{
		{
			name:    "valid host without port",
			host:    "my-actor.team-a.actors.resources.substrate.ate.dev",
			want:    resources.ActorRef{Atespace: "team-a", Name: "my-actor"},
			wantErr: false,
		},
		{
			name:    "valid host with port",
			host:    "my-actor.team-a.actors.resources.substrate.ate.dev:8443",
			want:    resources.ActorRef{Atespace: "team-a", Name: "my-actor"},
			wantErr: false,
		},
		{
			name:    "valid host with trailing dot",
			host:    "my-actor.team-a.actors.resources.substrate.ate.dev.",
			want:    resources.ActorRef{Atespace: "team-a", Name: "my-actor"},
			wantErr: false,
		},
		{
			name:    "valid host with trailing dot and port",
			host:    "my-actor.team-a.actors.resources.substrate.ate.dev.:8080",
			want:    resources.ActorRef{Atespace: "team-a", Name: "my-actor"},
			wantErr: false,
		},
		{
			name:    "missing atespace label",
			host:    "my-actor.actors.resources.substrate.ate.dev",
			wantErr: true,
		},
		{
			name:    "invalid suffix",
			host:    "my-actor.team-a.example.com",
			wantErr: true,
		},
		{
			name:    "invalid host port format",
			host:    "my-actor.team-a.actors.resources.substrate.ate.dev:invalid:port",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseActorRef(tc.host)
			if (err != nil) != tc.wantErr {
				t.Errorf("parseActorRef(%q) error = %v, wantErr %v", tc.host, err, tc.wantErr)
				return
			}
			if got != tc.want {
				t.Errorf("parseActorRef(%q) = %+v, want %+v", tc.host, got, tc.want)
			}
		})
	}
}

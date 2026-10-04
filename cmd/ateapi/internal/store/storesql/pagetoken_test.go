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

package storesql

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
)

func TestDecodePageToken(t *testing.T) {
	encodeRaw := func(token PageToken) string {
		b, err := json.Marshal(token)
		if err != nil {
			t.Fatalf("marshaling token: %v", err)
		}
		return base64.StdEncoding.EncodeToString(b)
	}
	teamAActors := EncodePageToken(KindActor, "team-a", []string{"a1"})

	tests := []struct {
		name      string
		token     string
		kind      Kind
		scope     string
		keyParts  int
		wantError bool
	}{
		{name: "matching kind and scope", token: teamAActors, kind: KindActor, scope: "team-a", keyParts: 1},
		{name: "empty token starts the list", token: "", kind: KindActor, scope: "team-a", keyParts: 1},
		{name: "other scope", token: teamAActors, kind: KindActor, scope: "team-b", keyParts: 1, wantError: true},
		{name: "other kind", token: teamAActors, kind: KindActorTemplate, scope: "team-a", keyParts: 1, wantError: true},
		{name: "wrong key shape", token: EncodePageToken(KindActor, "", []string{"only-an-atespace"}), kind: KindActor, scope: "", keyParts: 2, wantError: true},
		{name: "unsupported version", token: encodeRaw(PageToken{Version: pageTokenVersion + 1, Kind: KindActor, Scope: "team-a", Last: []string{"a1"}}), kind: KindActor, scope: "team-a", keyParts: 1, wantError: true},
		{name: "malformed json", token: base64.StdEncoding.EncodeToString([]byte("{")), kind: KindActor, scope: "", keyParts: 2, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodePageToken(tt.token, tt.kind, tt.scope, tt.keyParts)
			if tt.wantError {
				if !errors.Is(err, store.ErrInvalidPageToken) {
					t.Errorf("DecodePageToken() error = %v, want ErrInvalidPageToken", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodePageToken() error = %v", err)
			}
			if got.Kind != tt.kind || got.Scope != tt.scope {
				t.Errorf("DecodePageToken() = %+v, want kind %q scope %q", got, tt.kind, tt.scope)
			}
		})
	}
}

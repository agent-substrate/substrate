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

package actoridjwt

import (
	"encoding/json"
	"testing"
	"time"
)

func TestClaimsToWire_ClaimNamePerKind(t *testing.T) {
	base := Claims{
		Issuer:     "https://idp.ate-system.svc",
		Subject:    "x",
		Audiences:  []string{"a"},
		Expiration: time.Unix(1_800_000_000, 0),
		Substrate:  SubstrateClaims{Atespace: "team-a", ActorName: "my-actor", ActorUID: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"},
	}
	for _, tc := range []struct {
		kind       Kind
		wantClaim  string
		otherClaim string
	}{
		{KindActor, "actor.ate.dev", "worker-for-actor.ate.dev"},
		{KindWorkerForActor, "worker-for-actor.ate.dev", "actor.ate.dev"},
	} {
		claims := base
		claims.Kind = tc.kind
		wire, err := ClaimsToWire(&claims)
		if err != nil {
			t.Fatalf("ClaimsToWire(%v) failed: %v", tc.kind, err)
		}
		payload, err := json.Marshal(wire)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(payload, &raw); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if _, ok := raw[tc.wantClaim]; !ok {
			t.Errorf("kind %v: claim %q missing from %s", tc.kind, tc.wantClaim, payload)
		}
		for _, absent := range []string{tc.otherClaim, "ate.dev"} {
			if _, ok := raw[absent]; ok {
				t.Errorf("kind %v: unexpected claim %q in %s", tc.kind, absent, payload)
			}
		}
		var sub WireSubstrateClaims
		if err := json.Unmarshal(raw[tc.wantClaim], &sub); err != nil {
			t.Fatalf("kind %v: decode %q: %v", tc.kind, tc.wantClaim, err)
		}
		if want := (WireSubstrateClaims{Atespace: "team-a", ActorName: "my-actor", ActorUID: "3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"}); sub != want {
			t.Errorf("kind %v: %q = %+v, want %+v", tc.kind, tc.wantClaim, sub, want)
		}
	}
}

func TestClaimsToWire_NodeOnWorkerTokensOnly(t *testing.T) {
	claims := Claims{
		Kind:      KindWorkerForActor,
		Subject:   "x",
		Audiences: []string{"a"},
		Substrate: SubstrateClaims{Atespace: "team-a", ActorName: "my-actor", ActorUID: "u", Node: "node-1"},
	}
	wire, err := ClaimsToWire(&claims)
	if err != nil {
		t.Fatal(err)
	}
	if got := wire.WorkerForActor.Node; got != "node-1" {
		t.Errorf("worker-for-actor node = %q, want node-1", got)
	}
	claims.Kind = KindActor
	if _, err := ClaimsToWire(&claims); err == nil {
		t.Error("ClaimsToWire accepted a node on an actor's own token")
	}
}

func TestClaimsToWire_RejectsUnspecifiedKind(t *testing.T) {
	for _, kind := range []Kind{KindUnspecified, Kind(99)} {
		if _, err := ClaimsToWire(&Claims{Kind: kind, Subject: "x", Audiences: []string{"a"}}); err == nil {
			t.Errorf("ClaimsToWire(kind %v) succeeded, want error", kind)
		}
	}
}

func TestWireClaims_Substrate(t *testing.T) {
	actor := &WireSubstrateClaims{Atespace: "a", ActorName: "n", ActorUID: "u"}
	worker := &WireSubstrateClaims{Atespace: "a2", ActorName: "n2", ActorUID: "u2"}
	for _, tc := range []struct {
		name       string
		wire       WireClaims
		wantOK     bool
		wantKind   Kind
		wantClaims *WireSubstrateClaims
	}{
		{name: "neither claim", wire: WireClaims{}},
		{name: "both claims", wire: WireClaims{Actor: actor, WorkerForActor: worker}},
		{name: "actor", wire: WireClaims{Actor: actor}, wantOK: true, wantKind: KindActor, wantClaims: actor},
		{name: "worker-for-actor", wire: WireClaims{WorkerForActor: worker}, wantOK: true, wantKind: KindWorkerForActor, wantClaims: worker},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kind, claims, ok := tc.wire.Substrate()
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if kind != tc.wantKind {
				t.Errorf("kind = %v, want %v", kind, tc.wantKind)
			}
			if claims != tc.wantClaims {
				t.Errorf("claims = %+v, want %+v", claims, tc.wantClaims)
			}
		})
	}
}

func TestKind_Names(t *testing.T) {
	for _, tc := range []struct {
		kind          Kind
		prefix, claim string
	}{
		{KindUnspecified, "", ""},
		{KindActor, "actor", "actor.ate.dev"},
		{KindWorkerForActor, "worker-for-actor", "worker-for-actor.ate.dev"},
	} {
		if got := tc.kind.SubjectPrefix(); got != tc.prefix {
			t.Errorf("Kind(%d).SubjectPrefix() = %q, want %q", tc.kind, got, tc.prefix)
		}
		if got := tc.kind.ClaimName(); got != tc.claim {
			t.Errorf("Kind(%d).ClaimName() = %q, want %q", tc.kind, got, tc.claim)
		}
	}
}

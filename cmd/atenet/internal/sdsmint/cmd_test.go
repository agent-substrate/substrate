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

package sdsmint

import (
	"testing"
	"time"
)

func TestValidateTTL(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{name: "default", ttl: defaultTTL},
		{name: "deployed value", ttl: 15 * time.Minute},
		{name: "the load-test value", ttl: 5 * time.Minute},
		// The edges of validateTTL's accepted band, spelled out because the
		// bounds are local to it. Both are inclusive.
		{name: "at the band floor", ttl: 2 * time.Minute},
		{name: "at the band ceiling", ttl: 24 * time.Hour},
		// Just under the floor, which the band used to accept.
		{name: "one minute", ttl: time.Minute, wantErr: true},

		// The whole point of the exercise: --leaf-cert-ttl=0 used to start a server
		// that logged 0 and issued defaultTTL leaves.
		{name: "zero", ttl: 0, wantErr: true},
		{name: "negative", ttl: -time.Minute, wantErr: true},

		// Outside the band. These used to start the server with a warning; a
		// TTL that no longer means what it says is now refused outright.
		{name: "short", ttl: 30 * time.Second, wantErr: true},
		{name: "long", ttl: 48 * time.Hour, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := config{LeafCertTTL: tc.ttl}.validateTTL()
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("validateTTL(ttl=%s) = %v; want an error = %v", tc.ttl, err, tc.wantErr)
			}
		})
	}
}

// TestDefaultTTLIsTheFlagDefault guards the trap this validation was added
// for: a fallback and a flag default that disagree, so the lifetime depends on
// which path you came in through.
func TestDefaultTTLIsTheFlagDefault(t *testing.T) {
	flag := NewSdsmintCmd().Flags().Lookup("leaf-cert-ttl")
	if flag == nil {
		t.Fatal("no --leaf-cert-ttl flag")
	}
	if got, want := flag.DefValue, defaultTTL.String(); got != want {
		t.Errorf("--leaf-cert-ttl default = %q; want %q, the same constant newMinter and newServer fall back to", got, want)
	}
	if err := (config{LeafCertTTL: defaultTTL}).validateTTL(); err != nil {
		t.Errorf("validateTTL(defaultTTL) = %v; the default has to be a value run will start with", err)
	}
}

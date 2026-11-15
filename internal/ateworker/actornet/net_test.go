// Copyright 2026 The Agent Substrate Authors
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

package actornet

import "testing"

func TestIPFamiliesForPodIPs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		podIPs  []string
		want    IPFamilies
		wantErr bool
	}{
		{"IPv4", []string{"10.0.0.2"}, IPv4, false},
		{"IPv6", []string{"fd00::2"}, IPv6, false},
		{"dual stack", []string{"10.0.0.2", "fd00::2"}, IPv4 | IPv6, false},
		{"dual stack reversed", []string{"fd00::2", "10.0.0.2"}, IPv4 | IPv6, false},
		{"missing", nil, 0, true},
		{"invalid", []string{"not-an-ip"}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := IPFamiliesForPodIPs(tc.podIPs)
			if (err != nil) != tc.wantErr {
				t.Fatalf("IPFamiliesForPodIPs(%v) error = %v, wantErr %v", tc.podIPs, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("IPFamiliesForPodIPs(%v) = %v, want %v", tc.podIPs, got, tc.want)
			}
		})
	}
}

func TestIPFamiliesActorEndpoints(t *testing.T) {
	for _, tc := range []struct {
		families IPFamilies
		actor    string
		gateway  string
		upstream string
	}{
		{0, ActorVethIPv4, ActorVethGateway, "http://169.254.17.2:80"},
		{IPv4, ActorVethIPv4, ActorVethGateway, "http://169.254.17.2:80"},
		{IPv6, ActorVethIPv6, ActorVethIPv6Gateway, "http://[fd00:169:254::2]:80"},
		{IPv4 | IPv6, ActorVethIPv4, ActorVethGateway, "http://169.254.17.2:80"},
	} {
		if got := tc.families.ActorVethIP(); got != tc.actor {
			t.Errorf("%v ActorIP() = %q, want %q", tc.families, got, tc.actor)
		}
		if got := tc.families.GatewayIP(); got != tc.gateway {
			t.Errorf("%v GatewayIP() = %q, want %q", tc.families, got, tc.gateway)
		}
		if got := tc.families.ActorHTTPUpstream(); got != tc.upstream {
			t.Errorf("%v ActorHTTPUpstream() = %q, want %q", tc.families, got, tc.upstream)
		}
	}
}

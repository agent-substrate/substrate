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
		name       string
		podIPs     []string
		enableIPv6 bool
		want       IPFamilies
		wantErr    bool
	}{
		{name: "IPv4 gate off", podIPs: []string{"10.0.0.2"}, want: IPv4},
		{name: "IPv4 gate on", podIPs: []string{"10.0.0.2"}, enableIPv6: true, want: IPv4},
		{name: "IPv6 gate off", podIPs: []string{"fd00::2"}, want: IPv4},
		{name: "IPv6 gate on", podIPs: []string{"fd00::2"}, enableIPv6: true, want: IPv4 | IPv6},
		{name: "dual stack gate off", podIPs: []string{"10.0.0.2", "fd00::2"}, want: IPv4},
		{name: "dual stack gate on", podIPs: []string{"10.0.0.2", "fd00::2"}, enableIPv6: true, want: IPv4 | IPv6},
		{name: "dual stack reversed gate on", podIPs: []string{"fd00::2", "10.0.0.2"}, enableIPv6: true, want: IPv4 | IPv6},
		{name: "missing", wantErr: true},
		{name: "invalid", podIPs: []string{"not-an-ip"}, enableIPv6: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := IPFamiliesForPodIPs(tc.podIPs, tc.enableIPv6)
			if (err != nil) != tc.wantErr {
				t.Fatalf("IPFamiliesForPodIPs(%v) error = %v, wantErr %v", tc.podIPs, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("IPFamiliesForPodIPs(%v) = %v, want %v", tc.podIPs, got, tc.want)
			}
		})
	}
}

func TestIPFamiliesString(t *testing.T) {
	for _, tc := range []struct {
		families IPFamilies
		value    string
	}{
		{IPv4, "ipv4"},
		{IPv4 | IPv6, "ipv4,ipv6"},
	} {
		if got := tc.families.String(); got != tc.value {
			t.Errorf("%v String() = %q, want %q", tc.families, got, tc.value)
		}
	}
}

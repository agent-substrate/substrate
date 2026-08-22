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

package ipfamily

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestClusterIPsByFamily(t *testing.T) {
	tests := []struct {
		name       string
		spec       corev1.ServiceSpec
		wantV4     string
		wantV6     string
		wantReason string
	}{
		{
			name:       "single stack IPv4",
			spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10", ClusterIPs: []string{"10.96.0.10"}},
			wantV4:     "10.96.0.10",
			wantReason: "the default policy on an IPv4 cluster",
		},
		{
			name:       "single stack IPv6",
			spec:       corev1.ServiceSpec{ClusterIP: "fd00:10:96::8857", ClusterIPs: []string{"fd00:10:96::8857"}},
			wantV6:     "fd00:10:96::8857",
			wantReason: "an IPv6-only cluster allocates a v6 ClusterIP with no ipFamilyPolicy set",
		},
		{
			name:       "dual stack IPv4 primary",
			spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10", ClusterIPs: []string{"10.96.0.10", "fd00:10:96::8857"}},
			wantV4:     "10.96.0.10",
			wantV6:     "fd00:10:96::8857",
			wantReason: "both families are usable regardless of which one is primary",
		},
		{
			name:       "dual stack IPv6 primary",
			spec:       corev1.ServiceSpec{ClusterIP: "fd00:10:96::8857", ClusterIPs: []string{"fd00:10:96::8857", "10.96.0.10"}},
			wantV4:     "10.96.0.10",
			wantV6:     "fd00:10:96::8857",
			wantReason: "the order of ClusterIPs is the family preference, not a family label",
		},
		{
			name:       "scalar only",
			spec:       corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
			wantV4:     "10.96.0.10",
			wantReason: "a hand-built Service may set only the singular field",
		},
		{
			name:       "headless",
			spec:       corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, ClusterIPs: []string{corev1.ClusterIPNone}},
			wantReason: "None is a sentinel, not an address",
		},
		{
			name:       "not yet allocated",
			spec:       corev1.ServiceSpec{},
			wantReason: "a Service observed before the allocator has run has neither",
		},
		{
			name:       "v4-mapped v6 belongs to neither family",
			spec:       corev1.ServiceSpec{ClusterIPs: []string{"::ffff:10.96.0.10"}},
			wantReason: "net.IP.To4 would misfile this as IPv4; kube never allocates one, so dropping it beats guessing",
		},
		{
			name:       "unparseable entries are skipped",
			spec:       corev1.ServiceSpec{ClusterIPs: []string{"not-an-ip", "10.96.0.10"}},
			wantV4:     "10.96.0.10",
			wantReason: "a junk entry must not shadow a usable one",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &corev1.Service{Spec: tc.spec}
			v4, v6 := ClusterIPsByFamily(svc)
			if v4 != tc.wantV4 || v6 != tc.wantV6 {
				t.Errorf("ClusterIPsByFamily(%+v) = (%q, %q), want (%q, %q): %s", tc.spec, v4, v6, tc.wantV4, tc.wantV6, tc.wantReason)
			}
		})
	}
}

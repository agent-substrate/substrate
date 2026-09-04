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

package netutil

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestClusterIPsByFamily(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spec  corev1.ServiceSpec
		want4 string
		want6 string
	}{
		{
			name:  "single stack IPv4",
			spec:  corev1.ServiceSpec{ClusterIP: "10.96.0.10", ClusterIPs: []string{"10.96.0.10"}},
			want4: "10.96.0.10",
		},
		{
			name:  "single stack IPv6",
			spec:  corev1.ServiceSpec{ClusterIP: "fd00:10:96::a", ClusterIPs: []string{"fd00:10:96::a"}},
			want6: "fd00:10:96::a",
		},
		{
			name:  "dual stack IPv4 primary",
			spec:  corev1.ServiceSpec{ClusterIP: "10.96.0.10", ClusterIPs: []string{"10.96.0.10", "fd00:10:96::a"}},
			want4: "10.96.0.10",
			want6: "fd00:10:96::a",
		},
		{
			name:  "dual stack IPv6 primary",
			spec:  corev1.ServiceSpec{ClusterIP: "fd00:10:96::a", ClusterIPs: []string{"fd00:10:96::a", "10.96.0.10"}},
			want4: "10.96.0.10",
			want6: "fd00:10:96::a",
		},
		{
			// A Service built by hand or by a fake client may only set the scalar.
			name:  "scalar ClusterIP only",
			spec:  corev1.ServiceSpec{ClusterIP: "10.96.0.10"},
			want4: "10.96.0.10",
		},
		{
			name: "headless",
			spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, ClusterIPs: []string{corev1.ClusterIPNone}},
		},
		{
			name: "no cluster IP",
			spec: corev1.ServiceSpec{},
		},
		{
			name:  "unparseable entry is skipped",
			spec:  corev1.ServiceSpec{ClusterIPs: []string{"not-an-ip", "", "10.96.0.10"}},
			want4: "10.96.0.10",
		},
		{
			// A v4-mapped v6 address counts as neither family: net.IP.To4 would
			// file it as IPv4, and calling it IPv6 would claim a dual-stack
			// Service the cluster does not have.
			name: "v4-mapped v6 counts as neither",
			spec: corev1.ServiceSpec{ClusterIPs: []string{"::ffff:10.96.0.10"}},
		},
		{
			name:  "first entry per family wins",
			spec:  corev1.ServiceSpec{ClusterIPs: []string{"10.96.0.10", "10.96.0.11", "fd00:10:96::a", "fd00:10:96::b"}},
			want4: "10.96.0.10",
			want6: "fd00:10:96::a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ClusterIPsByFamily(&corev1.Service{Spec: tc.spec})
			if got.V4 != tc.want4 || got.V6 != tc.want6 {
				t.Errorf("ClusterIPsByFamily() = (%q, %q), want (%q, %q)", got.V4, got.V6, tc.want4, tc.want6)
			}
		})
	}
}

func TestClusterIPsByFamilyNilService(t *testing.T) {
	if got := ClusterIPsByFamily(nil); got != (ServiceClusterIPs{}) {
		t.Errorf("ClusterIPsByFamily(nil) = %+v, want zero value", got)
	}
}

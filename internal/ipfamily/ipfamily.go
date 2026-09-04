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

// Package ipfamily sorts Kubernetes addresses into their IP families.
package ipfamily

import (
	"net/netip"

	corev1 "k8s.io/api/core/v1"
)

// ClusterIPsByFamily splits a Service's cluster IPs into its IPv4 and IPv6
// entries, returning "" for a family the Service does not have.
//
// A Service with no ipFamilyPolicy is SingleStack, so even on a dual-stack
// cluster it has exactly one ClusterIP and one of the two return values is
// empty. Callers must handle that: it is the steady state everywhere
// ipFamilyPolicy has not been set, and it is what distinguishes "this cluster
// has no address to offer in that family" from a misconfiguration.
//
// Spec.ClusterIPs is preferred over the singular Spec.ClusterIP, with a
// fallback to the latter because a Service built by hand (or by a fake client)
// may only set the scalar. Headless Services, and any entry that is not a
// parseable address, are skipped rather than returned.
func ClusterIPsByFamily(svc *corev1.Service) (v4, v6 string) {
	ips := svc.Spec.ClusterIPs
	if len(ips) == 0 && svc.Spec.ClusterIP != "" {
		ips = []string{svc.Spec.ClusterIP}
	}
	for _, ip := range ips {
		if ip == "" || ip == corev1.ClusterIPNone {
			continue
		}
		// netip rather than net.IP: net.IP.To4 returns non-nil for a v4-mapped
		// v6 address and would misfile it as IPv4.
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}
		switch {
		case addr.Is4() && v4 == "":
			v4 = ip
		case addr.Is6() && !addr.Is4In6() && v6 == "":
			v6 = ip
		}
	}
	return v4, v6
}

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

// Package netutil holds DNS and address-family helpers for the e2e suites.
package netutil

import (
	"net/netip"

	corev1 "k8s.io/api/core/v1"
)

// ServiceClusterIPs holds a Service's cluster IPs split by family; a family
// the Service does not have is the empty string.
type ServiceClusterIPs struct {
	V4 string
	V6 string
}

// ClusterIPsByFamily gets the Service's cluster IPs split by IPv4 and IPv6.
func ClusterIPsByFamily(svc *corev1.Service) ServiceClusterIPs {
	var ips ServiceClusterIPs
	if svc == nil {
		return ips
	}
	addrs := svc.Spec.ClusterIPs
	if len(addrs) == 0 && svc.Spec.ClusterIP != "" {
		addrs = []string{svc.Spec.ClusterIP}
	}
	for _, ip := range addrs {
		if ip == "" || ip == corev1.ClusterIPNone {
			continue
		}
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			continue
		}
		switch {
		case addr.Is4() && ips.V4 == "":
			ips.V4 = ip
		// A v4-mapped v6 address counts as neither family.
		case addr.Is6() && !addr.Is4In6() && ips.V6 == "":
			ips.V6 = ip
		}
	}
	return ips
}

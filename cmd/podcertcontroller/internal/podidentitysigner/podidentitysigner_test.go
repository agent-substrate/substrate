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

package podidentitysigner

import (
	"crypto/x509"
	"slices"
	"testing"
)

func TestExtKeyUsages(t *testing.T) {
	testCases := []struct {
		name           string
		namespace      string
		serviceAccount string
		want           []x509.ExtKeyUsage
	}{
		{
			name:           "atelet gets clientAuth and serverAuth",
			namespace:      "ate-system",
			serviceAccount: "atelet",
			want:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		},
		{
			name:           "other SA in ate-system is client-only",
			namespace:      "ate-system",
			serviceAccount: "ate-controller",
			want:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		{
			name:           "atelet SA name in another namespace is client-only",
			namespace:      "default",
			serviceAccount: "atelet",
			want:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
		{
			name:           "ordinary workload is client-only",
			namespace:      "default",
			serviceAccount: "default",
			want:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := extKeyUsages(tc.namespace, tc.serviceAccount)
			if !slices.Equal(got, tc.want) {
				t.Errorf("extKeyUsages(%q, %q) = %v, want %v", tc.namespace, tc.serviceAccount, got, tc.want)
			}
		})
	}
}

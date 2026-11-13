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

package podcertificate

import (
	"testing"
	"time"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	"k8s.io/utils/ptr"
)

func TestLifetime(t *testing.T) {
	tests := []struct {
		name    string
		max     *int32
		want    time.Duration
		wantErr bool
	}{
		{name: "unset", want: 24 * time.Hour},
		{name: "capped", max: ptr.To(int32(7 * 86400)), want: 24 * time.Hour},
		{name: "shorter", max: ptr.To(int32(7200)), want: 2 * time.Hour},
		{name: "minimum", max: ptr.To(int32(3600)), want: time.Hour},
		{name: "under minimum", max: ptr.To(int32(3599)), wantErr: true},
		{name: "zero", max: ptr.To(int32(0)), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pcr := &certsv1beta1.PodCertificateRequest{Spec: certsv1beta1.PodCertificateRequestSpec{MaxExpirationSeconds: tc.max}}
			got, err := Lifetime(pcr)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Lifetime() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Lifetime() = %v, want %v", got, tc.want)
			}
		})
	}
}

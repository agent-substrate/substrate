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

package k8sjwt

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestKubernetesClaimsLogValue(t *testing.T) {
	c := KubernetesClaims{
		Issuer:             "iss-1",
		Subject:            "sub-1",
		Audiences:          []string{"aud-1"},
		Expiration:         time.Unix(1000, 0),
		JTI:                "jti-1",
		Namespace:          "ns-1",
		ServiceAccountName: "sa-1",
		ServiceAccountUID:  "sauid-1",
		PodName:            "pod-1",
		PodUID:             "poduid-1",
		NodeName:           "node-1",
		NodeUID:            "nodeuid-1",
		SecretName:         "deny-secret-name",
		SecretUID:          "deny-secret-uid",
		NotBefore:          time.Unix(900, 0),
		IssuedAt:           time.Unix(800, 0),
	}

	for _, tc := range []struct {
		name string
		arg  any
	}{
		{"value", c},
		{"pointer", &c},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			slog.New(slog.NewJSONHandler(&buf, nil)).Info("verified", slog.Any("claims", tc.arg))
			out := buf.String()

			for _, want := range []string{"iss-1", "sub-1", "aud-1", "jti-1", "ns-1", "sa-1", "pod-1", "node-1"} {
				if !strings.Contains(out, want) {
					t.Errorf("claims log missing expected value %q: %s", want, out)
				}
			}
			for _, deny := range []string{"deny-secret-name", "deny-secret-uid", "sauid-1", "poduid-1", "nodeuid-1"} {
				if strings.Contains(out, deny) {
					t.Errorf("claims log leaked excluded value %q: %s", deny, out)
				}
			}
		})
	}
}

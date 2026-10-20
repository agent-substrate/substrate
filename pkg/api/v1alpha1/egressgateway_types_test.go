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

package v1alpha1

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEgressGatewayValidation(t *testing.T) {
	for _, tt := range []struct {
		name, address, identity string
		wantErr                 bool
	}{
		{"valid", "gateway.example:443", "spiffe://cluster.local/ns/egress/sa/gateway", false},
		{"ipv6", "[::1]:443", "spiffe://example/gateway", false},
		{"missing-address", "", "spiffe://example/gateway", true},
		{"missing-port", "gateway.example", "spiffe://example/gateway", true},
		{"missing-identity", "gateway:443", "", true},
		{"invalid-identity", "gateway:443", "https://example/gateway", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gateway := &EgressGateway{ObjectMeta: metav1.ObjectMeta{Name: "test-" + tt.name}, Spec: EgressGatewaySpec{Address: tt.address, Identity: tt.identity}}
			err := k8sClient.Create(context.Background(), gateway)
			if err == nil {
				t.Cleanup(func() {
					if err := k8sClient.Delete(context.Background(), gateway); err != nil {
						t.Error(err)
					}
				})
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("Create() error = %v, want error %v", err, tt.wantErr)
			}
		})
	}
}

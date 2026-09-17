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

package main

import (
	"context"
	"strings"
	"testing"
)

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
	})
	*postgresConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestValidateEgressGatewayAddress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		address string
		wantErr bool
	}{
		{name: "empty", address: "", wantErr: true},
		{name: "set", address: "atenet-egress.ate-system.svc:443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldAddress := *egressGatewayAddress
			t.Cleanup(func() {
				*egressGatewayAddress = oldAddress
			})
			*egressGatewayAddress = tc.address

			err := validateEgressGatewayAddress()
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("validateEgressGatewayAddress() error = %v, wantErr %t", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "--egress-gateway-address is required") {
				t.Errorf("validateEgressGatewayAddress() error = %v, want missing-address error", err)
			}
		})
	}
}

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

package controlapi

import (
	"context"
	"errors"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/egressgateway"
	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
)

func TestEgressGateway(t *testing.T) {
	unavailable := errors.New("gateway unavailable")
	for _, tt := range []struct {
		name        string
		resolve     egressgateway.Resolve
		wantAddress string
		wantErr     bool
	}{
		{name: "missing resolver", wantErr: true},
		{name: "missing gateway", resolve: func(context.Context, string) (*atev1.EgressGatewaySpec, error) { return nil, unavailable }, wantErr: true},
		{name: "empty result", resolve: func(context.Context, string) (*atev1.EgressGatewaySpec, error) { return nil, nil }, wantErr: true},
		{name: "assigned gateway", resolve: func(_ context.Context, atespace string) (*atev1.EgressGatewaySpec, error) {
			return &atev1.EgressGatewaySpec{Address: atespace + ".gateway:443"}, nil
		}, wantAddress: "team-a.gateway:443"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := &ActorWorkflow{resolveEgressGateway: tt.resolve}
			got, err := w.egressGateway(context.Background(), "team-a")
			if (err != nil) != tt.wantErr {
				t.Fatalf("gateway = %v, %v", got, err)
			}
			if err == nil && got.GetAddress() != tt.wantAddress {
				t.Fatalf("gateway = %q, want %q", got.GetAddress(), tt.wantAddress)
			}
		})
	}
}

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

// Package egressgateway resolves administrator-owned Atespace gateway bindings.
package egressgateway

import (
	"context"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.ate.dev/substrate/internal/resources"
	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Resolve returns the gateway assigned to an Atespace. Errors must fail closed.
type Resolve func(context.Context, string) (*atev1.EgressGatewaySpec, error)

type atespaceReader interface {
	GetAtespace(context.Context, string) (*ateapipb.Atespace, error)
}
type gatewayReader interface {
	Get(string) (*atev1.EgressGateway, error)
}

// New uses the persisted Atespace binding and the Kubernetes gateway cache.
// An omitted binding selects the resource named default, never a direct address.
func New(atespaces atespaceReader, gateways gatewayReader) Resolve {
	return func(ctx context.Context, name string) (*atev1.EgressGatewaySpec, error) {
		atespace, err := atespaces.GetAtespace(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("reading gateway binding for Atespace %q: %w", name, err)
		}
		gatewayName := "default"
		if ref := atespace.GetEgressGateway(); ref != nil {
			if ref.GetAtespace() != "" || ref.GetName() == "" {
				return nil, fmt.Errorf("invalid gateway binding for Atespace %q", name)
			}
			gatewayName = ref.GetName()
		}
		gateway, err := gateways.Get(gatewayName)
		if err != nil {
			return nil, fmt.Errorf("reading EgressGateway %q: %w", gatewayName, err)
		}
		if gateway.DeletionTimestamp != nil {
			return nil, fmt.Errorf("EgressGateway %q is being deleted", gatewayName)
		}
		spec := gateway.Spec
		if spec.Address == "" {
			return nil, fmt.Errorf("EgressGateway %q has no address", gatewayName)
		}
		if errs := resources.ValidateHostPort(field.NewPath("spec", "address"), spec.Address); len(errs) > 0 {
			return nil, fmt.Errorf("invalid EgressGateway %q: %w", gatewayName, errs.ToAggregate())
		}
		if _, err := spiffeid.FromString(spec.Identity); err != nil {
			return nil, fmt.Errorf("invalid EgressGateway %q identity: %w", gatewayName, err)
		}
		return &spec, nil
	}
}

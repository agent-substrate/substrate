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
	"errors"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/cache"
)

// Resolve returns the gateway assigned to an Atespace. Errors must fail closed.
type Resolve func(context.Context, string) (*atev1.EgressGatewaySpec, error)

type atespaceReader interface {
	GetAtespace(context.Context, string) (*ateapipb.Atespace, error)
}
type gatewayReader interface {
	Get(string) (*atev1.EgressGateway, error)
}

// Resolver reads Atespace bindings and administrator-owned gateways.
type Resolver struct {
	atespaces atespaceReader
	gateways  gatewayReader
	indexer   cache.Indexer
}

const identityIndex = "egressGatewayIdentity"

// IndexIdentities must run before the gateway informer starts.
func IndexIdentities(informer cache.SharedIndexInformer) error {
	return informer.AddIndexers(cache.Indexers{identityIndex: func(obj any) ([]string, error) {
		gateway := obj.(*atev1.EgressGateway)
		return []string{gateway.Spec.Identity}, nil
	}})
}

// New uses the persisted Atespace binding and the Kubernetes gateway cache.
func New(atespaces atespaceReader, gateways gatewayReader, indexer cache.Indexer) *Resolver {
	return &Resolver{atespaces: atespaces, gateways: gateways, indexer: indexer}
}

// IsGateway checks the informer index without reading the Atespace store.
func (r *Resolver) IsGateway(identity string) (bool, error) {
	objects, err := r.indexer.ByIndex(identityIndex, identity)
	if err != nil {
		return false, apierror.Internal("looking up egress gateway identity: %v", err)
	}
	return len(objects) > 0, nil
}

// Resolve returns the current gateway for a persisted Atespace.
func (r *Resolver) Resolve(ctx context.Context, name string) (*atev1.EgressGatewaySpec, error) {
	atespace, err := r.atespaces.GetAtespace(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, apierror.FailedPrecondition("Atespace %q does not exist", name)
	}
	if err != nil {
		return nil, apierror.Unavailable("reading gateway binding for Atespace %q: %v", name, err)
	}
	return r.ForAtespace(atespace)
}

// ForAtespace validates a binding before creation or activation. An omitted
// binding selects default. Activation revalidates because gateways can change.
func (r *Resolver) ForAtespace(atespace *ateapipb.Atespace) (*atev1.EgressGatewaySpec, error) {
	gatewayName := "default"
	if ref := atespace.GetEgressGateway(); ref != nil {
		if ref.GetAtespace() != "" || ref.GetName() == "" {
			return nil, apierror.InvalidArgument("invalid egress gateway reference")
		}
		gatewayName = ref.GetName()
	}
	gateway, err := r.gateways.Get(gatewayName)
	if kerrors.IsNotFound(err) {
		return nil, apierror.FailedPrecondition("EgressGateway %q does not exist", gatewayName)
	}
	if err != nil {
		return nil, apierror.Unavailable("reading EgressGateway %q: %v", gatewayName, err)
	}
	if gateway.DeletionTimestamp != nil {
		return nil, apierror.FailedPrecondition("EgressGateway %q is being deleted", gatewayName)
	}
	spec := gateway.Spec
	if spec.Address == "" {
		return nil, apierror.FailedPrecondition("EgressGateway %q has no address", gatewayName)
	}
	if errs := resources.ValidateHostPort(field.NewPath("spec", "address"), spec.Address); len(errs) > 0 {
		return nil, apierror.FailedPrecondition("invalid EgressGateway %q: %v", gatewayName, errs.ToAggregate())
	}
	if _, err := spiffeid.FromString(spec.Identity); err != nil {
		return nil, apierror.FailedPrecondition("invalid EgressGateway %q identity: %v", gatewayName, err)
	}
	return &spec, nil
}

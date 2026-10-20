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

// Package egressgatewaytest supplies an informer-backed default gateway.
package egressgatewaytest

import (
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/egressgateway"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	listers "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

// New creates a resolver with default and any additional named gateways in its cache.
func New(t testing.TB, persistence store.Interface, names ...string) *egressgateway.Resolver {
	t.Helper()
	informer := cache.NewSharedIndexInformer(nil, &atev1.EgressGateway{}, 0, cache.Indexers{})
	if err := egressgateway.IndexIdentities(informer); err != nil {
		t.Fatal(err)
	}
	indexer := informer.GetIndexer()
	for _, name := range append([]string{"default"}, names...) {
		if err := indexer.Add(&atev1.EgressGateway{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: atev1.EgressGatewaySpec{Address: "gateway:443", Identity: "spiffe://example/gateway"}}); err != nil {
			t.Fatal(err)
		}
	}
	return egressgateway.New(persistence, listers.NewEgressGatewayLister(indexer), indexer)
}

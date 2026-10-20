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

package egressgateway

import (
	"context"
	"errors"
	"testing"

	atev1 "go.ate.dev/substrate/pkg/api/v1alpha1"
	listers "go.ate.dev/substrate/pkg/client/listers/api/v1alpha1"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

type atespaceStub struct {
	value *ateapipb.Atespace
	err   error
}

func (s atespaceStub) GetAtespace(context.Context, string) (*ateapipb.Atespace, error) {
	return s.value, s.err
}

func TestResolve(t *testing.T) {
	for _, tt := range []struct {
		name, ref, address, identity                 string
		missing, terminating, storeError, namespaced bool
		wantErr                                      bool
	}{
		{name: "default", address: "default.example:443", identity: "spiffe://example/default"},
		{name: "assigned", ref: "tenant-a", address: "tenant.example:8443", identity: "spiffe://example/tenant-a"},
		{name: "IPv6", ref: "tenant-a", address: "[::1]:443", identity: "spiffe://example/tenant-a"},
		{name: "missing gateway", missing: true, wantErr: true},
		{name: "missing atespace", storeError: true, wantErr: true},
		{name: "terminating", terminating: true, wantErr: true},
		{name: "empty address", identity: "spiffe://example/a", wantErr: true},
		{name: "invalid address", address: "host:99999", identity: "spiffe://example/a", wantErr: true},
		{name: "invalid identity", address: "host:443", identity: "https://example/a", wantErr: true},
		{name: "namespaced reference", ref: "tenant-a", namespaced: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := &ateapipb.Atespace{}
			name := "default"
			if tt.ref != "" {
				name = tt.ref
				a.EgressGateway = &ateapipb.ObjectRef{Name: name}
			}
			if tt.namespaced {
				a.EgressGateway.Atespace = "other"
			}
			st := atespaceStub{value: a}
			if tt.storeError {
				st.err = errors.New("unavailable")
			}
			indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			gateway := &atev1.EgressGateway{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: atev1.EgressGatewaySpec{Address: tt.address, Identity: tt.identity}}
			if tt.terminating {
				now := metav1.Now()
				gateway.DeletionTimestamp = &now
			}
			if !tt.missing {
				if err := indexer.Add(runtime.Object(gateway)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := New(st, listers.NewEgressGatewayLister(indexer))(context.Background(), "team-a")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() = %v, %v", got, err)
			}
			if err == nil && (got.Address != tt.address || got.Identity != tt.identity) {
				t.Fatalf("wrong gateway: %+v", got)
			}
		})
	}
}

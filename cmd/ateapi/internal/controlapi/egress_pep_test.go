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
	"testing"

	"github.com/agent-substrate/substrate/internal/egress"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

func TestResolveEgressPEPAddressEmpty(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if got != "" {
		t.Fatalf("resolveEgressPEPAddress() = %q, want empty", got)
	}
}

func TestResolveEgressPEPAddressSelectsBestGateway(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, gw := range []*unstructured.Unstructured{
		gatewayPEP("global", "ate-system", map[string]string{
			egress.LabelPEP: "true",
		}, "HTTP", int64(15080)),
		gatewayPEP("space", "ate-system", map[string]string{
			egress.LabelPEP:      "true",
			egress.LabelAtespace: "space-a",
		}, "HTTP", int64(15081)),
		gatewayPEP("actor", "ate-system", map[string]string{
			egress.LabelPEP:      "true",
			egress.LabelAtespace: "space-a",
			egress.LabelActor:    "actor-a",
		}, "HTTP", int64(15082)),
		gatewayPEP("other-actor", "ate-system", map[string]string{
			egress.LabelPEP:      "true",
			egress.LabelAtespace: "space-a",
			egress.LabelActor:    "actor-b",
		}, "HTTP", int64(15083)),
		gatewayPEP("other-space", "ate-system", map[string]string{
			egress.LabelPEP:      "true",
			egress.LabelAtespace: "space-b",
		}, "HTTP", int64(15084)),
		gatewayPEP("unlabeled", "ate-system", nil, "HTTP", int64(15085)),
	} {
		if err := indexer.Add(gw); err != nil {
			t.Fatalf("indexer.Add() error = %v", err)
		}
	}

	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	want := "actor.ate-system.svc.cluster.local:15082"
	if got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}
}

func TestResolveEgressPEPAddressFallsBackToAtespaceThenGlobal(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, gw := range []*unstructured.Unstructured{
		gatewayPEP("global", "ate-system", map[string]string{
			egress.LabelPEP: "true",
		}, "HTTP", int64(15080)),
		gatewayPEP("space", "ate-system", map[string]string{
			egress.LabelPEP:      "true",
			egress.LabelAtespace: "space-a",
		}, "HTTP", int64(15081)),
	} {
		if err := indexer.Add(gw); err != nil {
			t.Fatalf("indexer.Add() error = %v", err)
		}
	}

	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-without-specific-pep")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if want := "space.ate-system.svc.cluster.local:15081"; got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}

	got, err = resolveEgressPEPAddress(t.Context(), indexer, "space-without-specific-pep", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if want := "global.ate-system.svc.cluster.local:15080"; got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}
}

func TestResolveEgressPEPAddressRequiresHTTPListener(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := indexer.Add(gatewayPEP("tcp-only", "ate-system", map[string]string{
		egress.LabelPEP: "true",
	}, "TCP", int64(15080))); err != nil {
		t.Fatalf("indexer.Add() error = %v", err)
	}

	if _, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a"); err == nil {
		t.Fatal("resolveEgressPEPAddress() error = nil, want error")
	}
}

func gatewayPEP(name, namespace string, labels map[string]string, protocol string, port int64) *unstructured.Unstructured {
	u := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "gateway.networking.k8s.io/v1",
			"kind":       "Gateway",
			"spec": map[string]any{
				"listeners": []any{
					map[string]any{
						"name":     "http",
						"protocol": protocol,
						"port":     port,
					},
				},
			},
			"status": map[string]any{
				"conditions": []any{
					map[string]any{
						"type":   "Programmed",
						"status": "True",
					},
				},
			},
		},
	}
	u.SetName(name)
	u.SetNamespace(namespace)
	u.SetLabels(labels)
	return u
}

func TestResolveEgressPEPAddressSkipsUnprogrammedGateway(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	unprogrammed := gatewayPEP("actor", "ate-system", map[string]string{
		egress.LabelPEP:      "true",
		egress.LabelAtespace: "space-a",
		egress.LabelActor:    "actor-a",
	}, "HTTP", int64(15082))
	if err := unstructured.SetNestedSlice(unprogrammed.Object, []any{
		map[string]any{"type": "Programmed", "status": "False"},
	}, "status", "conditions"); err != nil {
		t.Fatalf("SetNestedSlice() error = %v", err)
	}
	for _, gw := range []*unstructured.Unstructured{
		unprogrammed,
		gatewayPEP("global", "ate-system", map[string]string{
			egress.LabelPEP: "true",
		}, "HTTP", int64(15080)),
	} {
		if err := indexer.Add(gw); err != nil {
			t.Fatalf("indexer.Add() error = %v", err)
		}
	}

	// The unprogrammed actor-scoped PEP is skipped; resolution falls back to
	// the programmed global PEP.
	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if want := "global.ate-system.svc.cluster.local:15080"; got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}
}

func TestResolveEgressPEPAddressActorLabelWithoutAtespaceMatchesNothing(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, gw := range []*unstructured.Unstructured{
		// Misconfigured: actor label without an atespace label can never match.
		gatewayPEP("broken-actor", "ate-system", map[string]string{
			egress.LabelPEP:   "true",
			egress.LabelActor: "actor-a",
		}, "HTTP", int64(15082)),
		gatewayPEP("global", "ate-system", map[string]string{
			egress.LabelPEP: "true",
		}, "HTTP", int64(15080)),
	} {
		if err := indexer.Add(gw); err != nil {
			t.Fatalf("indexer.Add() error = %v", err)
		}
	}

	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if want := "global.ate-system.svc.cluster.local:15080"; got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}
}

func TestResolveEgressPEPAddressPrefersStatusAddress(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	gw := gatewayPEP("global", "ate-system", map[string]string{
		egress.LabelPEP: "true",
	}, "HTTP", int64(15080))
	if err := unstructured.SetNestedSlice(gw.Object, []any{
		map[string]any{"type": "Hostname", "value": "pep.example.internal"},
	}, "status", "addresses"); err != nil {
		t.Fatalf("SetNestedSlice() error = %v", err)
	}
	if err := indexer.Add(gw); err != nil {
		t.Fatalf("indexer.Add() error = %v", err)
	}

	got, err := resolveEgressPEPAddress(t.Context(), indexer, "space-a", "actor-a")
	if err != nil {
		t.Fatalf("resolveEgressPEPAddress() error = %v", err)
	}
	if want := "pep.example.internal:15080"; got != want {
		t.Fatalf("resolveEgressPEPAddress() = %q, want %q", got, want)
	}
}

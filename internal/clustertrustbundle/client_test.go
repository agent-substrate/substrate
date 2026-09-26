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

package clustertrustbundle

import (
	"context"
	"fmt"
	"testing"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name, selected string
		v1, beta       bool
		discoveryErr   error
	}{
		{name: "both", v1: true, beta: true, selected: "v1"},
		{name: "stable only", v1: true, selected: "v1"},
		{name: "beta only", beta: true, selected: "v1beta1"},
		{name: "neither"},
		{name: "missing v1 group", beta: true, selected: "v1beta1", discoveryErr: apierrors.NewNotFound(schema.GroupResource{}, "v1")},
		{name: "forbidden", beta: true, discoveryErr: apierrors.NewForbidden(schema.GroupResource{}, "", fmt.Errorf("denied"))},
		{name: "server failure", beta: true, discoveryErr: apierrors.NewInternalError(fmt.Errorf("unavailable"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			for _, version := range []string{"v1", "v1beta1"} {
				resources := &metav1.APIResourceList{GroupVersion: "certificates.k8s.io/" + version}
				if (version == "v1" && tc.v1) || (version == "v1beta1" && tc.beta) {
					resources.APIResources = []metav1.APIResource{{Name: "clustertrustbundles"}}
				}
				kc.Resources = append(kc.Resources, resources)
			}
			calls := 0
			kc.PrependReactor("get", "resource", func(ktesting.Action) (bool, runtime.Object, error) {
				calls++
				if calls == 1 && tc.discoveryErr != nil {
					return true, nil, tc.discoveryErr
				}
				return false, nil, nil
			})
			c, err := New(kc)
			if tc.selected == "" {
				if err == nil {
					t.Fatal("expected discovery failure")
				}
				if tc.discoveryErr != nil && calls != 1 {
					t.Fatal("fell back after a discovery error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.V1() != (tc.selected == "v1") {
				t.Fatalf("selected v1=%v, want %s", c.V1(), tc.selected)
			}
		})
	}
}

func TestOperationsUseSelectedVersion(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + version,
				APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
			c, err := New(kc)
			if err != nil {
				t.Fatal(err)
			}
			ctb := &certsv1beta1.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{Name: "example", Labels: map[string]string{"live": "true"}},
				Spec:       certsv1beta1.ClusterTrustBundleSpec{SignerName: "example.com/identity", TrustBundle: "first"},
			}
			if err := c.Create(context.Background(), ctb); err != nil {
				t.Fatal(err)
			}
			got, err := c.Get(context.Background(), ctb.Name)
			if err != nil {
				t.Fatal(err)
			}
			got.Spec.TrustBundle = "second"
			if err := c.Update(context.Background(), got); err != nil {
				t.Fatal(err)
			}
			items, err := c.List(context.Background(), metav1.ListOptions{LabelSelector: "live=true"})
			if err != nil || len(items) != 1 || items[0].Spec.TrustBundle != "second" {
				t.Fatalf("List = %v, %v", items, err)
			}
			for _, action := range kc.Actions() {
				if action.GetResource().Resource == "clustertrustbundles" && action.GetResource().Version != version {
					t.Errorf("%s used %s, want %s", action.GetVerb(), action.GetResource().Version, version)
				}
			}
			informer := c.Informer("example")
			var obj any = got
			if c.V1() {
				obj = &certsv1.ClusterTrustBundle{ObjectMeta: ctb.ObjectMeta, Spec: certsv1.ClusterTrustBundleSpec{TrustBundle: "second"}}
			}
			if err := informer.GetIndexer().Add(obj); err != nil {
				t.Fatal(err)
			}
			cached, err := CachedBundle(informer, c.V1(), "example")
			if err != nil || cached.Spec.TrustBundle != "second" {
				t.Fatalf("CachedBundle = %v, %v", cached, err)
			}
			if _, err := CachedBundle(informer, c.V1(), "missing"); !apierrors.IsNotFound(err) {
				t.Fatalf("missing bundle: %v", err)
			}
		})
	}
}

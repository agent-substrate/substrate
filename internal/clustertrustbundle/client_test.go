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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		stable, beta, missingStable bool
		err                         error
		want                        string
	}{
		{name: "both", stable: true, beta: true, want: "v1"},
		{name: "stable only", stable: true, want: "v1"},
		{name: "beta only", beta: true, want: "v1beta1"},
		{name: "missing stable group", beta: true, missingStable: true, want: "v1beta1"},
		{name: "neither"},
		{name: "forbidden", beta: true, err: apierrors.NewForbidden(schema.GroupResource{}, "", fmt.Errorf("denied"))},
		{name: "unavailable", beta: true, err: apierrors.NewServiceUnavailable("unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			for _, version := range []string{"v1", "v1beta1"} {
				if version == "v1" && tc.missingStable {
					continue
				}
				resources := &metav1.APIResourceList{GroupVersion: "certificates.k8s.io/" + version, APIResources: []metav1.APIResource{{Name: "certificatesigningrequests"}}}
				if (version == "v1" && tc.stable) || (version == "v1beta1" && tc.beta) {
					resources.APIResources = append(resources.APIResources, metav1.APIResource{Name: "clustertrustbundles"})
				}
				kc.Resources = append(kc.Resources, resources)
			}
			calls := 0
			kc.PrependReactor("get", "resource", func(ktesting.Action) (bool, runtime.Object, error) { calls++; return tc.err != nil, nil, tc.err })
			gv, err := Discover(kc.Discovery())
			if tc.want == "" {
				if err == nil {
					t.Fatal("expected discovery error")
				}
				if tc.err != nil && calls != 1 {
					t.Fatal("fell back after discovery error")
				}
				if notServed := errors.Is(err, ErrNotServed); notServed != (tc.err == nil) {
					t.Fatalf("errors.Is(%v, ErrNotServed) = %t", err, notServed)
				}
				return
			}
			if err != nil || gv.Version != tc.want {
				t.Fatalf("discovery = %v, %v", gv, err)
			}
		})
	}
}

func TestClient(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + version, APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
			c, err := NewClient(kc, func(opts *metav1.ListOptions) { opts.LabelSelector = "live=true" })
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			want := &certsv1.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{Name: "example.com:signer:bundle", UID: "uid", ResourceVersion: "42", Labels: map[string]string{"live": "true"}},
				Spec:       certsv1.ClusterTrustBundleSpec{SignerName: "example.com/signer", TrustBundle: "root-a"},
			}
			check := func(got *certsv1.ClusterTrustBundle, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got.ObjectMeta, want.ObjectMeta) || got.Spec != want.Spec {
					t.Fatalf("lost bundle fields: %#v", got)
				}
			}
			check(c.Create(ctx, want, metav1.CreateOptions{FieldManager: "test"}))
			check(c.Get(ctx, want.Name, metav1.GetOptions{}))
			list, err := c.List(ctx, metav1.ListOptions{LabelSelector: "live=true"})
			if err != nil || len(list.Items) != 1 {
				t.Fatalf("list = %v, %v", list, err)
			}
			check(&list.Items[0], nil)
			if c.informer != nil {
				t.Fatal("live operations allocated an informer")
			}
			if _, err := c.GetCached("missing"); !apierrors.IsNotFound(err) {
				t.Fatalf("missing cache entry: %v", err)
			}
			events := make(chan any, 8)
			_, err = c.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) { events <- obj }, UpdateFunc: func(_, obj any) { events <- obj }, DeleteFunc: func(obj any) { events <- obj },
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { defer close(done); c.Informer().Run(ctx.Done()) }()
			defer func() { cancel(); <-done }()
			receive := func() {
				t.Helper()
				select {
				case obj := <-events:
					if version == "v1" {
						if _, ok := obj.(*certsv1.ClusterTrustBundle); !ok {
							t.Fatalf("non-native object: %T", obj)
						}
					} else {
						if _, ok := obj.(*certsv1beta1.ClusterTrustBundle); !ok {
							t.Fatalf("non-native object: %T", obj)
						}
					}
				case <-ctx.Done():
					t.Fatal("informer event timeout")
				}
			}
			receive()
			check(c.GetCached(want.Name))
			want = want.DeepCopy()
			want.ResourceVersion = "43"
			want.Spec.TrustBundle = "root-b"
			check(c.Update(ctx, want, metav1.UpdateOptions{FieldManager: "test"}))
			receive()
			check(c.GetCached(want.Name))
			if err := c.Delete(ctx, want.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			receive()
			if _, err := c.GetCached(want.Name); !apierrors.IsNotFound(err) {
				t.Fatalf("deleted cache entry: %v", err)
			}
			for _, a := range kc.Actions() {
				if a.GetResource().Resource != "clustertrustbundles" {
					continue
				}
				if a.GetResource().Version != version {
					t.Fatalf("wrong endpoint: %v", a)
				}
				if watch, ok := a.(ktesting.WatchAction); ok && watch.GetWatchRestrictions().Labels.String() != "live=true" {
					t.Fatal("watch lost selector")
				}
			}
			kc.PrependReactor("get", "clustertrustbundles", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{}, "", fmt.Errorf("denied"))
			})
			if _, err := c.Get(ctx, want.Name, metav1.GetOptions{}); !apierrors.IsForbidden(err) {
				t.Fatalf("lost API error: %v", err)
			}
		})
	}
}

func TestConversionCopiesMetadata(t *testing.T) {
	original := &certsv1beta1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"key": "original"}}}
	stable := ToV1(original)
	stable.Labels["key"] = "stable"
	beta := ToBeta(stable)
	beta.Labels["key"] = "beta"
	if original.Labels["key"] != "original" || stable.Labels["key"] != "stable" {
		t.Fatal("conversion aliased metadata")
	}
}

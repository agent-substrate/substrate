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

package podcertificate

import (
	"context"
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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

func TestDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name         string
		v1, beta     bool
		discoveryErr error
		wantVersion  string
	}{
		{name: "prefer stable", v1: true, beta: true, wantVersion: "v1"},
		{name: "stable only", v1: true, wantVersion: "v1"},
		{name: "beta fallback", beta: true, wantVersion: "v1beta1"},
		{name: "neither served"},
		{name: "missing group version", beta: true, discoveryErr: apierrors.NewNotFound(schema.GroupResource{Group: "certificates.k8s.io", Resource: "v1"}, ""), wantVersion: "v1beta1"},
		{name: "forbidden", beta: true, discoveryErr: apierrors.NewForbidden(schema.GroupResource{}, "", fmt.Errorf("denied"))},
		{name: "server error", beta: true, discoveryErr: apierrors.NewInternalError(fmt.Errorf("unavailable"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			for _, version := range []string{"v1", "v1beta1"} {
				resources := &metav1.APIResourceList{GroupVersion: "certificates.k8s.io/" + version}
				// Stable discovery exists on older clusters for CSRs alone.
				resources.APIResources = []metav1.APIResource{{Name: "certificatesigningrequests"}}
				if (version == "v1" && tc.v1) || (version == "v1beta1" && tc.beta) {
					resources.APIResources = append(resources.APIResources, metav1.APIResource{Name: "podcertificaterequests"})
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
			client, err := NewClient(kc)
			if tc.wantVersion == "" {
				if err == nil {
					t.Fatal("expected discovery failure")
				}
				if tc.discoveryErr != nil && calls != 1 {
					t.Fatal("fell back after discovery failure")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if client.v1 != (tc.wantVersion == "v1") {
				t.Fatalf("wrong version: v1=%v", client.v1)
			}
			if client.v1 && calls != 1 {
				t.Fatal("unnecessary beta discovery")
			}
		})
	}
}

func TestInformerAndStatus(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + version, APIResources: []metav1.APIResource{{Name: "podcertificaterequests"}}}}
			client, err := NewClient(kc)
			if err != nil {
				t.Fatal(err)
			}
			pcr := &certsv1beta1.PodCertificateRequest{
				TypeMeta:   metav1.TypeMeta{APIVersion: "certificates.k8s.io/" + version, Kind: "PodCertificateRequest"},
				ObjectMeta: metav1.ObjectMeta{Name: "request", Namespace: "test", ResourceVersion: "123", UID: "request-uid"},
				Spec: certsv1beta1.PodCertificateRequestSpec{
					SignerName: "example.com/signer", PodName: "pod", PodUID: "pod-uid", ServiceAccountName: "sa", ServiceAccountUID: "sa-uid",
					NodeName: "node", NodeUID: "node-uid", MaxExpirationSeconds: ptr.To(int32(3600)), StubPKCS10Request: []byte("csr"),
					UnverifiedUserAnnotations: map[string]string{"example.com/key": "value"},
				},
			}
			if version == "v1beta1" {
				pcr.Spec.StubPKCS10Request = nil
				pcr.Spec.PKIXPublicKey = []byte("legacy-key") //nolint:staticcheck // Exercise legacy requests.
			}
			object := func(p *certsv1beta1.PodCertificateRequest) runtime.Object {
				if version == "v1beta1" {
					return p
				}
				return toV1(p)
			}
			kc.PrependReactor("list", "podcertificaterequests", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetResource().Version != version {
					return true, nil, fmt.Errorf("wrong list version")
				}
				if version == "v1" {
					return true, &certsv1.PodCertificateRequestList{Items: []certsv1.PodCertificateRequest{*object(pcr).(*certsv1.PodCertificateRequest)}}, nil
				}
				return true, &certsv1beta1.PodCertificateRequestList{Items: []certsv1beta1.PodCertificateRequest{*pcr}}, nil
			})
			events := watch.NewRaceFreeFake()
			watching := make(chan struct{}, 1)
			kc.PrependWatchReactor("podcertificaterequests", func(action ktesting.Action) (bool, watch.Interface, error) {
				if action.GetResource().Version != version {
					return true, nil, fmt.Errorf("wrong watch version")
				}
				watching <- struct{}{}
				return true, events, nil
			})
			informer := client.Informer()
			observed := make(chan any, 4)
			_, err = informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(obj any) { observed <- obj },
				UpdateFunc: func(_, obj any) { observed <- obj },
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan struct{})
			go func() { defer close(done); informer.Run(ctx.Done()) }()
			defer func() { cancel(); <-done }()
			receive := func() *certsv1beta1.PodCertificateRequest {
				select {
				case native := <-observed:
					if reflect.TypeOf(native) != reflect.TypeOf(object(pcr)) {
						t.Fatalf("informer changed object type: %T", native)
					}
					cached, exists, err := informer.GetIndexer().GetByKey("test/request")
					if err != nil || !exists {
						t.Fatalf("cache lookup: exists=%v, err=%v", exists, err)
					}
					if reflect.TypeOf(cached) != reflect.TypeOf(native) {
						t.Fatalf("cache changed object type: %T", cached)
					}
					got, err := client.GetCached("test", "request")
					if err != nil {
						t.Fatal(err)
					}
					return got
				case <-ctx.Done():
					t.Fatal("timed out waiting for informer")
					return nil
				}
			}
			if _, err := client.GetCached("test", "missing"); !apierrors.IsNotFound(err) {
				t.Fatalf("expected NotFound for missing request, got %v", err)
			}
			got := receive()
			if !reflect.DeepEqual(got.Spec, pcr.Spec) || !reflect.DeepEqual(got.ObjectMeta, pcr.ObjectMeta) {
				t.Fatalf("list conversion lost fields: %#v", got)
			}
			select {
			case <-watching:
			case <-ctx.Done():
				t.Fatal("watch did not start")
			}
			updated := pcr.DeepCopy()
			updated.ResourceVersion = "124"
			events.Modify(object(updated))
			got = receive().DeepCopy()
			if got.ResourceVersion != "124" {
				t.Fatal("watch update lost resource version")
			}
			now := metav1.Now()
			got.Status = certsv1beta1.PodCertificateRequestStatus{
				Conditions:       []metav1.Condition{{Type: "Issued", Status: metav1.ConditionTrue, Reason: "Issued", LastTransitionTime: now}},
				CertificateChain: "certificate", NotBefore: &now, BeginRefreshAt: &now, NotAfter: &now,
			}
			wrote := false
			kc.PrependReactor("update", "podcertificaterequests", func(action ktesting.Action) (bool, runtime.Object, error) {
				if action.GetResource().Version != version || action.GetSubresource() != "status" {
					return true, nil, fmt.Errorf("wrong status endpoint")
				}
				obj := action.(ktesting.UpdateAction).GetObject()
				actual := obj
				if version == "v1" {
					actual = toBeta(obj.(*certsv1.PodCertificateRequest))
				}
				if !reflect.DeepEqual(actual, got) {
					return true, nil, fmt.Errorf("status conversion lost fields: %#v", actual)
				}
				wrote = true
				return true, obj, nil
			})
			if err := client.UpdateStatus(ctx, got); err != nil {
				t.Fatal(err)
			}
			if !wrote {
				t.Fatal("status was not written")
			}
			kc.PrependReactor("update", "podcertificaterequests", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewConflict(schema.GroupResource{}, "request", fmt.Errorf("changed"))
			})
			if err := client.UpdateStatus(ctx, got); !apierrors.IsConflict(err) {
				t.Fatalf("lost conflict error: %v", err)
			}
		})
	}
}

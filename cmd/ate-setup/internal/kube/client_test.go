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

package kube

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery/cached/memory"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/restmapper"
)

// Installing a CRD and then applying an instance of it is the whole reason
// InvalidateDiscovery exists, and it is easy to get wrong: emptying the
// discovery cache does nothing on its own, because the deferred mapper keeps
// the delegate it already built.
func TestInvalidateDiscoveryRebuildsTheRESTMapper(t *testing.T) {
	cs := kubefake.NewSimpleClientset()
	cs.Fake.Resources = []*metav1.APIResourceList{{
		GroupVersion: "v1",
		APIResources: []metav1.APIResource{{Name: "secrets", Kind: "Secret", Namespaced: true}},
	}}

	cached := memory.NewMemCacheClient(cs.Discovery())
	c := &Client{
		discovery: cached,
		mapper:    restmapper.NewDeferredDiscoveryRESTMapper(cached),
	}

	sandboxConfig := schema.GroupKind{Group: "ate.dev", Kind: "SandboxConfig"}
	if _, err := c.mapper.RESTMapping(sandboxConfig, "v1alpha1"); err == nil {
		t.Fatal("RESTMapping() resolved SandboxConfig before its CRD was installed, want an error")
	}

	// The CRD lands, as `deploy crds` would install it.
	cs.Fake.Resources = append(cs.Fake.Resources, &metav1.APIResourceList{
		GroupVersion: "ate.dev/v1alpha1",
		APIResources: []metav1.APIResource{{Name: "sandboxconfigs", Kind: "SandboxConfig", Namespaced: true}},
	})

	c.InvalidateDiscovery()

	mapping, err := c.mapper.RESTMapping(sandboxConfig, "v1alpha1")
	if err != nil {
		t.Fatalf("RESTMapping() after InvalidateDiscovery() error = %v, want the new CRD to be mappable", err)
	}
	want := schema.GroupVersionResource{Group: "ate.dev", Version: "v1alpha1", Resource: "sandboxconfigs"}
	if mapping.Resource != want {
		t.Errorf("mapping.Resource = %v, want %v", mapping.Resource, want)
	}

	// The reverse direction is the one that is silently wrong. A failed lookup
	// makes the deferred mapper re-discover on its own, so a missing kind
	// recovers either way; a mapping that still resolves is simply returned,
	// stale, and the apply goes to a resource that is no longer there.
	cs.Fake.Resources = cs.Fake.Resources[:1]
	c.InvalidateDiscovery()

	if _, err := c.mapper.RESTMapping(sandboxConfig, "v1alpha1"); err == nil {
		t.Error("RESTMapping() still resolved SandboxConfig after its CRD left discovery, want an error")
	}
}

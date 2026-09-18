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
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Teardown walks a fixed list of manifests covering every install shape, so a
// path the running configuration never referenced is expected to be absent.
//
// The zero-value Client has no cluster connection: reaching the delete calls
// would panic, which is the point. A missing path must short-circuit before
// any request rather than fail the surrounding teardown loop.
func TestDeletePathIgnoresMissingPath(t *testing.T) {
	c := &Client{}

	for _, name := range []string{"absent.yaml", "absent-dir"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := c.DeletePath(context.Background(), path); err != nil {
				t.Errorf("DeletePath(%q) = %v, want nil", path, err)
			}
		})
	}
}

// A path that exists but cannot be parsed is a real problem and must still
// surface, so the ErrNotExist check above does not become a blanket catch.
func TestDeletePathReportsUnparseableManifest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.yaml")
	if err := os.WriteFile(path, []byte("kind: [unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := (&Client{}).DeletePath(context.Background(), path); err == nil {
		t.Error("DeletePath() = nil, want an error for an unparseable manifest")
	}
}

// fakeResource is the slice of the dynamic client ApplyMissing reaches: Get to
// decide, Apply to create. It records applies so the test can tell a kept
// object from a rewritten one.
type fakeResource struct {
	dynamic.NamespaceableResourceInterface
	objs    map[string]*unstructured.Unstructured
	applied []string
}

func (f *fakeResource) Namespace(string) dynamic.ResourceInterface { return f }

func (f *fakeResource) Get(_ context.Context, name string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	obj, ok := f.objs[name]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "ate.dev", Resource: "sandboxconfigs"}, name)
	}
	return obj, nil
}

func (f *fakeResource) Apply(_ context.Context, name string, obj *unstructured.Unstructured, _ metav1.ApplyOptions, _ ...string) (*unstructured.Unstructured, error) {
	f.applied = append(f.applied, name)
	f.objs[name] = obj
	return obj, nil
}

type fakeDynamic struct{ res *fakeResource }

func (f fakeDynamic) Resource(schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return f.res
}

func sandboxConfig(name, marker string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: "ate.dev", Version: "v1alpha1", Kind: "SandboxConfig"})
	obj.SetName(name)
	_ = unstructured.SetNestedField(obj.Object, marker, "spec", "marker")
	return obj
}

// The default SandboxConfig is installed once and then belongs to the
// operator, so a redeploy must create what is missing and leave the rest as it
// found it, edits included.
func TestApplyMissingKeepsExistingObjects(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "ate.dev", Version: "v1alpha1", Kind: "SandboxConfig"}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{gvk.GroupVersion()})
	mapper.Add(gvk, meta.RESTScopeRoot)
	res := &fakeResource{objs: map[string]*unstructured.Unstructured{
		"gvisor-default": sandboxConfig("gvisor-default", "operator-edited"),
	}}
	c := &Client{Dynamic: fakeDynamic{res}, mapper: mapper}

	var kept []string
	err := c.ApplyMissing(context.Background(),
		[]*unstructured.Unstructured{sandboxConfig("gvisor-default", "release"), sandboxConfig("extra", "release")},
		func(obj *unstructured.Unstructured) { kept = append(kept, obj.GetName()) })
	if err != nil {
		t.Fatalf("ApplyMissing() = %v", err)
	}

	if want := []string{"gvisor-default"}; !slices.Equal(kept, want) {
		t.Errorf("kept = %v, want %v", kept, want)
	}
	if want := []string{"extra"}; !slices.Equal(res.applied, want) {
		t.Errorf("applied = %v, want %v", res.applied, want)
	}
	marker, _, _ := unstructured.NestedString(res.objs["gvisor-default"].Object, "spec", "marker")
	if marker != "operator-edited" {
		t.Errorf("gvisor-default spec.marker = %q, want the operator's value kept", marker)
	}
}

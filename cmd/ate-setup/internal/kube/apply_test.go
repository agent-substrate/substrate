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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
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

// stubResource is a dynamicResource whose Apply returns queued results.
type stubResource struct {
	results  []error
	attempts int
}

func (s *stubResource) Apply(context.Context, string, *unstructured.Unstructured, metav1.ApplyOptions, ...string) (*unstructured.Unstructured, error) {
	s.attempts++
	if s.attempts <= len(s.results) {
		return nil, s.results[s.attempts-1]
	}
	return nil, nil
}

func (s *stubResource) Delete(context.Context, string, metav1.DeleteOptions, ...string) error {
	return nil
}

func (s *stubResource) Get(context.Context, string, metav1.GetOptions, ...string) (*unstructured.Unstructured, error) {
	return nil, nil
}

func testConfigMap() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion("v1")
	obj.SetKind("ConfigMap")
	obj.SetNamespace("ate-system")
	obj.SetName("ate-api-server-envvars")
	return obj
}

// shortApplyBackoff keeps the retry tests from sleeping through the real one.
func shortApplyBackoff(t *testing.T) {
	t.Helper()
	original := applyBackoff
	applyBackoff = wait.Backoff{Steps: original.Steps, Duration: time.Millisecond, Factor: 1.0}
	t.Cleanup(func() { applyBackoff = original })
}

// The two failures that routinely hit an apply -- a webhook whose pod is still
// starting, and an object racing the propagation of the CRD or namespace that
// admits it -- used to end the install on the first response.
func TestApplyWithRetryRetriesTransientFailures(t *testing.T) {
	shortApplyBackoff(t)

	ri := &stubResource{results: []error{
		apierrors.NewInternalError(errors.New("failed calling webhook: connection refused")),
		apierrors.NewServiceUnavailable("apiserver is starting"),
		apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "ate-api-server-envvars", errors.New("the object has been modified")),
	}}

	if err := applyWithRetry(t.Context(), ri, testConfigMap()); err != nil {
		t.Fatalf("applyWithRetry() error = %v, want the retries to succeed", err)
	}
	if want := len(ri.results) + 1; ri.attempts != want {
		t.Errorf("apply attempted %d times, want %d", ri.attempts, want)
	}
}

// Retrying a rejection only delays the report, and the message the operator
// needs has to survive the retry wrapper.
func TestApplyWithRetryReportsRejections(t *testing.T) {
	shortApplyBackoff(t)

	ri := &stubResource{results: []error{
		apierrors.NewInvalid(schema.GroupKind{Kind: "ConfigMap"}, "ate-api-server-envvars", nil),
	}}

	err := applyWithRetry(t.Context(), ri, testConfigMap())
	if !apierrors.IsInvalid(err) {
		t.Fatalf("applyWithRetry() error = %v, want the Invalid reported unchanged", err)
	}
	if ri.attempts != 1 {
		t.Errorf("apply attempted %d times, want 1", ri.attempts)
	}
}

// Retrying past a cancelled context turns ^C into an eight-second wait.
func TestApplyWithRetryStopsOnCancellation(t *testing.T) {
	shortApplyBackoff(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	ri := &stubResource{results: []error{
		apierrors.NewServiceUnavailable("apiserver is starting"),
		apierrors.NewServiceUnavailable("apiserver is starting"),
	}}

	if err := applyWithRetry(ctx, ri, testConfigMap()); err == nil {
		t.Fatal("applyWithRetry() succeeded, want the first error returned")
	}
	if ri.attempts != 1 {
		t.Errorf("apply attempted %d times, want 1", ri.attempts)
	}
}

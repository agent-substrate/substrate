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
	"fmt"
	"io/fs"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

// crdKind is the kind whose creation invalidates discovery for later objects.
const crdKind = "CustomResourceDefinition"

// Apply server-side applies every object in order.
//
// The shell scripts piped manifests to `kubectl apply -f -`, which is a
// client-side apply by default. Server-side apply is used instead because it
// is the supported path for repeated reconciliation of the same objects and
// avoids the last-applied-configuration annotation growing unboundedly on the
// large generated CRDs.
func (c *Client) Apply(ctx context.Context, objs []*unstructured.Unstructured) error {
	sawCRD := false
	for _, obj := range objs {
		if sawCRD && obj.GetKind() != crdKind {
			// A CRD earlier in this stream may define this object's kind.
			c.InvalidateDiscovery()
			sawCRD = false
		}
		if err := c.ApplyOne(ctx, obj); err != nil {
			return err
		}
		if obj.GetKind() == crdKind {
			sawCRD = true
		}
	}
	if sawCRD {
		c.InvalidateDiscovery()
	}
	return nil
}

// ApplyOne server-side applies a single object.
func (c *Client) ApplyOne(ctx context.Context, obj *unstructured.Unstructured) error {
	ri, err := c.resourceFor(obj)
	if err != nil {
		return err
	}
	_, err = ri.Apply(ctx, obj.GetName(), obj, metav1.ApplyOptions{
		FieldManager: FieldManager,
		Force:        true,
	})
	if err != nil {
		return fmt.Errorf("while applying %s: %w", Describe(obj), err)
	}
	return nil
}

// ApplyPath applies a manifest file or directory.
func (c *Client) ApplyPath(ctx context.Context, path string) error {
	objs, err := LoadPath(path)
	if err != nil {
		return err
	}
	return c.Apply(ctx, objs)
}

// ApplyBytes applies a multi-document manifest held in memory, such as the
// output of ko resolve or a kustomize build.
func (c *Client) ApplyBytes(ctx context.Context, data []byte) error {
	objs, err := DecodeManifestBytes(data)
	if err != nil {
		return err
	}
	return c.Apply(ctx, objs)
}

// ApplyTolerant applies objects but skips any whose kind the cluster does not
// recognize, reporting the skips through onSkip.
//
// The upstream CSI hostpath bundle ships a VolumeSnapshotClass whose CRD is
// not installed on a stock Kind cluster. The shell installer dealt with that by
// ignoring kubectl's exit code entirely, which also hid real failures; skipping
// only the unmappable kinds keeps everything else strict.
func (c *Client) ApplyTolerant(ctx context.Context, objs []*unstructured.Unstructured, onSkip func(obj *unstructured.Unstructured, err error)) error {
	for _, obj := range objs {
		err := c.ApplyOne(ctx, obj)
		if err != nil && meta.IsNoMatchError(err) {
			if onSkip != nil {
				onSkip(obj, err)
			}
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete removes every object, ignoring those that are already gone. This is
// the `kubectl delete --ignore-not-found -f` equivalent.
func (c *Client) Delete(ctx context.Context, objs []*unstructured.Unstructured) error {
	for _, obj := range objs {
		if err := c.DeleteOne(ctx, obj); err != nil {
			return err
		}
	}
	return nil
}

// DeleteOne removes a single object, ignoring NotFound.
//
// A kind that no longer resolves is also treated as already deleted: tearing
// down after the CRDs have been removed must not fail, which is how
// `kubectl delete --ignore-not-found` behaved for these manifests.
func (c *Client) DeleteOne(ctx context.Context, obj *unstructured.Unstructured) error {
	ri, err := c.resourceFor(obj)
	if err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	err = ri.Delete(ctx, obj.GetName(), metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while deleting %s: %w", Describe(obj), err)
	}
	return nil
}

// DeletePath deletes the objects described by a manifest file or directory.
//
// A path that does not exist is not an error. Teardown runs over a fixed list
// covering every install shape, so a manifest the running configuration never
// referenced is simply absent -- the same reason the shell scripts passed
// --ignore-not-found. Erroring here would abort the loop and strand every
// resource named after the missing file.
func (c *Client) DeletePath(ctx context.Context, path string) error {
	objs, err := LoadPath(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return c.Delete(ctx, objs)
}

// DeleteBytes deletes the objects described by an in-memory manifest.
func (c *Client) DeleteBytes(ctx context.Context, data []byte) error {
	objs, err := DecodeManifestBytes(data)
	if err != nil {
		return err
	}
	return c.Delete(ctx, objs)
}

// Exists reports whether a named object is present.
func (c *Client) Exists(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string) (bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	ri, err := c.resourceFor(obj)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return false, nil
		}
		return false, err
	}
	if _, err := ri.Get(ctx, name, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("while getting %s: %w", Describe(obj), err)
	}
	return true, nil
}

// resourceFor maps an object to its dynamic resource interface, scoped to the
// object's namespace when the resource is namespaced.
func (c *Client) resourceFor(obj *unstructured.Unstructured) (dynamicResource, error) {
	gvk := obj.GroupVersionKind()
	mapping, err := c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		// Discovery may predate a CRD applied moments ago; retry once against
		// fresh discovery data before giving up.
		c.InvalidateDiscovery()
		mapping, err = c.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("while resolving %s: %w", gvk, err)
		}
	}

	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		namespace := obj.GetNamespace()
		if namespace == "" {
			namespace = metav1.NamespaceDefault
		}
		return c.Dynamic.Resource(mapping.Resource).Namespace(namespace), nil
	}
	return c.Dynamic.Resource(mapping.Resource), nil
}

// dynamicResource is the subset of dynamic.ResourceInterface used here; the
// namespaced and cluster-scoped clients both satisfy it.
type dynamicResource interface {
	Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error
	Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error)
}

// pollInterval is how often the wait helpers re-check cluster state.
const pollInterval = 2 * time.Second

// poll runs check until it reports done, the timeout expires, or ctx is
// cancelled. Transient errors from check abort the wait; returning (false, nil)
// means "not ready yet".
func poll(ctx context.Context, timeout time.Duration, check func(context.Context) (bool, error)) error {
	return wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, check)
}

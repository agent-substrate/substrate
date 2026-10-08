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

// Package clustertrustbundle supports stable and beta Kubernetes trust bundle APIs.
package clustertrustbundle

import (
	"context"
	"fmt"
	"sync"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	informersv1 "k8s.io/client-go/informers/certificates/v1"
	informersv1beta1 "k8s.io/client-go/informers/certificates/v1beta1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/certificates/v1"
	listersv1beta1 "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// Discover prefers v1 and falls back only when discovery reports it absent.
func Discover(d discovery.DiscoveryInterface) (schema.GroupVersion, error) {
	for _, gv := range []schema.GroupVersion{certsv1.SchemeGroupVersion, certsv1beta1.SchemeGroupVersion} {
		resources, err := d.ServerResourcesForGroupVersion(gv.String())
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return schema.GroupVersion{}, fmt.Errorf("discover ClusterTrustBundle %s: %w", gv.Version, err)
		}
		for _, resource := range resources.APIResources {
			if resource.Name == "clustertrustbundles" {
				return gv, nil
			}
		}
	}
	return schema.GroupVersion{}, fmt.Errorf("neither v1 nor v1beta1 ClusterTrustBundle is served")
}

// Client uses one API version for live operations and its native informer cache.
// Returned objects use the stable v1 representation.
type Client struct {
	kc           kubernetes.Interface
	v1           bool
	informer     cache.SharedIndexInformer
	informerOnce sync.Once
	tweak        func(*metav1.ListOptions)
}

func NewClient(kc kubernetes.Interface, tweak func(*metav1.ListOptions)) (*Client, error) {
	gv, err := Discover(kc.Discovery())
	if err != nil {
		return nil, err
	}
	return &Client{kc: kc, v1: gv == certsv1.SchemeGroupVersion, tweak: tweak}, nil
}

// Informer lazily creates a shared informer. Live operations do not allocate one.
// The caller must start it once if cached reads are needed.
func (c *Client) Informer() cache.SharedIndexInformer {
	c.informerOnce.Do(func() {
		if c.v1 {
			c.informer = informersv1.NewFilteredClusterTrustBundleInformer(c.kc, 24*time.Hour, cache.Indexers{}, c.tweak)
		} else {
			c.informer = informersv1beta1.NewFilteredClusterTrustBundleInformer(c.kc, 24*time.Hour, cache.Indexers{}, c.tweak)
		}
	})
	return c.informer
}

// GetCached returns a read-only bundle from this client's informer cache.
func (c *Client) GetCached(name string) (*certsv1.ClusterTrustBundle, error) {
	if c.v1 {
		return listersv1.NewClusterTrustBundleLister(c.Informer().GetIndexer()).Get(name)
	}
	bundle, err := listersv1beta1.NewClusterTrustBundleLister(c.Informer().GetIndexer()).Get(name)
	return ToV1(bundle), err
}

func (c *Client) List(ctx context.Context, opts metav1.ListOptions) (*certsv1.ClusterTrustBundleList, error) {
	if c.v1 {
		return c.kc.CertificatesV1().ClusterTrustBundles().List(ctx, opts)
	}
	list, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().List(ctx, opts)
	if err != nil {
		return nil, err
	}
	converted := &certsv1.ClusterTrustBundleList{ListMeta: list.ListMeta, Items: make([]certsv1.ClusterTrustBundle, len(list.Items))}
	for i := range list.Items {
		converted.Items[i] = *ToV1(&list.Items[i])
	}
	return converted, nil
}

// ToBeta copies a stable bundle into the beta representation.
func ToBeta(bundle *certsv1.ClusterTrustBundle) *certsv1beta1.ClusterTrustBundle {
	if bundle == nil {
		return nil
	}
	return &certsv1beta1.ClusterTrustBundle{
		TypeMeta:   metav1.TypeMeta{APIVersion: certsv1beta1.SchemeGroupVersion.String(), Kind: "ClusterTrustBundle"},
		ObjectMeta: *bundle.ObjectMeta.DeepCopy(),
		Spec:       certsv1beta1.ClusterTrustBundleSpec(bundle.Spec),
	}
}

// ToV1 copies a beta bundle into the stable representation.
func ToV1(bundle *certsv1beta1.ClusterTrustBundle) *certsv1.ClusterTrustBundle {
	if bundle == nil {
		return nil
	}
	return &certsv1.ClusterTrustBundle{
		TypeMeta:   metav1.TypeMeta{APIVersion: certsv1.SchemeGroupVersion.String(), Kind: "ClusterTrustBundle"},
		ObjectMeta: *bundle.ObjectMeta.DeepCopy(),
		Spec:       certsv1.ClusterTrustBundleSpec(bundle.Spec),
	}
}

func (c *Client) Get(ctx context.Context, name string, opts metav1.GetOptions) (*certsv1.ClusterTrustBundle, error) {
	if c.v1 {
		return c.kc.CertificatesV1().ClusterTrustBundles().Get(ctx, name, opts)
	}
	result, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().Get(ctx, name, opts)
	return ToV1(result), err
}

func (c *Client) Create(ctx context.Context, bundle *certsv1.ClusterTrustBundle, opts metav1.CreateOptions) (*certsv1.ClusterTrustBundle, error) {
	if c.v1 {
		return c.kc.CertificatesV1().ClusterTrustBundles().Create(ctx, bundle, opts)
	}
	result, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().Create(ctx, ToBeta(bundle), opts)
	return ToV1(result), err
}

func (c *Client) Update(ctx context.Context, bundle *certsv1.ClusterTrustBundle, opts metav1.UpdateOptions) (*certsv1.ClusterTrustBundle, error) {
	if c.v1 {
		return c.kc.CertificatesV1().ClusterTrustBundles().Update(ctx, bundle, opts)
	}
	result, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().Update(ctx, ToBeta(bundle), opts)
	return ToV1(result), err
}

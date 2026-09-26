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

// Package clustertrustbundle selects a served CTB API and presents its
// contents in the beta representation used by the signers.
package clustertrustbundle

import (
	"context"
	"fmt"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Client uses one discovered API version for all of its operations.
type Client struct {
	kc kubernetes.Interface
	v1 bool
}

// New prefers v1 and falls back to beta only when the CTB resource is absent.
func New(kc kubernetes.Interface) (*Client, error) {
	for _, version := range []string{"v1", "v1beta1"} {
		resources, err := kc.Discovery().ServerResourcesForGroupVersion("certificates.k8s.io/" + version)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("discover ClusterTrustBundle %s: %w", version, err)
		}
		for _, resource := range resources.APIResources {
			if resource.Name == "clustertrustbundles" {
				return &Client{kc: kc, v1: version == "v1"}, nil
			}
		}
	}
	return nil, fmt.Errorf("neither v1 nor v1beta1 ClusterTrustBundle is served")
}

// V1 reports whether the selected version is stable.
func (c *Client) V1() bool { return c.v1 }

// Get returns an independent beta representation of the selected API's object.
func (c *Client) Get(ctx context.Context, name string) (*certsv1beta1.ClusterTrustBundle, error) {
	if !c.v1 {
		return c.kc.CertificatesV1beta1().ClusterTrustBundles().Get(ctx, name, metav1.GetOptions{})
	}
	ctb, err := c.kc.CertificatesV1().ClusterTrustBundles().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return toBeta(ctb), nil
}

// List returns beta representations from the selected version.
func (c *Client) List(ctx context.Context, opts metav1.ListOptions) ([]certsv1beta1.ClusterTrustBundle, error) {
	if !c.v1 {
		ctbs, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().List(ctx, opts)
		if err != nil {
			return nil, err
		}
		return ctbs.Items, nil
	}
	ctbs, err := c.kc.CertificatesV1().ClusterTrustBundles().List(ctx, opts)
	if err != nil {
		return nil, err
	}
	items := make([]certsv1beta1.ClusterTrustBundle, 0, len(ctbs.Items))
	for i := range ctbs.Items {
		items = append(items, *toBeta(&ctbs.Items[i]))
	}
	return items, nil
}

// Create publishes through the selected version.
func (c *Client) Create(ctx context.Context, ctb *certsv1beta1.ClusterTrustBundle) error {
	if !c.v1 {
		_, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().Create(ctx, ctb, metav1.CreateOptions{})
		return err
	}
	_, err := c.kc.CertificatesV1().ClusterTrustBundles().Create(ctx, toV1(ctb), metav1.CreateOptions{})
	return err
}

// Update preserves metadata from Get and writes through the selected version.
func (c *Client) Update(ctx context.Context, ctb *certsv1beta1.ClusterTrustBundle) error {
	if !c.v1 {
		_, err := c.kc.CertificatesV1beta1().ClusterTrustBundles().Update(ctx, ctb, metav1.UpdateOptions{})
		return err
	}
	_, err := c.kc.CertificatesV1().ClusterTrustBundles().Update(ctx, toV1(ctb), metav1.UpdateOptions{})
	return err
}

// Informer watches the selected API version, optionally narrowing by object name.
func (c *Client) Informer(name string) cache.SharedIndexInformer {
	options := []informers.SharedInformerOption{}
	if name != "" {
		options = append(options, informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
		}))
	}
	factory := informers.NewSharedInformerFactoryWithOptions(c.kc, 24*time.Hour, options...)
	if c.v1 {
		return factory.Certificates().V1().ClusterTrustBundles().Informer()
	}
	return factory.Certificates().V1beta1().ClusterTrustBundles().Informer()
}

// CachedBundle returns a beta representation from an informer indexer.
func CachedBundle(informer cache.SharedIndexInformer, v1 bool, name string) (*certsv1beta1.ClusterTrustBundle, error) {
	obj, exists, err := informer.GetIndexer().GetByKey(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(certsv1beta1.Resource("clustertrustbundles"), name)
	}
	if v1 {
		return toBeta(obj.(*certsv1.ClusterTrustBundle)), nil
	}
	return obj.(*certsv1beta1.ClusterTrustBundle), nil
}

func toBeta(ctb *certsv1.ClusterTrustBundle) *certsv1beta1.ClusterTrustBundle {
	return &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: *ctb.ObjectMeta.DeepCopy(),
		Spec: certsv1beta1.ClusterTrustBundleSpec{
			SignerName: ctb.Spec.SignerName, TrustBundle: ctb.Spec.TrustBundle,
		},
	}
}

func toV1(ctb *certsv1beta1.ClusterTrustBundle) *certsv1.ClusterTrustBundle {
	return &certsv1.ClusterTrustBundle{
		ObjectMeta: *ctb.ObjectMeta.DeepCopy(),
		Spec: certsv1.ClusterTrustBundleSpec{
			SignerName: ctb.Spec.SignerName, TrustBundle: ctb.Spec.TrustBundle,
		},
	}
}

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
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	informersv1 "k8s.io/client-go/informers/certificates/v1"
	informersv1beta1 "k8s.io/client-go/informers/certificates/v1beta1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/certificates/v1"
	listersv1beta1 "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// Client selects one served PCR API version for both watches and status updates.
// Signers use the beta representation internally to retain legacy public keys.
type Client struct {
	kc       kubernetes.Interface
	v1       bool
	informer cache.SharedIndexInformer
}

// NewClient prefers v1 and falls back only when discovery reports it absent.
func NewClient(kc kubernetes.Interface) (*Client, error) {
	for _, version := range []string{"v1", "v1beta1"} {
		resources, err := kc.Discovery().ServerResourcesForGroupVersion("certificates.k8s.io/" + version)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("discover PodCertificateRequest %s: %w", version, err)
		}
		for _, resource := range resources.APIResources {
			if resource.Name == "podcertificaterequests" {
				client := &Client{kc: kc, v1: version == "v1"}
				indexers := cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}
				if client.v1 {
					client.informer = informersv1.NewPodCertificateRequestInformer(kc, metav1.NamespaceAll, 24*time.Hour, indexers)
				} else {
					client.informer = informersv1beta1.NewPodCertificateRequestInformer(kc, metav1.NamespaceAll, 24*time.Hour, indexers)
				}
				return client, nil
			}
		}
	}
	return nil, fmt.Errorf("neither v1 nor v1beta1 PodCertificateRequest is served")
}

// Informer returns this client's informer, which caches the selected API version.
// The caller must run it once for all controllers sharing this client.
func (c *Client) Informer() cache.SharedIndexInformer {
	return c.informer
}

// GetCached reads from the owned informer cache and returns the signers' beta representation.
// Callers must treat the result as read-only, as with a standard lister.
func (c *Client) GetCached(namespace, name string) (*certsv1beta1.PodCertificateRequest, error) {
	if !c.v1 {
		return listersv1beta1.NewPodCertificateRequestLister(c.informer.GetIndexer()).PodCertificateRequests(namespace).Get(name)
	}
	pcr, err := listersv1.NewPodCertificateRequestLister(c.informer.GetIndexer()).PodCertificateRequests(namespace).Get(name)
	if err != nil {
		return nil, err
	}
	return toBeta(pcr), nil
}

// UpdateStatus writes through the same API version used by the informer.
func (c *Client) UpdateStatus(ctx context.Context, pcr *certsv1beta1.PodCertificateRequest) error {
	if !c.v1 {
		_, err := c.kc.CertificatesV1beta1().PodCertificateRequests(pcr.Namespace).UpdateStatus(ctx, pcr, metav1.UpdateOptions{})
		return err
	}
	converted := toV1(pcr)
	_, err := c.kc.CertificatesV1().PodCertificateRequests(pcr.Namespace).UpdateStatus(ctx, converted, metav1.UpdateOptions{})
	return err
}

// toBeta copies the fields shared by the stable and beta APIs.
func toBeta(pcr *certsv1.PodCertificateRequest) *certsv1beta1.PodCertificateRequest {
	pcr = pcr.DeepCopy()
	return &certsv1beta1.PodCertificateRequest{
		TypeMeta:   metav1.TypeMeta{APIVersion: certsv1beta1.SchemeGroupVersion.String(), Kind: pcr.Kind},
		ObjectMeta: pcr.ObjectMeta,
		Spec: certsv1beta1.PodCertificateRequestSpec{
			SignerName:                pcr.Spec.SignerName,
			PodName:                   pcr.Spec.PodName,
			PodUID:                    pcr.Spec.PodUID,
			ServiceAccountName:        pcr.Spec.ServiceAccountName,
			ServiceAccountUID:         pcr.Spec.ServiceAccountUID,
			NodeName:                  pcr.Spec.NodeName,
			NodeUID:                   pcr.Spec.NodeUID,
			MaxExpirationSeconds:      pcr.Spec.MaxExpirationSeconds,
			StubPKCS10Request:         pcr.Spec.StubPKCS10Request,
			UnverifiedUserAnnotations: pcr.Spec.UnverifiedUserAnnotations,
		},
		Status: certsv1beta1.PodCertificateRequestStatus(pcr.Status),
	}
}

// toV1 copies the fields shared by the stable and beta APIs.
func toV1(pcr *certsv1beta1.PodCertificateRequest) *certsv1.PodCertificateRequest {
	pcr = pcr.DeepCopy()
	return &certsv1.PodCertificateRequest{
		TypeMeta:   metav1.TypeMeta{APIVersion: certsv1.SchemeGroupVersion.String(), Kind: pcr.Kind},
		ObjectMeta: pcr.ObjectMeta,
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                pcr.Spec.SignerName,
			PodName:                   pcr.Spec.PodName,
			PodUID:                    pcr.Spec.PodUID,
			ServiceAccountName:        pcr.Spec.ServiceAccountName,
			ServiceAccountUID:         pcr.Spec.ServiceAccountUID,
			NodeName:                  pcr.Spec.NodeName,
			NodeUID:                   pcr.Spec.NodeUID,
			MaxExpirationSeconds:      pcr.Spec.MaxExpirationSeconds,
			StubPKCS10Request:         pcr.Spec.StubPKCS10Request,
			UnverifiedUserAnnotations: pcr.Spec.UnverifiedUserAnnotations,
		},
		Status: certsv1.PodCertificateRequestStatus(pcr.Status),
	}
}

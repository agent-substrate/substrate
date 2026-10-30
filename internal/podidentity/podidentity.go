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

// Package podidentity requests certificates from substrate's podidentity
// signer for processes that do not run in a pod.
package podidentity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

// SignerName is the podidentity signer, which issues the SPIFFE certificates
// the ingress router verifies.
const SignerName = "podidentity.podcert.ate.dev/identity"

// RequestCertificate creates a PodCertificateRequest for serviceAccount in
// namespace, waits for the podidentity signer to issue it, and deletes it
// again. The key never leaves this process.
//
// The pod, ServiceAccount and node the request names do not exist:
// kube-apiserver only checks them for requests a kubelet creates, and the
// signer takes the identity from the request. requester names the node and
// prefixes the request's name, so the request shows who made it.
func RequestCertificate(ctx context.Context, clientset kubernetes.Interface, requester, namespace, serviceAccount string, timeout time.Duration) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating key: %w", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("creating stub CSR: %w", err)
	}

	pcr := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{GenerateName: requester + "-", Namespace: namespace},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:           SignerName,
			PodName:              requester,
			PodUID:               uuid.NewUUID(),
			ServiceAccountName:   serviceAccount,
			ServiceAccountUID:    uuid.NewUUID(),
			NodeName:             types.NodeName(requester),
			NodeUID:              uuid.NewUUID(),
			MaxExpirationSeconds: ptr.To(int32(24 * 60 * 60)),
			StubPKCS10Request:    csr,
		},
	}
	pcrs, err := podCertificateRequests(clientset, namespace)
	if err != nil {
		return tls.Certificate{}, err
	}
	name, err := pcrs.create(ctx, pcr)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("creating PodCertificateRequest in %s: %w", namespace, err)
	}
	defer func() {
		// A leftover request is harmless: kube-apiserver garbage collects
		// old PodCertificateRequests.
		delCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = pcrs.delete(delCtx, name)
	}()

	var chainPEM string
	err = wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		status, err := pcrs.status(ctx, name)
		if err != nil {
			return false, err
		}
		for _, c := range status.Conditions {
			if c.Status != metav1.ConditionTrue {
				continue
			}
			switch c.Type {
			case certsv1.PodCertificateRequestConditionTypeIssued:
				chainPEM = status.CertificateChain
				return true, nil
			case certsv1.PodCertificateRequestConditionTypeDenied, certsv1.PodCertificateRequestConditionTypeFailed:
				return false, fmt.Errorf("%s: %s: %s", c.Type, c.Reason, c.Message)
			}
		}
		return false, nil
	})
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("waiting for PodCertificateRequest %s/%s to be issued: %w", namespace, name, err)
	}

	cert := tls.Certificate{PrivateKey: key}
	for rest := []byte(chainPEM); ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		cert.Certificate = append(cert.Certificate, block.Bytes)
	}
	if len(cert.Certificate) == 0 {
		return tls.Certificate{}, fmt.Errorf("PodCertificateRequest %s/%s was issued with no certificates", namespace, name)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parsing the issued leaf certificate: %w", err)
	}
	if !key.PublicKey.Equal(leaf.PublicKey) {
		return tls.Certificate{}, fmt.Errorf("the issued certificate does not carry the requested public key")
	}
	cert.Leaf = leaf
	return cert, nil
}

// pcrClient creates, reads and deletes PodCertificateRequests through
// whichever of certificates.k8s.io v1 and v1beta1 the cluster serves,
// preferring v1, as the podcertificate controller does.
type pcrClient struct {
	clientset kubernetes.Interface
	namespace string
	v1        bool
}

func podCertificateRequests(clientset kubernetes.Interface, namespace string) (*pcrClient, error) {
	for _, version := range []string{"v1", "v1beta1"} {
		resources, err := clientset.Discovery().ServerResourcesForGroupVersion("certificates.k8s.io/" + version)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("discovering PodCertificateRequest %s: %w", version, err)
		}
		for _, r := range resources.APIResources {
			if r.Name == "podcertificaterequests" {
				return &pcrClient{clientset: clientset, namespace: namespace, v1: version == "v1"}, nil
			}
		}
	}
	return nil, fmt.Errorf("the cluster serves neither v1 nor v1beta1 PodCertificateRequest")
}

func (c *pcrClient) create(ctx context.Context, pcr *certsv1.PodCertificateRequest) (string, error) {
	if c.v1 {
		created, err := c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Create(ctx, pcr, metav1.CreateOptions{})
		if err != nil {
			return "", err
		}
		return created.Name, nil
	}
	beta := &certsv1beta1.PodCertificateRequest{
		ObjectMeta: pcr.ObjectMeta,
		Spec: certsv1beta1.PodCertificateRequestSpec{
			SignerName:           pcr.Spec.SignerName,
			PodName:              pcr.Spec.PodName,
			PodUID:               pcr.Spec.PodUID,
			ServiceAccountName:   pcr.Spec.ServiceAccountName,
			ServiceAccountUID:    pcr.Spec.ServiceAccountUID,
			NodeName:             pcr.Spec.NodeName,
			NodeUID:              pcr.Spec.NodeUID,
			MaxExpirationSeconds: pcr.Spec.MaxExpirationSeconds,
			StubPKCS10Request:    pcr.Spec.StubPKCS10Request,
		},
	}
	created, err := c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Create(ctx, beta, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return created.Name, nil
}

func (c *pcrClient) status(ctx context.Context, name string) (certsv1.PodCertificateRequestStatus, error) {
	if c.v1 {
		pcr, err := c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return certsv1.PodCertificateRequestStatus{}, err
		}
		return pcr.Status, nil
	}
	pcr, err := c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return certsv1.PodCertificateRequestStatus{}, err
	}
	return certsv1.PodCertificateRequestStatus(pcr.Status), nil
}

func (c *pcrClient) delete(ctx context.Context, name string) error {
	if c.v1 {
		return c.clientset.CertificatesV1().PodCertificateRequests(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}
	return c.clientset.CertificatesV1beta1().PodCertificateRequests(c.namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

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

package podidentitysigner

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/podcertificate"
	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/signercontroller"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const Name = "podidentity.podcert.ate.dev/identity"
const CTBPrefix = "podidentity.podcert.ate.dev:identity:"

type Impl struct {
	kc     kubernetes.Interface
	caPool localca.Pool

	clock clock.PassiveClock
}

func NewImpl(kc kubernetes.Interface, caPool localca.Pool, clock clock.PassiveClock) *Impl {
	return &Impl{
		kc:     kc,
		caPool: caPool,
		clock:  clock,
	}
}

var _ signercontroller.SignerImpl = (*Impl)(nil)

func (h *Impl) SignerName() string {
	return Name
}

func (h *Impl) DesiredClusterTrustBundles() ([]*certsv1beta1.ClusterTrustBundle, error) {
	name := CTBPrefix + "primary-bundle"

	trustAnchors, err := h.caPool.TrustAnchors()
	if err != nil {
		return nil, fmt.Errorf("while retrieving CA pool trust anchors: %w", err)
	}

	wantTrustBundle := bytes.Buffer{}
	for _, anchor := range trustAnchors {
		block := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: anchor.Raw,
		})
		_, _ = wantTrustBundle.Write(block)
	}

	wantCTB := &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"podcert.ate.dev/canarying": "live",
			},
		},
		Spec: certsv1beta1.ClusterTrustBundleSpec{
			SignerName:  Name,
			TrustBundle: wantTrustBundle.String(),
		},
	}

	return []*certsv1beta1.ClusterTrustBundle{
		wantCTB,
	}, nil
}

func (h *Impl) MakeCert(ctx context.Context, pcr *certsv1beta1.PodCertificateRequest) error {
	// Fetch the pod to get its ServiceAccount
	pod, err := h.kc.CoreV1().Pods(pcr.ObjectMeta.Namespace).Get(ctx, pcr.Spec.PodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("while getting pod %s/%s: %w", pcr.ObjectMeta.Namespace, pcr.Spec.PodName, err)
	}

	if pod.ObjectMeta.UID != pcr.Spec.PodUID {
		return fmt.Errorf("pod UID mismatch: expected %s, got %s", pcr.Spec.PodUID, pod.ObjectMeta.UID)
	}

	subjectPublicKey, err := podcertificate.PublicKey(pcr)
	if err != nil {
		return err
	}

	lifetime := 24 * time.Hour
	requestedLifetime := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
	if requestedLifetime < lifetime {
		lifetime = requestedLifetime
	}

	notBefore := h.clock.Now().Add(-2 * time.Minute)
	notAfter := notBefore.Add(lifetime)
	beginRefreshAt := notAfter.Add(-30 * time.Minute)

	spiffeURI := &url.URL{
		Scheme: "spiffe",
		Host:   "cluster.local",
		Path:   path.Join("ns", pcr.ObjectMeta.Namespace, "sa", pcr.Spec.ServiceAccountName),
	}

	template := &x509.Certificate{
		// Some golang certificate handling code assumes that if the parent and
		// template Subject fields compare equal, we are doing a self-signing
		// operation [1].
		//
		// I'm not sure if this is correct, but for defense in depth include
		// some random content in the subject.
		//
		// [1] https://cs.opensource.google/go/go/+/refs/tags/go1.27.0:src/crypto/x509/x509.go;l=1871
		Subject: pkix.Name{
			CommonName: rand.Text(),
		},
		BasicConstraintsValid: true,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		URIs:                  []*url.URL{spiffeURI},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		// AuthorityKeyID is automatically set to the SubjectKeyID of the parent
		// certificate.
	}

	// Fields are sourced from the PCR spec (attested by kube-apiserver) rather
	// than the Pod object, which lacks the ServiceAccount and Node UIDs.
	podIdentity := &substratex509.PodIdentity{
		Namespace:          pcr.ObjectMeta.Namespace,
		ServiceAccountName: pcr.Spec.ServiceAccountName,
		ServiceAccountUID:  string(pcr.Spec.ServiceAccountUID),
		PodName:            pcr.Spec.PodName,
		PodUID:             string(pcr.Spec.PodUID),
		NodeName:           string(pcr.Spec.NodeName),
		NodeUID:            string(pcr.Spec.NodeUID),
	}
	if err := substratex509.AddPodIdentityToCertificate(podIdentity, template); err != nil {
		return fmt.Errorf("while adding pod identity to certificate: %w", err)
	}

	chainDER, err := h.caPool.CreateCertificate(template, subjectPublicKey)
	if err != nil {
		return fmt.Errorf("while signing certificate: %w", err)
	}

	chainPEM := &bytes.Buffer{}
	for _, certDER := range chainDER {
		err = pem.Encode(chainPEM, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certDER,
		})
		if err != nil {
			return fmt.Errorf("while encoding certificate to PEM: %w", err)
		}
	}

	pcr = pcr.DeepCopy()
	pcr.Status.Conditions = []metav1.Condition{
		{
			Type:               certsv1beta1.PodCertificateRequestConditionTypeIssued,
			Status:             metav1.ConditionTrue,
			Reason:             "Reason",
			Message:            "Issued",
			LastTransitionTime: metav1.NewTime(h.clock.Now()),
		},
	}
	pcr.Status.CertificateChain = chainPEM.String()
	pcr.Status.NotBefore = ptr.To(metav1.NewTime(notBefore))
	pcr.Status.BeginRefreshAt = ptr.To(metav1.NewTime(beginRefreshAt))
	pcr.Status.NotAfter = ptr.To(metav1.NewTime(notAfter))

	_, err = h.kc.CertificatesV1beta1().PodCertificateRequests(pcr.ObjectMeta.Namespace).UpdateStatus(ctx, pcr, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("while updating PodCertificateRequest: %w", err)
	}

	return nil
}

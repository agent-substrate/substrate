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

package servicednssigner

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/podcertificate"
	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/signercontroller"
	"github.com/agent-substrate/substrate/internal/localca"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const Name = "servicedns.podcert.ate.dev/identity"

const CTBPrefix = "servicedns.podcert.ate.dev:identity:"

type Impl struct {
	kc     kubernetes.Interface
	caPool *localca.Pool

	clock clock.PassiveClock
}

func NewImpl(kc kubernetes.Interface, caPool *localca.Pool, clock clock.PassiveClock) *Impl {
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

func (h *Impl) DesiredClusterTrustBundles() []*certsv1beta1.ClusterTrustBundle {
	name := CTBPrefix + "primary-bundle"

	wantTrustBundle := bytes.Buffer{}
	for _, ca := range h.caPool.CAs {
		block := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: ca.RootCertificate.Raw,
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
	}
}

func (h *Impl) MakeCert(ctx context.Context, pcr *certsv1beta1.PodCertificateRequest) error {
	// TODO: Switch from live reads to indexer

	// If our signer had a policy about which pods are allowed to request
	// certificates, it would be implemented here.

	svcs, err := h.kc.CoreV1().Services(pcr.ObjectMeta.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("while listing services: %w", err)
	}

	// TODO: Looping over every service isn't great.  Maintain an index of pod
	// to covering services.

	dnsNames := []string{}
	for _, svc := range svcs.Items {
		switch svc.Spec.Type {
		case corev1.ServiceTypeClusterIP, corev1.ServiceTypeNodePort, corev1.ServiceTypeLoadBalancer:
			// ok
		default:
			// This service type doesn't select pods using a label selector.
			continue
		}

		// Find the set of pods that the service selects.
		matchedPods, err := h.kc.CoreV1().Pods(pcr.ObjectMeta.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: svc.Spec.Selector}),
		})
		if err != nil {
			return fmt.Errorf("while selecting pods for service %q: %w", pcr.ObjectMeta.Namespace+"/"+svc.ObjectMeta.Name, err)
		}

		for _, matchedPod := range matchedPods.Items {
			if matchedPod.ObjectMeta.Name == pcr.Spec.PodName && matchedPod.ObjectMeta.UID == pcr.Spec.PodUID {
				// TODO: I'm making some assumptions about the DNS names that
				// resolve to a given Service.  I know at least one
				// configuration that I suspect doesn't match these assumptions
				// --- GKE with VPC-scoped Cloud DNS [1].
				//
				// [1] https://cloud.google.com/kubernetes-engine/docs/how-to/cloud-dns#vpc_scope_dns
				name := fmt.Sprintf("%s.%s.svc", svc.ObjectMeta.Name, svc.ObjectMeta.Namespace)
				dnsNames = append(dnsNames, name)
			}
		}
	}

	// TODO: Encode the OIDC issuer of the cluster into the certificate.

	subjectPublicKey, err := podcertificate.PublicKey(pcr)
	if err != nil {
		return err
	}

	// If our signer had an opinion on which key types were allowable, it would
	// check subjectPublicKey, and deny the PCR with a SuggestedKeyType
	// condition on it.

	lifetime := 24 * time.Hour
	requestedLifetime := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
	if requestedLifetime < lifetime {
		lifetime = requestedLifetime
	}

	notBefore := h.clock.Now().Add(-2 * time.Minute)
	notAfter := notBefore.Add(lifetime)
	beginRefreshAt := notAfter.Add(-30 * time.Minute)

	template := &x509.Certificate{
		BasicConstraintsValid: true,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		DNSNames:              dnsNames,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	subjectCertDER, err := x509.CreateCertificate(rand.Reader, template, h.caPool.CAs[0].RootCertificate, subjectPublicKey, h.caPool.CAs[0].SigningKey)
	if err != nil {
		return fmt.Errorf("while signing subject cert: %w", err)
	}

	chainDER := [][]byte{subjectCertDER}
	for _, intermed := range h.caPool.CAs[0].IntermediateCertificates {
		chainDER = append(chainDER, intermed.Raw)
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

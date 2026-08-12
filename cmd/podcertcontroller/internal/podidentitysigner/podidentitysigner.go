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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const Name = "podidentity.podcert.ate.dev/identity"
const CTBPrefix = "podidentity.podcert.ate.dev:identity:"

// atelet's identity, as installed by manifests/ate-install/atelet.yaml. Pods
// running as atelet serve TLS (e.g. to ate-apiserver), so their certs also
// carry the serverAuth EKU.
const (
	ateletNamespace      = "ate-system"
	ateletServiceAccount = "atelet"
)

// workerPoolLabel marks pods created by atecontroller for a WorkerPool. Worker
// pods host the atunnel ingress server, presenting this cert as a TLS server
// cert to atenet-router, so they need the serverAuth EKU too. They run as the
// actor namespace's default ServiceAccount, so the label is what distinguishes
// them rather than their identity.
const workerPoolLabel = "ate.dev/worker-pool"

func extKeyUsages(pod *corev1.Pod, namespace, serviceAccount string) []x509.ExtKeyUsage {
	usages := []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	_, isWorker := pod.ObjectMeta.Labels[workerPoolLabel]
	isAtelet := namespace == ateletNamespace && serviceAccount == ateletServiceAccount
	if isAtelet || isWorker {
		usages = append(usages, x509.ExtKeyUsageServerAuth)
	}
	return usages
}

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

	parent := h.caPool.CAs[0].RootCertificate

	template := &x509.Certificate{
		BasicConstraintsValid: true,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		URIs:                  []*url.URL{spiffeURI},
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           extKeyUsages(pod, pcr.ObjectMeta.Namespace, pcr.Spec.ServiceAccountName),
		// Link the leaf to its issuing CA by key id so verifiers can disambiguate
		// a multi-CA trust bundle (e.g. valkey trusts both the servicedns and
		// podidentity CAs).
		// https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.1
		AuthorityKeyId: parent.SubjectKeyId,
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

	subjectCertDER, err := x509.CreateCertificate(rand.Reader, template, parent, subjectPublicKey, h.caPool.CAs[0].SigningKey)
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

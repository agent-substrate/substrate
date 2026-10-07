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

// Package postgressigner issues database login certificates to the API server.
package postgressigner

import (
	"bytes"
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/podcertificate"
	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/signercontroller"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/pkg/postgressetup"
	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const Name = "postgres.podcert.ate.dev/identity"

// UsernameAnnotation selects one of the two bundled database logins.
const UsernameAnnotation = "postgres.podcert.ate.dev/username"

const CTBPrefix = "postgres.podcert.ate.dev:identity:"

type Impl struct {
	pcrClient *podcertificate.Client
	caPool    localca.Pool
}

func NewImpl(caPool localca.Pool, pcrClient *podcertificate.Client) *Impl {
	return &Impl{
		pcrClient: pcrClient,
		caPool:    caPool,
	}
}

var _ signercontroller.SignerImpl = (*Impl)(nil)

func (h *Impl) SignerName() string {
	return Name
}

func (h *Impl) DesiredClusterTrustBundles() ([]*certsv1.ClusterTrustBundle, error) {
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

	wantCTB := &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"podcert.ate.dev/canarying": "live",
			},
		},
		Spec: certsv1.ClusterTrustBundleSpec{
			SignerName:  Name,
			TrustBundle: wantTrustBundle.String(),
		},
	}

	return []*certsv1.ClusterTrustBundle{
		wantCTB,
	}, nil
}

func (h *Impl) MakeCert(ctx context.Context, pcr *certsv1beta1.PodCertificateRequest) error {
	if pcr.Spec.SignerName != Name {
		return fmt.Errorf("unexpected signer %q", pcr.Spec.SignerName)
	}
	// kube-apiserver validates the request identity. Authorization is tied to
	// the namespace and service account, not the user-supplied annotations.
	if pcr.Namespace != installdefaults.SystemNamespace || pcr.Spec.ServiceAccountName != "ate-api-server" {
		return h.deny(ctx, pcr, "UnauthorizedServiceAccount", fmt.Sprintf("only %s/ate-api-server may request PostgreSQL login certificates", installdefaults.SystemNamespace))
	}
	// Annotations are untrusted input; this service account may request only
	// the two bundled application logins, never the administrator login.
	username := pcr.Spec.UnverifiedUserAnnotations[UsernameAnnotation]
	if len(pcr.Spec.UnverifiedUserAnnotations) != 1 || (username != postgressetup.OwnerUser && username != postgressetup.ReadWriteUser) {
		return h.deny(ctx, pcr, certsv1beta1.PodCertificateRequestConditionInvalidUserConfig, "request must contain only the username annotation naming a bundled application login")
	}

	subjectPublicKey, err := podcertificate.PublicKey(pcr)
	if err != nil {
		return err
	}

	lifetime := 24 * time.Hour
	if pcr.Spec.MaxExpirationSeconds != nil {
		requestedLifetime := time.Duration(*pcr.Spec.MaxExpirationSeconds) * time.Second
		if requestedLifetime < time.Hour {
			return fmt.Errorf("requested certificate lifetime must be at least one hour")
		}
		if requestedLifetime < lifetime {
			lifetime = requestedLifetime
		}
	}

	notBefore := time.Now().Add(-2 * time.Minute)
	notAfter := notBefore.Add(lifetime)
	beginRefreshAt := notAfter.Add(-30 * time.Minute)

	template := &x509.Certificate{
		Subject:               pkix.Name{CommonName: username},
		BasicConstraintsValid: true,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		// AuthorityKeyID is automatically set to the SubjectKeyID of the parent
		// certificate, as long as we are not self-signing a root.
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
			LastTransitionTime: metav1.NewTime(time.Now()),
		},
	}
	pcr.Status.CertificateChain = chainPEM.String()
	pcr.Status.NotBefore = ptr.To(metav1.NewTime(notBefore))
	pcr.Status.BeginRefreshAt = ptr.To(metav1.NewTime(beginRefreshAt))
	pcr.Status.NotAfter = ptr.To(metav1.NewTime(notAfter))

	err = h.pcrClient.UpdateStatus(ctx, pcr)
	if err != nil {
		return fmt.Errorf("while updating PodCertificateRequest: %w", err)
	}

	return nil
}

func (h *Impl) deny(ctx context.Context, pcr *certsv1beta1.PodCertificateRequest, reason, message string) error {
	pcr = pcr.DeepCopy()
	pcr.Status.Conditions = []metav1.Condition{{
		Type:               certsv1beta1.PodCertificateRequestConditionTypeDenied,
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.NewTime(time.Now()),
	}}
	return h.pcrClient.UpdateStatus(ctx, pcr)
}

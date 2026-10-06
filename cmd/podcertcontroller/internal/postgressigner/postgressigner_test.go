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

package postgressigner

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/podcertcontroller/internal/podcertificate"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/pkg/postgressetup"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

func TestMakeCert(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		mutate                            func(*corev1.Pod, *certsv1beta1.PodCertificateRequest)
		wantDenied, wantError, failUpdate bool
		lifetime                          time.Duration
		wantUsername                      string
	}{
		{name: "runtime login", lifetime: 24 * time.Hour},
		{name: "owner login", lifetime: 24 * time.Hour, wantUsername: postgressetup.OwnerUser, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.UnverifiedUserAnnotations[UsernameAnnotation] = postgressetup.OwnerUser
		}},
		{name: "missing username", wantDenied: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) { p.Spec.UnverifiedUserAnnotations = nil }},
		{name: "administrator username", wantDenied: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.UnverifiedUserAnnotations[UsernameAnnotation] = "postgres"
		}},
		{name: "unknown username", wantDenied: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.UnverifiedUserAnnotations[UsernameAnnotation] = "other"
		}},
		{name: "unknown annotation", wantDenied: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.UnverifiedUserAnnotations["other.example/username"] = postgressetup.OwnerUser
		}},
		{name: "denial status update error", wantError: true, failUpdate: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) { p.Spec.UnverifiedUserAnnotations = nil }},
		{name: "short lifetime", lifetime: time.Hour, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.MaxExpirationSeconds = ptr.To(int32(3600))
		}},
		{name: "capped lifetime", lifetime: 24 * time.Hour, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.MaxExpirationSeconds = ptr.To(int32(7 * 86400))
		}},
		{name: "default lifetime", lifetime: 24 * time.Hour, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) { p.Spec.MaxExpirationSeconds = nil }},
		{name: "wrong namespace", wantDenied: true, mutate: func(pod *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			pod.Namespace = "other"
			p.Namespace = "other"
		}},
		{name: "wrong service account", wantDenied: true, mutate: func(pod *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			pod.Spec.ServiceAccountName = "atelet"
			p.Spec.ServiceAccountName = "atelet"
		}},
		{name: "replaced pod", wantError: true, mutate: func(pod *corev1.Pod, _ *certsv1beta1.PodCertificateRequest) { pod.UID = "replacement" }},
		{name: "pod service account mismatch", wantError: true, mutate: func(pod *corev1.Pod, _ *certsv1beta1.PodCertificateRequest) { pod.Spec.ServiceAccountName = "default" }},
		{name: "missing pod", wantError: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) { p.Spec.PodName = "missing" }},
		{name: "wrong signer", wantError: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.SignerName = "podidentity.podcert.ate.dev/identity"
		}},
		{name: "invalid key", wantError: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.StubPKCS10Request = []byte("invalid")
		}},
		{name: "invalid lifetime", wantError: true, mutate: func(_ *corev1.Pod, p *certsv1beta1.PodCertificateRequest) {
			p.Spec.MaxExpirationSeconds = ptr.To(int32(0))
		}},
		{name: "status update error", wantError: true, failUpdate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca, err := localca.GenerateCA("postgres-test", localca.KeyTypeED25519, 365*24*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			public, private, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			// Requester-supplied subject and extensions must never control the login.
			csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "attacker"}, DNSNames: []string{"attacker"}}, private)
			if err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "api-1", UID: "pod-1"}, Spec: corev1.PodSpec{ServiceAccountName: "ate-api-server"}}
			pcr := &certsv1beta1.PodCertificateRequest{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: "req-1"}, Spec: certsv1beta1.PodCertificateRequestSpec{SignerName: Name, PodName: pod.Name, PodUID: pod.UID, ServiceAccountName: pod.Spec.ServiceAccountName, MaxExpirationSeconds: ptr.To(int32(86400)), StubPKCS10Request: csr, UnverifiedUserAnnotations: map[string]string{UsernameAnnotation: postgressetup.ReadWriteUser}}}
			if tc.mutate != nil {
				tc.mutate(pod, pcr)
			}
			kc := fake.NewSimpleClientset(pod, pcr)
			kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/v1beta1", APIResources: []metav1.APIResource{{Name: "podcertificaterequests"}}}}
			client, err := podcertificate.NewClient(kc)
			if err != nil {
				t.Fatal(err)
			}
			if tc.failUpdate {
				kc.PrependReactor("update", "podcertificaterequests", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("update failed") })
			}
			impl := NewImpl(kc, &localca.ConcretePool{CAs: []*localca.CA{ca}}, client)
			err = impl.MakeCert(t.Context(), pcr)
			if (err != nil) != tc.wantError {
				t.Fatalf("MakeCert error = %v, wantError %v", err, tc.wantError)
			}
			got, err := kc.CertificatesV1beta1().PodCertificateRequests(pcr.Namespace).Get(t.Context(), pcr.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantError {
				if got.Status.CertificateChain != "" || len(got.Status.Conditions) != 0 {
					t.Fatal("failed request received certificate or status")
				}
				return
			}
			condition := certsv1beta1.PodCertificateRequestConditionTypeIssued
			if tc.wantDenied {
				condition = certsv1beta1.PodCertificateRequestConditionTypeDenied
			}
			if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != condition || got.Status.Conditions[0].Status != metav1.ConditionTrue {
				t.Fatalf("unexpected conditions: %+v", got.Status.Conditions)
			}
			if tc.wantDenied {
				if got.Status.CertificateChain != "" {
					t.Fatal("unauthorized pod received certificate")
				}
				return
			}
			block, _ := pem.Decode([]byte(got.Status.CertificateChain))
			if block == nil {
				t.Fatal("missing certificate")
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			roots.AddCert(ca.RootCertificate)
			if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
				t.Fatal(err)
			}
			wantUsername := tc.wantUsername
			if wantUsername == "" {
				wantUsername = postgressetup.ReadWriteUser
			}
			if leaf.Subject.CommonName != wantUsername || !slices.Equal(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}) || len(leaf.DNSNames) != 0 || len(leaf.URIs) != 0 {
				t.Fatalf("unexpected certificate identity/usages: %+v", leaf)
			}
			if !leaf.PublicKey.(ed25519.PublicKey).Equal(public) {
				t.Fatal("certificate has wrong public key")
			}
			if leaf.NotAfter.Sub(leaf.NotBefore) != tc.lifetime {
				t.Fatalf("lifetime = %v, want %v", leaf.NotAfter.Sub(leaf.NotBefore), tc.lifetime)
			}
			if got.Status.NotBefore == nil || got.Status.NotAfter == nil || got.Status.BeginRefreshAt == nil || !got.Status.NotBefore.Time.Truncate(time.Second).Equal(leaf.NotBefore) || !got.Status.NotAfter.Time.Truncate(time.Second).Equal(leaf.NotAfter) || !got.Status.BeginRefreshAt.Time.Truncate(time.Second).Equal(leaf.NotAfter.Add(-30*time.Minute)) {
				t.Fatalf("incorrect refresh times: %+v", got.Status)
			}
		})
	}
}

func TestDesiredClusterTrustBundles(t *testing.T) {
	pool := &localca.ConcretePool{}
	for _, id := range []string{"old", "new"} {
		ca, err := localca.GenerateCA(id, localca.KeyTypeED25519, 365*24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		pool.CAs = append(pool.CAs, ca)
	}
	impl := NewImpl(nil, pool, nil)
	bundles, err := impl.DesiredClusterTrustBundles()
	if err != nil {
		t.Fatal(err)
	}
	if impl.SignerName() != Name || len(bundles) != 1 {
		t.Fatalf("unexpected signer/bundles: %v", bundles)
	}
	bundle := bundles[0]
	if bundle.Name != CTBPrefix+"primary-bundle" || bundle.Spec.SignerName != Name || bundle.Labels["podcert.ate.dev/canarying"] != "live" {
		t.Fatalf("incorrect trust bundle: %+v", bundle)
	}
	want := ""
	for _, ca := range pool.CAs {
		want += string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}))
	}
	if bundle.Spec.TrustBundle != want {
		t.Fatal("trust bundle does not contain both roots")
	}
}

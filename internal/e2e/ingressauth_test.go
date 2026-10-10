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

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// fakePodIdentitySigner answers PodCertificateRequests the way the podidentity
// signer does, signing the stub CSR's key with a throwaway CA.
type fakePodIdentitySigner struct {
	t      *testing.T
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
	uri    func(spec certsv1.PodCertificateRequestSpec) string
	denied bool
	// got is the spec of the last request created.
	got certsv1.PodCertificateRequestSpec
}

func newFakePodIdentitySigner(t *testing.T) *fakePodIdentitySigner {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &fakePodIdentitySigner{
		t: t, ca: ca, caKey: caKey,
		uri: func(spec certsv1.PodCertificateRequestSpec) string {
			// The namespace is not in the spec; the tests use one namespace.
			return installdefaults.SPIFFEID(RouterClientNamespace, spec.ServiceAccountName)
		},
	}
}

func (s *fakePodIdentitySigner) status(spec certsv1.PodCertificateRequestSpec) certsv1.PodCertificateRequestStatus {
	s.t.Helper()
	s.got = spec
	if s.denied {
		return certsv1.PodCertificateRequestStatus{Conditions: []metav1.Condition{{
			Type: certsv1.PodCertificateRequestConditionTypeDenied, Status: metav1.ConditionTrue,
			Reason: "UnsupportedKeyType", Message: "no",
		}}}
	}
	csr, err := x509.ParseCertificateRequest(spec.StubPKCS10Request)
	if err != nil {
		s.t.Fatalf("parsing the stub CSR: %v", err)
	}
	uri, err := url.Parse(s.uri(spec))
	if err != nil {
		s.t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, s.ca, csr.PublicKey, s.caKey)
	if err != nil {
		s.t.Fatal(err)
	}
	return certsv1.PodCertificateRequestStatus{
		Conditions: []metav1.Condition{{
			Type: certsv1.PodCertificateRequestConditionTypeIssued, Status: metav1.ConditionTrue,
			Reason: "Reason", Message: "Issued",
		}},
		CertificateChain: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
	}
}

// install serves PodCertificateRequest at version and has s answer every
// request created through it.
func (s *fakePodIdentitySigner) install(kc *fake.Clientset, version string) {
	kc.Resources = []*metav1.APIResourceList{{
		GroupVersion: "certificates.k8s.io/" + version,
		APIResources: []metav1.APIResource{{Name: "podcertificaterequests", Namespaced: true}},
	}}
	// Returning false hands the mutated object on to the object tracker.
	kc.PrependReactor("create", "podcertificaterequests", func(action k8stesting.Action) (bool, runtime.Object, error) {
		switch pcr := action.(k8stesting.CreateAction).GetObject().(type) {
		case *certsv1.PodCertificateRequest:
			pcr.Status = s.status(pcr.Spec)
		case *certsv1beta1.PodCertificateRequest:
			pcr.Status = certsv1beta1.PodCertificateRequestStatus(s.status(certsv1.PodCertificateRequestSpec{
				SignerName:         pcr.Spec.SignerName,
				ServiceAccountName: pcr.Spec.ServiceAccountName,
				StubPKCS10Request:  pcr.Spec.StubPKCS10Request,
			}))
		default:
			s.t.Fatalf("unexpected PodCertificateRequest type %T", pcr)
		}
		return false, nil, nil
	})
}

func TestRequestPodIdentityCertificate(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			signer := newFakePodIdentitySigner(t)
			signer.install(kc, version)

			cert, err := requestPodIdentityCertificate(context.Background(), kc, RouterClientNamespace, "router-client", time.Second)
			if err != nil {
				t.Fatalf("requestPodIdentityCertificate: %v", err)
			}

			if signer.got.SignerName != podIdentitySignerName {
				t.Errorf("signerName = %q, want %q", signer.got.SignerName, podIdentitySignerName)
			}
			if signer.got.ServiceAccountName != "router-client" {
				t.Errorf("serviceAccountName = %q, want router-client", signer.got.ServiceAccountName)
			}
			if want := RouterClientSPIFFEID("router-client"); cert.Leaf == nil || len(cert.Leaf.URIs) != 1 || cert.Leaf.URIs[0].String() != want {
				t.Fatalf("leaf URIs = %v, want [%s]", cert.Leaf.URIs, want)
			}
			key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
			if !ok || !key.PublicKey.Equal(cert.Leaf.PublicKey) {
				t.Errorf("the private key does not match the leaf certificate")
			}

			// The request is deleted once the certificate is in hand.
			var remaining int
			if version == "v1" {
				list, err := kc.CertificatesV1().PodCertificateRequests(RouterClientNamespace).List(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				remaining = len(list.Items)
			} else {
				list, err := kc.CertificatesV1beta1().PodCertificateRequests(RouterClientNamespace).List(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				remaining = len(list.Items)
			}
			if remaining != 0 {
				t.Errorf("%d PodCertificateRequests left behind, want 0", remaining)
			}
		})
	}
}

func TestRequestPodIdentityCertificateErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, kc *fake.Clientset)
		wantErr string
	}{
		{
			name: "denied",
			setup: func(t *testing.T, kc *fake.Clientset) {
				s := newFakePodIdentitySigner(t)
				s.denied = true
				s.install(kc, "v1")
			},
			wantErr: "Denied",
		},
		{
			name: "wrong SPIFFE ID",
			setup: func(t *testing.T, kc *fake.Clientset) {
				s := newFakePodIdentitySigner(t)
				s.uri = func(certsv1.PodCertificateRequestSpec) string {
					return "spiffe://cluster.local/ns/other/sa/router-client"
				}
				s.install(kc, "v1")
			},
			wantErr: "URI SANs",
		},
		{
			name: "never issued",
			setup: func(t *testing.T, kc *fake.Clientset) {
				kc.Resources = []*metav1.APIResourceList{{
					GroupVersion: "certificates.k8s.io/v1",
					APIResources: []metav1.APIResource{{Name: "podcertificaterequests", Namespaced: true}},
				}}
			},
			wantErr: "to be issued",
		},
		{
			name:    "not served",
			setup:   func(*testing.T, *fake.Clientset) {},
			wantErr: "neither v1 nor v1beta1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			tc.setup(t, kc)
			_, err := requestPodIdentityCertificate(context.Background(), kc, RouterClientNamespace, "router-client", 100*time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("requestPodIdentityCertificate error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

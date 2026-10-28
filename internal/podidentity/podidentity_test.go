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

package podidentity

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testNamespace      = "ns"
	testServiceAccount = "client"
)

// fakePodIdentitySigner answers PodCertificateRequests the way the podidentity
// signer does, signing the stub CSR's key with a throwaway CA.
type fakePodIdentitySigner struct {
	t      *testing.T
	ca     *x509.Certificate
	caKey  *ecdsa.PrivateKey
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
	}
}

func (s *fakePodIdentitySigner) status(spec certsv1.PodCertificateRequestSpec, publicKey crypto.PublicKey) certsv1.PodCertificateRequestStatus {
	s.t.Helper()
	s.got = spec
	if s.denied {
		return certsv1.PodCertificateRequestStatus{Conditions: []metav1.Condition{{
			Type: certsv1.PodCertificateRequestConditionTypeDenied, Status: metav1.ConditionTrue,
			Reason: "UnsupportedKeyType", Message: "no",
		}}}
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, s.ca, publicKey, s.caKey)
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
			pcr.Name = pcr.GenerateName + "abcde"
			csr, err := x509.ParseCertificateRequest(pcr.Spec.StubPKCS10Request)
			if err != nil {
				s.t.Fatalf("parsing the stub CSR: %v", err)
			}
			pcr.Status = s.status(pcr.Spec, csr.PublicKey)
		case *certsv1beta1.PodCertificateRequest:
			pcr.Name = pcr.GenerateName + "abcde"
			// Clusters before Kubernetes 1.36 only know the deprecated pair,
			// and later ones reject it alongside stubPKCS10Request.
			if len(pcr.Spec.StubPKCS10Request) != 0 {
				s.t.Error("v1beta1 request sets stubPKCS10Request")
			}
			publicKey, err := x509.ParsePKIXPublicKey(pcr.Spec.PKIXPublicKey) //nolint:staticcheck // SA1019: the deprecated field is what v1beta1 requests carry.
			if err != nil {
				s.t.Fatalf("parsing pkixPublicKey: %v", err)
			}
			digest := sha256.Sum256([]byte(pcr.Spec.PodUID))
			if !ecdsa.VerifyASN1(publicKey.(*ecdsa.PublicKey), digest[:], pcr.Spec.ProofOfPossession) { //nolint:staticcheck // SA1019: same as above.
				s.t.Error("proofOfPossession does not verify against pkixPublicKey")
			}
			pcr.Status = certsv1beta1.PodCertificateRequestStatus(s.status(certsv1.PodCertificateRequestSpec{
				SignerName:         pcr.Spec.SignerName,
				ServiceAccountName: pcr.Spec.ServiceAccountName,
			}, publicKey))
		default:
			s.t.Fatalf("unexpected PodCertificateRequest type %T", pcr)
		}
		return false, nil, nil
	})
}

func TestRequestCertificate(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			signer := newFakePodIdentitySigner(t)
			signer.install(kc, version)

			cert, err := RequestCertificate(context.Background(), kc, "test", testNamespace, testServiceAccount, time.Second)
			if err != nil {
				t.Fatalf("RequestCertificate: %v", err)
			}

			if signer.got.SignerName != SignerName {
				t.Errorf("signerName = %q, want %q", signer.got.SignerName, SignerName)
			}
			if signer.got.ServiceAccountName != testServiceAccount {
				t.Errorf("serviceAccountName = %q, want %q", signer.got.ServiceAccountName, testServiceAccount)
			}
			if cert.Leaf == nil {
				t.Fatal("cert.Leaf is nil")
			}
			key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
			if !ok || !key.PublicKey.Equal(cert.Leaf.PublicKey) {
				t.Errorf("the private key does not match the leaf certificate")
			}

			// The request is deleted once the certificate is in hand.
			var remaining int
			if version == "v1" {
				list, err := kc.CertificatesV1().PodCertificateRequests(testNamespace).List(context.Background(), metav1.ListOptions{})
				if err != nil {
					t.Fatal(err)
				}
				remaining = len(list.Items)
			} else {
				list, err := kc.CertificatesV1beta1().PodCertificateRequests(testNamespace).List(context.Background(), metav1.ListOptions{})
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

func TestRequestCertificateErrors(t *testing.T) {
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
			_, err := RequestCertificate(context.Background(), kc, "test", testNamespace, testServiceAccount, 100*time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("RequestCertificate error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

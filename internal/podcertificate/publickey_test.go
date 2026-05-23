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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"reflect"
	"testing"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
)

func TestPublicKeyFromStubPKCS10Request(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		t.Fatal(err)
	}

	got, err := PublicKey(&certsv1beta1.PodCertificateRequest{
		Spec: certsv1beta1.PodCertificateRequestSpec{
			StubPKCS10Request: csr,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, &key.PublicKey) {
		t.Fatalf("got %#v, want %#v", got, &key.PublicKey)
	}
}

func TestPublicKeyFallsBackToPKIXPublicKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkix, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}

	got, err := PublicKey(&certsv1beta1.PodCertificateRequest{
		Spec: certsv1beta1.PodCertificateRequestSpec{
			PKIXPublicKey: pkix,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, &key.PublicKey) {
		t.Fatalf("got %#v, want %#v", got, &key.PublicKey)
	}
}

func TestPublicKeyRequiresKeyMaterial(t *testing.T) {
	_, err := PublicKey(&certsv1beta1.PodCertificateRequest{})
	if err == nil {
		t.Fatal("got nil error, want error")
	}
}

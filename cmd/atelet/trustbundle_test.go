//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// testCertPEM mints a throwaway self-signed certificate, PEM-encoded.
func testCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}, &x509.Certificate{SerialNumber: big.NewInt(1)}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func ctbLister(t *testing.T, bundles ...*certsv1beta1.ClusterTrustBundle) certlisters.ClusterTrustBundleLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, b := range bundles {
		if err := indexer.Add(b); err != nil {
			t.Fatal(err)
		}
	}
	return certlisters.NewClusterTrustBundleLister(indexer)
}

// egressTrustBundleObjectName is the backing ClusterTrustBundle the allowlist
// maps EgressTrustBundleName to (named by atecontroller's reconciler).
const egressTrustBundleObjectName = "egress-mitm.ate.dev:mitm:primary-bundle"

func TestResolveTrustBundle(t *testing.T) {
	certPEM := testCertPEM(t)
	junk := "garbage\n" + string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte("x")}))

	t.Run("resolves the allowlisted name through the mapped object and sanitizes", func(t *testing.T) {
		lister := ctbLister(t, &certsv1beta1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
			// Junk around the certificate proves kubelet-style sanitization:
			// only the CERTIFICATE block survives, and the duplicate is dropped.
			Spec: certsv1beta1.ClusterTrustBundleSpec{TrustBundle: junk + string(certPEM) + string(certPEM)},
		})
		got, err := resolveTrustBundle(lister, EgressTrustBundleName)
		if err != nil {
			t.Fatalf("resolveTrustBundle: %v", err)
		}
		if string(got) != string(certPEM) {
			t.Errorf("pem bundle = %q, want the sanitized certificate", got)
		}
	})

	t.Run("unsupported bundle name fails naming it and the allowlist", func(t *testing.T) {
		// The lister has the bundle; the allowlist must still reject it —
		// supported names are a substrate decision, not a cluster lookup.
		lister := ctbLister(t, &certsv1beta1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: "my-own-bundle"},
			Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: string(certPEM)},
		})
		_, err := resolveTrustBundle(lister, "my-own-bundle")
		if err == nil || !strings.Contains(err.Error(), `"my-own-bundle"`) || !strings.Contains(err.Error(), "not supported") || !strings.Contains(err.Error(), EgressTrustBundleName) {
			t.Errorf("error = %v, want unsupported-name error listing the allowlist", err)
		}
	})

	t.Run("missing bundle fails naming it", func(t *testing.T) {
		_, err := resolveTrustBundle(ctbLister(t), EgressTrustBundleName)
		if err == nil || !strings.Contains(err.Error(), egressTrustBundleObjectName) || !strings.Contains(err.Error(), "not found") {
			t.Errorf("error = %v, want not-found naming the backing object", err)
		}
	})

	t.Run("unusable bundle fails naming it", func(t *testing.T) {
		lister := ctbLister(t, &certsv1beta1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
			Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: junk},
		})
		_, err := resolveTrustBundle(lister, EgressTrustBundleName)
		if err == nil || !strings.Contains(err.Error(), "unusable") {
			t.Errorf("error = %v, want unusable-bundle error", err)
		}
	})

	t.Run("nil lister fails with a clear error, not a panic", func(t *testing.T) {
		// A nil lister is a wiring bug (production always registers the
		// informer at boot); it must fail the actor start, not panic the
		// node daemon.
		_, err := resolveTrustBundle(nil, EgressTrustBundleName)
		if err == nil || !strings.Contains(err.Error(), "no ClusterTrustBundle lister") {
			t.Errorf("error = %v, want no-lister error", err)
		}
	})
}

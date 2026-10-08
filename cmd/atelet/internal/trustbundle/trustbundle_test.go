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

package trustbundle

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	certsv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	certlisters "k8s.io/client-go/listers/certificates/v1"
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

func ctbLister(t *testing.T, bundles ...*certsv1.ClusterTrustBundle) certlisters.ClusterTrustBundleLister {
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
// maps EgressName to (named by atecontroller's reconciler).
const egressTrustBundleObjectName = "egress-mitm.ate.dev:mitm:primary-bundle"

// parseBundle returns the DER of every CERTIFICATE block in a PEM bundle,
// sorted so bundles compare independent of the sanitizer's shuffle.
func parseBundle(t *testing.T, bundle []byte) []string {
	t.Helper()
	var ders []string
	for rest := bundle; ; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		ders = append(ders, string(block.Bytes))
	}
	slices.Sort(ders)
	return ders
}

func TestTrustBundleSourceRaw(t *testing.T) {
	certPEM := testCertPEM(t)

	t.Run("resolves the allowlisted name through the mapped object, unsanitized", func(t *testing.T) {
		raw := "garbage\n" + string(certPEM)
		s := &Source{getCTB: ctbLister(t, &certsv1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
			Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: raw},
		}).Get}
		got, err := s.raw(EgressName)
		if err != nil {
			t.Fatalf("raw: %v", err)
		}
		if string(got) != raw {
			t.Errorf("raw = %q, want the backing contents verbatim", got)
		}
	})

	t.Run("unknown bundle name fails naming it", func(t *testing.T) {
		// The lister has the bundle; the allowlist must still reject it —
		// supported names are a substrate decision, not a cluster lookup.
		s := &Source{getCTB: ctbLister(t, &certsv1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: "my-own-bundle"},
			Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: string(certPEM)},
		}).Get}
		_, err := s.raw("my-own-bundle")
		if err == nil || !strings.Contains(err.Error(), `unknown trust bundle "my-own-bundle"`) {
			t.Errorf("error = %v, want unknown-name error naming the bundle", err)
		}
	})

	t.Run("missing bundle fails naming it", func(t *testing.T) {
		s := &Source{getCTB: ctbLister(t).Get}
		_, err := s.raw(EgressName)
		if err == nil || !strings.Contains(err.Error(), egressTrustBundleObjectName) || !strings.Contains(err.Error(), "not found") {
			t.Errorf("error = %v, want not-found naming the backing object", err)
		}
	})
}

func TestTrustBundleSourceCombined(t *testing.T) {
	shared, mitmOnly, mozillaOnly := testCertPEM(t), testCertPEM(t), testCertPEM(t)
	mitmRaw := string(shared) + string(mitmOnly)
	mozillaRaw := slices.Concat(mozillaOnly, shared, mozillaOnly)
	s := &Source{
		getCTB: ctbLister(t, &certsv1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
			Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: mitmRaw},
		}).Get,
		systemRoots: mozillaRaw,
	}

	t.Run("unions and deduplicates across bundles", func(t *testing.T) {
		got, err := s.Combined([]string{EgressName, SystemRootsName})
		if err != nil {
			t.Fatalf("combined: %v", err)
		}
		want := parseBundle(t, slices.Concat(shared, mitmOnly, mozillaOnly))
		if diff := cmp.Diff(want, parseBundle(t, got)); diff != "" {
			t.Errorf("combined anchors mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("output is stable for the same anchors and tracks every bundle", func(t *testing.T) {
		names := []string{EgressName, SystemRootsName}
		first, err := s.Combined(names)
		if err != nil {
			t.Fatalf("combined: %v", err)
		}
		// The same anchors in a different arrangement project identically.
		again, err := s.Combined([]string{SystemRootsName, EgressName})
		if err != nil {
			t.Fatalf("combined: %v", err)
		}
		if !bytes.Equal(first, again) {
			t.Error("output changed across calls with the same anchors")
		}

		rotated := &Source{
			getCTB: ctbLister(t, &certsv1.ClusterTrustBundle{
				ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
				Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: string(testCertPEM(t))},
			}).Get,
			systemRoots: mozillaRaw,
			shuffleSeed: s.shuffleSeed,
		}
		afterRotation, err := rotated.Combined(names)
		if err != nil {
			t.Fatalf("combined: %v", err)
		}
		if bytes.Equal(first, afterRotation) {
			t.Error("output ignores a change to one bundle's contents")
		}
	})

	t.Run("single bundle", func(t *testing.T) {
		got, err := s.Combined([]string{SystemRootsName})
		if err != nil {
			t.Fatalf("combined: %v", err)
		}
		if diff := cmp.Diff(parseBundle(t, slices.Concat(shared, mozillaOnly)), parseBundle(t, got)); diff != "" {
			t.Errorf("anchors mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("any unresolvable name fails", func(t *testing.T) {
		_, err := s.Combined([]string{EgressName, "my-own-bundle"})
		if err == nil || !strings.Contains(err.Error(), "unknown trust bundle") {
			t.Errorf("error = %v, want unknown-name error", err)
		}
	})

	t.Run("no names fails", func(t *testing.T) {
		if _, err := s.Combined(nil); err == nil {
			t.Error("combined(nil) succeeded, want an error")
		}
	})
}

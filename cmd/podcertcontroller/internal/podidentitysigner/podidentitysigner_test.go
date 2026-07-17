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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/substratex509"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
)

// fixedClock is a PassiveClock frozen at a fixed instant.
type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time                  { return c.now }
func (c fixedClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

// testNow is a whole-second instant so times survive the x509 encoding
// round-trip (certificates carry 1s precision) and compare exactly. It must
// stay near wall-clock time because GenerateED25519CA stamps CA validity
// from time.Now().
var testNow = time.Now().UTC().Truncate(time.Second)

// makePodAndPCR returns a pod and a matching PodCertificateRequest with no
// key material set; callers fill in StubPKCS10Request.
func makePodAndPCR(namespace, podName, serviceAccount string, maxExpirationSeconds int32) (*corev1.Pod, *certsv1beta1.PodCertificateRequest) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID("pod-uid-1"),
		},
	}
	pcr := &certsv1beta1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "req-1",
		},
		Spec: certsv1beta1.PodCertificateRequestSpec{
			SignerName:           Name,
			PodName:              pod.ObjectMeta.Name,
			PodUID:               pod.ObjectMeta.UID,
			ServiceAccountName:   serviceAccount,
			ServiceAccountUID:    types.UID("sa-uid-1"),
			NodeName:             types.NodeName("node-1"),
			NodeUID:              types.UID("node-uid-1"),
			MaxExpirationSeconds: ptr.To(maxExpirationSeconds),
		},
	}
	return pod, pcr
}

// stubCSR returns a stub PKCS#10 request carrying priv's public key.
func stubCSR(t *testing.T, priv ed25519.PrivateKey) []byte {
	t.Helper()
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, priv)
	if err != nil {
		t.Fatalf("while creating stub CSR: %v", err)
	}
	return csr
}

func TestMakeCert(t *testing.T) {
	testCases := []struct {
		name                 string
		namespace            string
		podName              string
		serviceAccount       string
		maxExpirationSeconds int32
		wantLifetime         time.Duration
		wantURI              string
		wantEKUs             []x509.ExtKeyUsage
		wantIdentity         *substratex509.PodIdentity
	}{
		{
			name:                 "atelet in ate-system",
			namespace:            "ate-system",
			podName:              "atelet-abcde",
			serviceAccount:       "atelet",
			maxExpirationSeconds: 86400,
			wantLifetime:         24 * time.Hour,
			wantURI:              "spiffe://cluster.local/ns/ate-system/sa/atelet",
			wantEKUs:             []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			wantIdentity: &substratex509.PodIdentity{
				Namespace:          "ate-system",
				ServiceAccountName: "atelet",
				ServiceAccountUID:  "sa-uid-1",
				PodName:            "atelet-abcde",
				PodUID:             "pod-uid-1",
				NodeName:           "node-1",
				NodeUID:            "node-uid-1",
			},
		},
		{
			name:                 "ordinary workload is client-only",
			namespace:            "default",
			podName:              "myapp-0",
			serviceAccount:       "default",
			maxExpirationSeconds: 86400,
			wantLifetime:         24 * time.Hour,
			wantURI:              "spiffe://cluster.local/ns/default/sa/default",
			wantEKUs:             []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			wantIdentity: &substratex509.PodIdentity{
				Namespace:          "default",
				ServiceAccountName: "default",
				ServiceAccountUID:  "sa-uid-1",
				PodName:            "myapp-0",
				PodUID:             "pod-uid-1",
				NodeName:           "node-1",
				NodeUID:            "node-uid-1",
			},
		},
		{
			name:                 "requested lifetime capped at 24h",
			namespace:            "default",
			podName:              "myapp-0",
			serviceAccount:       "default",
			maxExpirationSeconds: 7 * 86400,
			wantLifetime:         24 * time.Hour,
			wantURI:              "spiffe://cluster.local/ns/default/sa/default",
			wantEKUs:             []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			wantIdentity: &substratex509.PodIdentity{
				Namespace:          "default",
				ServiceAccountName: "default",
				ServiceAccountUID:  "sa-uid-1",
				PodName:            "myapp-0",
				PodUID:             "pod-uid-1",
				NodeName:           "node-1",
				NodeUID:            "node-uid-1",
			},
		},
		{
			name:                 "shorter requested lifetime honored",
			namespace:            "default",
			podName:              "myapp-0",
			serviceAccount:       "default",
			maxExpirationSeconds: 3600,
			wantLifetime:         time.Hour,
			wantURI:              "spiffe://cluster.local/ns/default/sa/default",
			wantEKUs:             []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			wantIdentity: &substratex509.PodIdentity{
				Namespace:          "default",
				ServiceAccountName: "default",
				ServiceAccountUID:  "sa-uid-1",
				PodName:            "myapp-0",
				PodUID:             "pod-uid-1",
				NodeName:           "node-1",
				NodeUID:            "node-uid-1",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ca, err := localca.GenerateED25519CA("test-ca")
			if err != nil {
				t.Fatalf("while generating CA: %v", err)
			}
			caPool := &localca.Pool{CAs: []*localca.CA{ca}}

			subjectPub, subjectPriv, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatalf("while generating subject key: %v", err)
			}

			pod, pcr := makePodAndPCR(tc.namespace, tc.podName, tc.serviceAccount, tc.maxExpirationSeconds)
			pcr.Spec.StubPKCS10Request = stubCSR(t, subjectPriv)

			kc := fake.NewSimpleClientset(pod, pcr)
			impl := NewImpl(kc, caPool, fixedClock{now: testNow})

			if err := impl.MakeCert(context.Background(), pcr); err != nil {
				t.Fatalf("MakeCert: %v", err)
			}

			gotPCR, err := kc.CertificatesV1beta1().PodCertificateRequests(tc.namespace).Get(context.Background(), "req-1", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("while fetching updated PCR: %v", err)
			}
			if len(gotPCR.Status.Conditions) != 1 || gotPCR.Status.Conditions[0].Type != certsv1beta1.PodCertificateRequestConditionTypeIssued {
				t.Fatalf("PCR status not marked Issued: %+v", gotPCR.Status.Conditions)
			}

			block, rest := pem.Decode([]byte(gotPCR.Status.CertificateChain))
			if block == nil {
				t.Fatalf("certificate chain contains no PEM block")
			}
			if len(rest) != 0 {
				t.Errorf("expected exactly one certificate in chain (no intermediates), got trailing data")
			}
			leaf, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatalf("while parsing leaf certificate: %v", err)
			}

			roots := x509.NewCertPool()
			roots.AddCert(ca.RootCertificate)
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots:       roots,
				CurrentTime: testNow,
				KeyUsages:   tc.wantEKUs,
			}); err != nil {
				t.Errorf("leaf does not verify against CA root: %v", err)
			}

			leafPub, ok := leaf.PublicKey.(ed25519.PublicKey)
			if !ok || !leafPub.Equal(subjectPub) {
				t.Errorf("leaf public key %v is not the subject key %v", leaf.PublicKey, subjectPub)
			}

			wantNotBefore := testNow.Add(-2 * time.Minute)
			wantNotAfter := wantNotBefore.Add(tc.wantLifetime)
			wantBeginRefreshAt := wantNotAfter.Add(-30 * time.Minute)
			if !leaf.NotBefore.Equal(wantNotBefore) {
				t.Errorf("got NotBefore %v, want %v", leaf.NotBefore, wantNotBefore)
			}
			if !leaf.NotAfter.Equal(wantNotAfter) {
				t.Errorf("got NotAfter %v, want %v", leaf.NotAfter, wantNotAfter)
			}
			if gotPCR.Status.NotBefore == nil || !gotPCR.Status.NotBefore.Time.Equal(wantNotBefore) {
				t.Errorf("got status NotBefore %v, want %v", gotPCR.Status.NotBefore, wantNotBefore)
			}
			if gotPCR.Status.NotAfter == nil || !gotPCR.Status.NotAfter.Time.Equal(wantNotAfter) {
				t.Errorf("got status NotAfter %v, want %v", gotPCR.Status.NotAfter, wantNotAfter)
			}
			if gotPCR.Status.BeginRefreshAt == nil || !gotPCR.Status.BeginRefreshAt.Time.Equal(wantBeginRefreshAt) {
				t.Errorf("got status BeginRefreshAt %v, want %v", gotPCR.Status.BeginRefreshAt, wantBeginRefreshAt)
			}

			if len(leaf.URIs) != 1 || leaf.URIs[0].String() != tc.wantURI {
				t.Errorf("got URIs %v, want [%s]", leaf.URIs, tc.wantURI)
			}
			if !slices.Equal(leaf.ExtKeyUsage, tc.wantEKUs) {
				t.Errorf("got EKUs %v, want %v", leaf.ExtKeyUsage, tc.wantEKUs)
			}
			if !bytes.Equal(leaf.AuthorityKeyId, ca.RootCertificate.SubjectKeyId) {
				t.Errorf("got AuthorityKeyId %x, want CA SubjectKeyId %x", leaf.AuthorityKeyId, ca.RootCertificate.SubjectKeyId)
			}

			identity, err := substratex509.PodIdentityFromCertificate(leaf)
			if err != nil {
				t.Fatalf("while extracting PodIdentity: %v", err)
			}
			if *identity != *tc.wantIdentity {
				t.Errorf("got PodIdentity %+v, want %+v", identity, tc.wantIdentity)
			}
		})
	}
}

func TestMakeCertErrors(t *testing.T) {
	testCases := []struct {
		name       string
		omitPod    bool
		podUID     types.UID
		omitKey    bool
		failUpdate bool
	}{
		{
			name:    "pod not found",
			omitPod: true,
			podUID:  "pod-uid-1",
		},
		{
			name:   "pod UID mismatch",
			podUID: "other-uid",
		},
		{
			name:    "no key material in PCR",
			podUID:  "pod-uid-1",
			omitKey: true,
		},
		{
			name:       "status update fails",
			podUID:     "pod-uid-1",
			failUpdate: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ca, err := localca.GenerateED25519CA("test-ca")
			if err != nil {
				t.Fatalf("while generating CA: %v", err)
			}
			caPool := &localca.Pool{CAs: []*localca.CA{ca}}

			pod, pcr := makePodAndPCR("ate-system", "atelet-abcde", "atelet", 86400)
			pod.ObjectMeta.UID = tc.podUID
			if !tc.omitKey {
				_, subjectPriv, err := ed25519.GenerateKey(rand.Reader)
				if err != nil {
					t.Fatalf("while generating subject key: %v", err)
				}
				pcr.Spec.StubPKCS10Request = stubCSR(t, subjectPriv)
			}

			objects := []runtime.Object{pcr}
			if !tc.omitPod {
				objects = append(objects, pod)
			}
			kc := fake.NewSimpleClientset(objects...)
			if tc.failUpdate {
				kc.PrependReactor("update", "podcertificaterequests", func(action k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("injected update failure")
				})
			}
			impl := NewImpl(kc, caPool, fixedClock{now: testNow})

			if err := impl.MakeCert(context.Background(), pcr); err == nil {
				t.Fatalf("MakeCert: got nil error, want error")
			}

			gotPCR, err := kc.CertificatesV1beta1().PodCertificateRequests("ate-system").Get(context.Background(), "req-1", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("while fetching PCR: %v", err)
			}
			if len(gotPCR.Status.Conditions) != 0 || gotPCR.Status.CertificateChain != "" {
				t.Errorf("PCR status updated despite error: %+v", gotPCR.Status)
			}
		})
	}
}

func TestMakeCertChainIncludesIntermediates(t *testing.T) {
	ca, err := localca.GenerateED25519CA("test-ca")
	if err != nil {
		t.Fatalf("while generating CA: %v", err)
	}
	intermediateCA, err := localca.GenerateED25519CA("test-intermediate")
	if err != nil {
		t.Fatalf("while generating intermediate CA: %v", err)
	}
	ca.IntermediateCertificates = []*x509.Certificate{intermediateCA.RootCertificate}
	caPool := &localca.Pool{CAs: []*localca.CA{ca}}

	_, subjectPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("while generating subject key: %v", err)
	}

	pod, pcr := makePodAndPCR("ate-system", "atelet-abcde", "atelet", 86400)
	pcr.Spec.StubPKCS10Request = stubCSR(t, subjectPriv)

	kc := fake.NewSimpleClientset(pod, pcr)
	impl := NewImpl(kc, caPool, fixedClock{now: testNow})

	if err := impl.MakeCert(context.Background(), pcr); err != nil {
		t.Fatalf("MakeCert: %v", err)
	}

	gotPCR, err := kc.CertificatesV1beta1().PodCertificateRequests("ate-system").Get(context.Background(), "req-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("while fetching updated PCR: %v", err)
	}

	var chainDER [][]byte
	rest := []byte(gotPCR.Status.CertificateChain)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		chainDER = append(chainDER, block.Bytes)
	}
	if len(rest) != 0 {
		t.Errorf("certificate chain has trailing non-PEM data")
	}
	if len(chainDER) != 2 {
		t.Fatalf("got %d certificates in chain, want 2 (leaf + intermediate)", len(chainDER))
	}
	if _, err := x509.ParseCertificate(chainDER[0]); err != nil {
		t.Errorf("while parsing leaf certificate: %v", err)
	}
	if !bytes.Equal(chainDER[1], intermediateCA.RootCertificate.Raw) {
		t.Errorf("second chain entry is not the intermediate certificate")
	}
}

func TestDesiredClusterTrustBundles(t *testing.T) {
	ca1, err := localca.GenerateED25519CA("test-ca-1")
	if err != nil {
		t.Fatalf("while generating CA 1: %v", err)
	}
	ca2, err := localca.GenerateED25519CA("test-ca-2")
	if err != nil {
		t.Fatalf("while generating CA 2: %v", err)
	}
	caPool := &localca.Pool{CAs: []*localca.CA{ca1, ca2}}
	impl := NewImpl(nil, caPool, fixedClock{now: testNow})

	ctbs := impl.DesiredClusterTrustBundles()
	if len(ctbs) != 1 {
		t.Fatalf("got %d ClusterTrustBundles, want 1", len(ctbs))
	}
	ctb := ctbs[0]

	if want := CTBPrefix + "primary-bundle"; ctb.ObjectMeta.Name != want {
		t.Errorf("got CTB name %q, want %q", ctb.ObjectMeta.Name, want)
	}
	if ctb.Spec.SignerName != Name {
		t.Errorf("got signer name %q, want %q", ctb.Spec.SignerName, Name)
	}
	if got := ctb.ObjectMeta.Labels["podcert.ate.dev/canarying"]; got != "live" {
		t.Errorf("got canarying label %q, want %q", got, "live")
	}

	wantBundle := &bytes.Buffer{}
	for _, ca := range caPool.CAs {
		wantBundle.Write(pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: ca.RootCertificate.Raw,
		}))
	}
	if ctb.Spec.TrustBundle != wantBundle.String() {
		t.Errorf("got trust bundle:\n%s\nwant:\n%s", ctb.Spec.TrustBundle, wantBundle.String())
	}
}

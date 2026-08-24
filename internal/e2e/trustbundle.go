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
	"encoding/pem"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// Constants of atecontroller's EgressMITMTrustReconciler (#946): the CA pool
// Secret it watches (the key is what `kubectl-ate admin make-ca-pool` writes)
// and the ClusterTrustBundle it derives from that pool — the backing object
// of the allowlisted "egress-mitm.ate.dev" bundle the probe fixture projects.
//
// Suites provision the POOL and let the real reconciler publish the bundle,
// exercising the whole chain (pool -> reconciler -> bundle -> projection).
// Writing the bundle directly is not an option: the reconciler watches it
// and reverts or deletes hand-written contents.
const (
	// EgressTrustBundleObjectName is the reconciler-owned ClusterTrustBundle.
	EgressTrustBundleObjectName = "egress-mitm.ate.dev:mitm:primary-bundle"

	egressCAPoolNamespace  = "ate-system"
	egressCAPoolSecretName = "egress-mitm-ca-pool"
	egressCAPoolSecretKey  = "pool"
)

// EnsureEgressTrustBundle creates the CA pool only when absent, then waits for
// its trust bundle. It never replaces the pool because probe suites share it.
func EnsureEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients) {
	t.Helper()
	ensureEgressTrustBundle(t, ctx, clients.K8s)
}

func ensureEgressTrustBundle(t *testing.T, ctx context.Context, k8s kubernetes.Interface) {
	t.Helper()
	_, err := k8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
	if err == nil {
		waitForEgressTrustBundle(t, ctx, k8s, "")
		return
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("reading CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
	}
	secret, _ := newEgressTrustPool(t, "ate-e2e-trust")
	if _, err := k8s.CoreV1().Secrets(egressCAPoolNamespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("creating CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
	}
	waitForEgressTrustBundle(t, ctx, k8s, "")
}

// ReplaceEgressTrustPool installs a fresh single-CA pool and waits for its
// trust bundle. It deliberately leaves cleanup to hack/cleanup-e2e.sh because
// concurrently running probe suites share the pool.
func ReplaceEgressTrustPool(t *testing.T, ctx context.Context, clients *Clients, cn string) string {
	t.Helper()
	return replaceEgressTrustPool(t, ctx, clients.K8s, cn)
}

func replaceEgressTrustPool(t *testing.T, ctx context.Context, k8s kubernetes.Interface, cn string) string {
	t.Helper()
	secret, wantPEM := newEgressTrustPool(t, cn)

	if _, err := k8s.CoreV1().Secrets(egressCAPoolNamespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
		}
		existing, getErr := k8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("reading existing CA pool secret: %v", getErr)
		}
		existing.Data = secret.Data
		if _, err := k8s.CoreV1().Secrets(egressCAPoolNamespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating CA pool secret: %v", err)
		}
	}
	waitForEgressTrustBundle(t, ctx, k8s, wantPEM)
	return wantPEM
}

func newEgressTrustPool(t *testing.T, cn string) (*corev1.Secret, string) {
	t.Helper()
	ca, err := localca.GenerateCA(localca.GenerateOptions{ID: "mitm", CommonName: cn, KeyType: localca.KeyTypeECDSAP256})
	if err != nil {
		t.Fatalf("generating CA for the egress pool: %v", err)
	}
	poolBytes, err := localca.Marshal(&localca.Pool{CAs: []*localca.CA{ca}})
	if err != nil {
		t.Fatalf("marshaling the egress pool: %v", err)
	}
	wantPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}))

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: egressCAPoolNamespace, Name: egressCAPoolSecretName},
		Data:       map[string][]byte{egressCAPoolSecretKey: poolBytes},
	}, wantPEM
}

// waitForEgressTrustBundle polls the reconciler-owned bundle until its
// contents match want, or are merely non-empty when want is "", keeping the
// reconcile latency out of later assertions. Accepted race: this polls the
// apiserver while atelet resolves from its informer cache, but the suites'
// start/resume latency dwarfs watch delivery — if a rotated-bundle
// assertion ever flakes, this lag is the first suspect.
func waitForEgressTrustBundle(t *testing.T, ctx context.Context, k8s kubernetes.Interface, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctb, err := k8s.CertificatesV1beta1().ClusterTrustBundles().Get(ctx, EgressTrustBundleObjectName, metav1.GetOptions{})
		if err == nil {
			if got := ctb.Spec.TrustBundle; got == want || (want == "" && got != "") {
				return
			} else {
				last = got
			}
		} else {
			last = "<" + err.Error() + ">"
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for ClusterTrustBundle %q to carry the pool's root certificate (last observed: %.80q...); is atecontroller's EgressMITMTrustReconciler running?", EgressTrustBundleObjectName, last)
}

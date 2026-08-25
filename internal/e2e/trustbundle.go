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

// EnsureEgressTrustBundle makes sure the egress trust bundle exists, then
// waits until the reconciler-published bundle is non-empty. It provisions a
// pool only when there is none and never replaces one it finds: the pool is
// cluster-wide, and the sdsmint gateway mounts the one the install created.
// A suite that needs to OWN the pool's contents (the identity suite's
// deterministic assertions and rotation) uses ReplaceEgressTrustPool.
func EnsureEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients) {
	t.Helper()
	secret, _ := newEgressTrustPool(t, "ate-e2e-trust")
	createEgressTrustPool(t, ctx, clients, secret)
	waitForEgressTrustBundle(t, ctx, clients, "")
}

// ReplaceEgressTrustPool installs a fresh single-CA pool, waits for the
// reconciler to publish the derived bundle, and returns the PEM of the new
// CA's root certificate — exactly what a trustBundle projection must then
// deliver. cn keeps successive pools distinguishable in failure output.
// Create-or-replace keeps reruns self-healing after a failed prior run.
func ReplaceEgressTrustPool(t *testing.T, ctx context.Context, clients *Clients, cn string) string {
	t.Helper()
	secret, wantPEM := newEgressTrustPool(t, cn)
	if !createEgressTrustPool(t, ctx, clients, secret) {
		// Took over an existing pool: overwrite its contents without adopting
		// its cleanup, since whoever created it registered one already.
		existing, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading existing CA pool secret: %v", err)
		}
		existing.Data = secret.Data
		if _, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating CA pool secret: %v", err)
		}
	}
	waitForEgressTrustBundle(t, ctx, clients, wantPEM)
	return wantPEM
}

// newEgressTrustPool builds a fresh single-CA pool Secret — the shape
// `kubectl-ate admin make-ca-pool` writes for the egress MITM CA — and the PEM
// of its root certificate.
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
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: egressCAPoolNamespace, Name: egressCAPoolSecretName},
		Data:       map[string][]byte{egressCAPoolSecretKey: poolBytes},
	}, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}))
}

// createEgressTrustPool creates secret and reports whether this call created
// it, tolerating a pool that already exists so concurrent first users cannot
// clobber each other. The creator — and only the creator — deletes it when its
// test ends, whereupon the reconciler deletes the bundle: a run leaves nothing
// behind, and no caller removes a pool it merely found.
func createEgressTrustPool(t *testing.T, ctx context.Context, clients *Clients, secret *corev1.Secret) bool {
	t.Helper()
	if _, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
		}
		return false
	}
	t.Cleanup(func() {
		_ = clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Delete(context.Background(), egressCAPoolSecretName, metav1.DeleteOptions{})
	})
	return true
}

// waitForEgressTrustBundle polls the reconciler-owned bundle until its
// contents match want, or are merely non-empty when want is "", keeping the
// reconcile latency out of later assertions. Accepted race: this polls the
// apiserver while atelet resolves from its informer cache, but the suites'
// start/resume latency dwarfs watch delivery — if a rotated-bundle
// assertion ever flakes, this lag is the first suspect.
func waitForEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients, want string) {
	t.Helper()
	var last string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctb, err := clients.K8s.CertificatesV1beta1().ClusterTrustBundles().Get(ctx, EgressTrustBundleObjectName, metav1.GetOptions{})
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

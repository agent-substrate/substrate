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

// EnsureEgressTrustBundle makes sure the egress trust bundle exists: if the
// CA pool Secret is absent it provisions one (registering cleanup), then
// waits until the reconciler-published bundle is non-empty. DeployProbe calls
// this because the probe template projects the bundle and every actor —
// including the fixture's golden boot — fails closed while it is missing; a
// suite that needs to OWN the pool's contents (the identity suite's
// deterministic assertions and rotation) uses ReplaceEgressTrustPool first,
// which this then leaves untouched.
func EnsureEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients) {
	t.Helper()
	_, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
	if err == nil {
		waitForEgressTrustBundle(t, ctx, clients, "")
		return
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("reading CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
	}
	ReplaceEgressTrustPool(t, ctx, clients, "ate-e2e-trust")
}

// ReplaceEgressTrustPool creates or replaces the egress CA pool with a fresh
// single-CA pool (the shape `kubectl-ate admin make-ca-pool` creates for the
// egress MITM CA), waits for the reconciler to publish the derived bundle,
// and returns the PEM of the new CA's root certificate — exactly what a
// trustBundle projection must then deliver. cn keeps successive pools
// distinguishable in failure output. Cleanup deletes the Secret, whereupon
// the reconciler deletes the bundle; create-or-replace keeps reruns
// self-healing after a failed prior run.
func ReplaceEgressTrustPool(t *testing.T, ctx context.Context, clients *Clients, cn string) string {
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

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: egressCAPoolNamespace, Name: egressCAPoolSecretName},
		Data:       map[string][]byte{egressCAPoolSecretKey: poolBytes},
	}
	if _, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating CA pool secret %s/%s: %v", egressCAPoolNamespace, egressCAPoolSecretName, err)
		}
		existing, getErr := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
		if getErr != nil {
			t.Fatalf("reading existing CA pool secret: %v", getErr)
		}
		existing.Data = secret.Data
		if _, err := clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating CA pool secret: %v", err)
		}
		// The replacer takes over an existing pool without adopting its
		// cleanup: whoever created it registered one already.
		waitForEgressTrustBundle(t, ctx, clients, wantPEM)
		return wantPEM
	}
	t.Cleanup(func() {
		_ = clients.K8s.CoreV1().Secrets(egressCAPoolNamespace).Delete(context.Background(), egressCAPoolSecretName, metav1.DeleteOptions{})
	})
	waitForEgressTrustBundle(t, ctx, clients, wantPEM)
	return wantPEM
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

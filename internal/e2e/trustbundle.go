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
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
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

	// egressCAPoolSecretName is not release-prefixed: hack/install-ate.sh
	// creates it under this fixed name and the reconciler looks it up the same
	// way, so it does not follow the deployment's naming.
	egressCAPoolSecretName = "egress-mitm-ca-pool"

	egressCAPoolSecretKey = "pool"
)

// EnsureEgressTrustBundle makes sure the egress trust bundle exists, then
// waits until the reconciler-published bundle is non-empty. It provisions a
// pool only when there is none and never replaces one it finds: the pool is
// cluster-wide, and the egress gateway mounts the one the install created.
// A suite that needs to know the bundle's exact contents (the identity
// suite's assertions and live refresh) uses AddEgressCA.
func EnsureEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients) {
	t.Helper()
	createEgressTrustPool(t, ctx, clients, newEgressTrustPool(t))
	waitForEgressTrustBundle(t, ctx, clients, "")
}

// AddEgressCA adds a fresh CA, named id, to the egress CA pool, waits for the
// reconciler to publish the derived bundle, and returns that bundle: what a
// trustBundle projection must then deliver, in any order (see
// SameCertificates). The new CA is trusted but never signs, so every actor
// keeps trusting the leaves the gateway mints, and suites doing TLS through
// the gateway can run alongside. The pool is put back as found when the test
// ends.
func AddEgressCA(t *testing.T, ctx context.Context, clients *Clients, id string) string {
	t.Helper()
	EnsureEgressTrustBundle(t, ctx, clients)

	secrets := clients.K8s.CoreV1().Secrets(SystemNamespace())
	secret, err := secrets.Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("reading CA pool secret: %v", err)
	}
	before := secret.Data[egressCAPoolSecretKey]
	pool, bundle, err := addCA(before, id)
	if err != nil {
		t.Fatalf("adding CA %q to the egress pool: %v", id, err)
	}
	secret.Data[egressCAPoolSecretKey] = pool
	if _, err := secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating CA pool secret: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		secret, err := secrets.Get(ctx, egressCAPoolSecretName, metav1.GetOptions{})
		if err == nil {
			secret.Data[egressCAPoolSecretKey] = before
			_, err = secrets.Update(ctx, secret, metav1.UpdateOptions{})
		}
		if err != nil {
			t.Logf("cleanup: restoring CA pool secret failed, CA %q left in the pool: %v", id, err)
		}
	})
	waitForEgressTrustBundle(t, ctx, clients, bundle)
	return bundle
}

// addCA adds a fresh CA named id to a marshaled pool without changing which
// CA signs, and returns the new pool and the bundle the reconciler derives
// from it.
func addCA(poolBytes []byte, id string) ([]byte, string, error) {
	pool, err := localca.Unmarshal(poolBytes)
	if err != nil {
		return nil, "", fmt.Errorf("parsing the pool: %w", err)
	}
	ca, err := localca.GenerateCA(id, localca.KeyTypeECDSAP256, 365*24*time.Hour)
	if err != nil {
		return nil, "", fmt.Errorf("generating the CA: %w", err)
	}
	// Last, because a pool that names no signer signs with its first CA.
	pool.CAs = append(pool.CAs, ca)
	out, err := localca.Marshal(pool)
	if err != nil {
		return nil, "", fmt.Errorf("marshaling the pool: %w", err)
	}
	var bundle []byte
	for _, ca := range pool.CAs {
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw})...)
	}
	return out, string(bundle), nil
}

// SameCertificates reports whether two bundles are the same PEM CERTIFICATE
// blocks and nothing else. Order is ignored: atelet shuffles a bundle's
// certificates every time it writes one.
func SameCertificates(a, b string) bool {
	x, okX := certificateBlocks(a)
	y, okY := certificateBlocks(b)
	return okX && okY && slices.Equal(x, y)
}

// certificateBlocks splits bundle into its PEM blocks, sorted. It reports
// false if bundle holds anything else: other block types, headers, or text
// around or between the blocks.
func certificateBlocks(bundle string) ([]string, bool) {
	var blocks []string
	for rest := []byte(bundle); len(rest) > 0; {
		block, _ := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) > 0 {
			return nil, false
		}
		encoded := pem.EncodeToMemory(block)
		if !bytes.HasPrefix(rest, encoded) {
			return nil, false
		}
		blocks = append(blocks, string(encoded))
		rest = rest[len(encoded):]
	}
	slices.Sort(blocks)
	return blocks, true
}

// newEgressTrustPool builds a fresh single-CA pool Secret — the shape
// `kubectl-ate admin make-ca-pool` writes for the egress MITM CA.
func newEgressTrustPool(t *testing.T) *corev1.Secret {
	t.Helper()
	ca, err := localca.GenerateCA("mitm", localca.KeyTypeECDSAP256, 365*24*time.Hour)
	if err != nil {
		t.Fatalf("generating CA for the egress pool: %v", err)
	}
	poolBytes, err := localca.Marshal(&localca.ConcretePool{CAs: []*localca.CA{ca}})
	if err != nil {
		t.Fatalf("marshaling the egress pool: %v", err)
	}
	certificateChain, err := ca.TLSCertificateChainPEM()
	if err != nil {
		t.Fatalf("encoding the egress CA certificate chain: %v", err)
	}
	privateKey, err := ca.TLSPrivateKeyPEM()
	if err != nil {
		t.Fatalf("encoding the egress CA private key: %v", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: SystemNamespace(), Name: egressCAPoolSecretName},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			egressCAPoolSecretKey:   poolBytes,
			corev1.TLSCertKey:       certificateChain,
			corev1.TLSPrivateKeyKey: privateKey,
		},
	}
}

// createEgressTrustPool creates secret, tolerating a pool that already exists
// so concurrent first users cannot clobber each other. The creator — and only
// the creator — deletes it when its test ends, whereupon the reconciler
// deletes the bundle: a run leaves nothing behind, and no caller removes a
// pool it merely found.
func createEgressTrustPool(t *testing.T, ctx context.Context, clients *Clients, secret *corev1.Secret) {
	t.Helper()
	if _, err := clients.K8s.CoreV1().Secrets(SystemNamespace()).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			t.Fatalf("creating CA pool secret %s/%s: %v", SystemNamespace(), egressCAPoolSecretName, err)
		}
		return
	}
	t.Cleanup(func() {
		_ = clients.K8s.CoreV1().Secrets(SystemNamespace()).Delete(context.Background(), egressCAPoolSecretName, metav1.DeleteOptions{})
	})
}

// waitForEgressTrustBundle polls the reconciler-owned bundle until its
// contents match want, or are merely non-empty when want is "", keeping the
// reconcile latency out of later assertions. Accepted race: this polls the
// apiserver while atelet resolves from its informer cache, but the suites'
// start/resume latency dwarfs watch delivery — if a rotated-bundle
// assertion ever flakes, this lag is the first suspect.
func waitForEgressTrustBundle(t *testing.T, ctx context.Context, clients *Clients, want string) {
	t.Helper()
	bundles, err := clustertrustbundle.NewClient(clients.K8s, nil)
	if err != nil {
		t.Fatal(err)
	}
	var last string
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		ctb, err := bundles.Get(ctx, EgressTrustBundleObjectName, metav1.GetOptions{})
		if err == nil {
			if got := ctb.Spec.TrustBundle; SameCertificates(got, want) || (want == "" && got != "") {
				return
			} else {
				last = got
			}
		} else {
			last = "<" + err.Error() + ">"
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("timed out waiting for ClusterTrustBundle %q to carry the pool's root certificates (last observed: %.80q...); is atecontroller's EgressMITMTrustReconciler running?", EgressTrustBundleObjectName, last)
}

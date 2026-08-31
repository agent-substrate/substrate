// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"encoding/pem"
	"strings"
	"testing"
	"time"

	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/agent-substrate/substrate/internal/localca"
)

func egressMITMScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

// caPoolSecret marshals ids into a pool Secret shaped the way
// `kubectl-ate admin make-ca-pool` writes it, and returns the roots in pool
// order so a test can assert on exactly what should have been published.
func caPoolSecret(t *testing.T, ids ...string) (*corev1.Secret, *localca.ConcretePool) {
	t.Helper()
	pool := &localca.ConcretePool{}
	for _, id := range ids {
		ca, err := localca.GenerateCA(id, localca.KeyTypeED25519, 24*time.Hour)
		if err != nil {
			t.Fatalf("generate CA %q: %v", id, err)
		}
		pool.CAs = append(pool.CAs, ca)
	}
	return secretForPool(t, pool), pool
}

func secretForPool(t *testing.T, pool *localca.ConcretePool) *corev1.Secret {
	t.Helper()
	wire, err := localca.Marshal(pool)
	if err != nil {
		t.Fatalf("marshal CA pool: %v", err)
	}
	ref := EgressMITMCAPoolRef()
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name},
		Data:       map[string][]byte{"pool": wire},
	}
}

func rootPEM(t *testing.T, pool *localca.ConcretePool) string {
	t.Helper()
	var b strings.Builder
	for _, ca := range pool.CAs {
		if err := pem.Encode(&b, &pem.Block{Type: "CERTIFICATE", Bytes: ca.RootCertificate.Raw}); err != nil {
			t.Fatalf("encode root: %v", err)
		}
	}
	return b.String()
}

func reconcilePool(t *testing.T, c client.Client) error {
	t.Helper()
	r := &EgressMITMTrustReconciler{Client: c}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: EgressMITMCAPoolRef()})
	return err
}

func getTrustBundle(t *testing.T, c client.Client) (*certsv1beta1.ClusterTrustBundle, bool) {
	t.Helper()
	ctb := &certsv1beta1.ClusterTrustBundle{}
	err := c.Get(context.Background(), types.NamespacedName{Name: egressMITMTrustBundleName}, ctb)
	if k8errors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("get ClusterTrustBundle: %v", err)
	}
	return ctb, true
}

func TestEgressMITMTrustPublishesEveryRoot(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	secret, pool := caPoolSecret(t, "mitm", "mitm-next")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctb, ok := getTrustBundle(t, c)
	if !ok {
		t.Fatal("no ClusterTrustBundle was created")
	}
	if got, want := ctb.Spec.SignerName, egressMITMSignerName; got != want {
		t.Errorf("signerName = %q, want %q", got, want)
	}
	if got, want := ctb.Labels["podcert.ate.dev/canarying"], "live"; got != want {
		t.Errorf("canarying label = %q, want %q", got, want)
	}
	// Both roots, in pool order. A pool holds more than one CA so the anchor can
	// be rotated; dropping the outgoing root breaks leaves it is still signing.
	if got, want := ctb.Spec.TrustBundle, rootPEM(t, pool); got != want {
		t.Errorf("trustBundle =\n%s\nwant\n%s", got, want)
	}
}

// The pool carries each CA's signing key. Publishing it would hand every
// consumer of the bundle the ability to mint leaves for any name.
func TestEgressMITMTrustPublishesNoPrivateKey(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	secret, _ := caPoolSecret(t, "mitm")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	ctb, ok := getTrustBundle(t, c)
	if !ok {
		t.Fatal("no ClusterTrustBundle was created")
	}
	for rest := []byte(ctb.Spec.TrustBundle); len(rest) > 0; {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			t.Fatalf("trustBundle has trailing non-PEM bytes: %q", rest)
		}
		if block.Type != "CERTIFICATE" {
			t.Errorf("trustBundle contains a %q block; only CERTIFICATE belongs there", block.Type)
		}
	}
}

func TestEgressMITMTrustFollowsPoolRotation(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	secret, _ := caPoolSecret(t, "mitm")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	rotated, rotatedPool := caPoolSecret(t, "mitm", "mitm-next")
	current := &corev1.Secret{}
	if err := c.Get(context.Background(), EgressMITMCAPoolRef(), current); err != nil {
		t.Fatalf("get pool secret: %v", err)
	}
	current.Data = rotated.Data
	if err := c.Update(context.Background(), current); err != nil {
		t.Fatalf("update pool secret: %v", err)
	}

	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	ctb, ok := getTrustBundle(t, c)
	if !ok {
		t.Fatal("the ClusterTrustBundle disappeared")
	}
	if got, want := ctb.Spec.TrustBundle, rootPEM(t, rotatedPool); got != want {
		t.Errorf("trustBundle after rotation =\n%s\nwant\n%s", got, want)
	}
}

func TestEgressMITMTrustRecreatesDeletedBundle(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	secret, pool := caPoolSecret(t, "mitm")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	ctb, ok := getTrustBundle(t, c)
	if !ok {
		t.Fatal("no ClusterTrustBundle was created")
	}
	if err := c.Delete(context.Background(), ctb); err != nil {
		t.Fatalf("delete ClusterTrustBundle: %v", err)
	}

	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	got, ok := getTrustBundle(t, c)
	if !ok {
		t.Fatal("the ClusterTrustBundle was not recreated")
	}
	if want := rootPEM(t, pool); got.Spec.TrustBundle != want {
		t.Errorf("trustBundle =\n%s\nwant\n%s", got.Spec.TrustBundle, want)
	}
}

func TestEgressMITMTrustDeletesBundleWhenPoolIsGone(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	secret, _ := caPoolSecret(t, "mitm")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if err := c.Delete(context.Background(), secret); err != nil {
		t.Fatalf("delete pool secret: %v", err)
	}

	if err := reconcilePool(t, c); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if _, ok := getTrustBundle(t, c); ok {
		t.Error("the ClusterTrustBundle outlived its CA pool")
	}
}

func TestEgressMITMTrustLeavesForeignBundleAlone(t *testing.T) {
	t.Parallel()
	scheme := egressMITMScheme(t)
	foreign := &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: egressMITMTrustBundleName},
		Spec:       certsv1beta1.ClusterTrustBundleSpec{SignerName: "someone.else.example/identity"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()

	// No pool Secret exists, so this takes the delete path.
	if err := reconcilePool(t, c); err == nil {
		t.Fatal("Reconcile deleted a ClusterTrustBundle belonging to another signer")
	}
	if _, ok := getTrustBundle(t, c); !ok {
		t.Error("the foreign ClusterTrustBundle was deleted")
	}
}

// A pool that cannot be read says nothing about what the anchor should be.
// Truncating the published bundle would break every consumer at once, so the
// last good bundle has to survive an unreadable pool.
func TestEgressMITMTrustKeepsLastGoodBundleOnBadPool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data map[string][]byte
	}{
		{name: "missing key", data: map[string][]byte{"not-pool": []byte("{}")}},
		{name: "unparseable", data: map[string][]byte{"pool": []byte("not json")}},
		{name: "no CAs", data: map[string][]byte{"pool": []byte(`{"CAs":[]}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scheme := egressMITMScheme(t)
			secret, pool := caPoolSecret(t, "mitm")
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
			if err := reconcilePool(t, c); err != nil {
				t.Fatalf("first Reconcile: %v", err)
			}

			current := &corev1.Secret{}
			if err := c.Get(context.Background(), EgressMITMCAPoolRef(), current); err != nil {
				t.Fatalf("get pool secret: %v", err)
			}
			current.Data = tc.data
			if err := c.Update(context.Background(), current); err != nil {
				t.Fatalf("update pool secret: %v", err)
			}

			if err := reconcilePool(t, c); err == nil {
				t.Error("Reconcile accepted an unreadable pool; it should fail and requeue")
			}
			ctb, ok := getTrustBundle(t, c)
			if !ok {
				t.Fatal("the ClusterTrustBundle was removed by an unreadable pool")
			}
			if want := rootPEM(t, pool); ctb.Spec.TrustBundle != want {
				t.Errorf("trustBundle was rewritten from an unreadable pool:\n%s", ctb.Spec.TrustBundle)
			}
		})
	}
}

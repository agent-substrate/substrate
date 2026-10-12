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

	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/localca"
	certsv1 "k8s.io/api/certificates/v1"
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
	ref := EgressMITMCAPoolRef(installdefaults.SystemNamespace)
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
	r := &EgressMITMTrustReconciler{Client: c, SystemNamespace: installdefaults.SystemNamespace, bundleV1: true}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: EgressMITMCAPoolRef(installdefaults.SystemNamespace)})
	return err
}

func getTrustBundle(t *testing.T, c client.Client) (*certsv1.ClusterTrustBundle, bool) {
	t.Helper()
	ctb := &certsv1.ClusterTrustBundle{}
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
	if err := c.Get(context.Background(), EgressMITMCAPoolRef(installdefaults.SystemNamespace), current); err != nil {
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
	foreign := &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: egressMITMTrustBundleName},
		Spec:       certsv1.ClusterTrustBundleSpec{SignerName: "someone.else.example/identity"},
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
			if err := c.Get(context.Background(), EgressMITMCAPoolRef(installdefaults.SystemNamespace), current); err != nil {
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

func TestEgressMITMTrustAPIVersions(t *testing.T) {
	for _, stable := range []bool{false, true} {
		version := "v1beta1"
		if stable {
			version = "v1"
		}
		t.Run(version, func(t *testing.T) {
			ctx := t.Context()
			secret, pool := caPoolSecret(t, "initial")
			c := fake.NewClientBuilder().WithScheme(egressMITMScheme(t)).WithObjects(secret).Build()
			r := &EgressMITMTrustReconciler{Client: c, SystemNamespace: installdefaults.SystemNamespace, bundleV1: stable}
			reconcile := func() {
				t.Helper()
				if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: EgressMITMCAPoolRef(r.SystemNamespace)}); err != nil {
					t.Fatal(err)
				}
			}
			read := func() *certsv1.ClusterTrustBundle {
				t.Helper()
				obj := r.bundleObject()
				if err := c.Get(ctx, types.NamespacedName{Name: egressMITMTrustBundleName}, obj); err != nil {
					t.Fatal(err)
				}
				if stable {
					return obj.(*certsv1.ClusterTrustBundle)
				}
				return clustertrustbundle.ToV1(obj.(*certsv1beta1.ClusterTrustBundle))
			}
			reconcile()
			bundle := read()
			if bundle.Spec.TrustBundle != rootPEM(t, pool) || bundle.Spec.SignerName != egressMITMSignerName || bundle.Labels["podcert.ate.dev/canarying"] != "live" {
				t.Fatalf("incorrect bundle: %#v", bundle)
			}
			rotated, nextPool := caPoolSecret(t, "rotated")
			if err := c.Get(ctx, client.ObjectKeyFromObject(secret), secret); err != nil {
				t.Fatal(err)
			}
			secret.Data = rotated.Data
			if err := c.Update(ctx, secret); err != nil {
				t.Fatal(err)
			}
			reconcile()
			if read().Spec.TrustBundle != rootPEM(t, nextPool) {
				t.Fatal("rotation was not applied")
			}
			if err := c.Delete(ctx, secret); err != nil {
				t.Fatal(err)
			}
			reconcile()
			if err := c.Get(ctx, types.NamespacedName{Name: egressMITMTrustBundleName}, r.bundleObject()); !k8errors.IsNotFound(err) {
				t.Fatalf("bundle survived pool deletion: %v", err)
			}
			foreign := &certsv1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Name: egressMITMTrustBundleName}, Spec: certsv1.ClusterTrustBundleSpec{SignerName: "other.example/signer"}}
			var obj client.Object = foreign
			if !stable {
				obj = clustertrustbundle.ToBeta(foreign)
			}
			if err := c.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: EgressMITMCAPoolRef(r.SystemNamespace)}); err == nil {
				t.Fatal("did not reject deletion of a foreign bundle")
			}
			if read().Spec.SignerName != "other.example/signer" {
				t.Fatal("foreign bundle was changed")
			}
		})
	}
}

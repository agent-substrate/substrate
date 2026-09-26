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

package controllers

import (
	"testing"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	certsv1 "k8s.io/api/certificates/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEgressMITMTrustV1PublishesAndDeletes(t *testing.T) {
	t.Parallel()
	secret, pool := caPoolSecret(t, "mitm")
	c := fake.NewClientBuilder().WithScheme(egressMITMScheme(t)).WithObjects(secret).Build()
	r := &EgressMITMTrustReconciler{Client: c, CTBv1: true, SystemNamespace: installdefaults.SystemNamespace}
	req := ctrl.Request{NamespacedName: EgressMITMCAPoolRef(installdefaults.SystemNamespace)}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	ctb := &certsv1.ClusterTrustBundle{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: egressMITMTrustBundleName}, ctb); err != nil {
		t.Fatal(err)
	}
	if ctb.Spec.SignerName != egressMITMSignerName || ctb.Spec.TrustBundle != rootPEM(t, pool) {
		t.Fatalf("published bundle = %+v", ctb.Spec)
	}
	if err := c.Delete(t.Context(), secret); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), types.NamespacedName{Name: egressMITMTrustBundleName}, ctb); !k8errors.IsNotFound(err) {
		t.Fatalf("bundle after pool deletion: %v", err)
	}
}

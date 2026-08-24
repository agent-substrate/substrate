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
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/internal/localca"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestReplaceEgressTrustPoolLeavesSharedSecret(t *testing.T) {
	ctx := context.Background()
	k8s := kubernetesfake.NewSimpleClientset()
	var trustBundle string
	k8s.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		secret := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret)
		pool, err := localca.Unmarshal(secret.Data[egressCAPoolSecretKey])
		if err != nil {
			return true, nil, err
		}
		trustBundle = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pool.CAs[0].RootCertificate.Raw}))
		return false, nil, nil
	})
	k8s.PrependReactor("get", "clustertrustbundles", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &certsv1beta1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: EgressTrustBundleObjectName},
			Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: trustBundle},
		}, nil
	})

	t.Run("creator exits", func(t *testing.T) {
		replaceEgressTrustPool(t, ctx, k8s, "test-egress-trust")
	})

	if _, err := k8s.CoreV1().Secrets(egressCAPoolNamespace).Get(ctx, egressCAPoolSecretName, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatal("ReplaceEgressTrustPool deleted the shared CA pool when its creating test exited")
		}
		t.Fatalf("reading shared CA pool after its creating test exited: %v", err)
	}
}

func TestEnsureEgressTrustBundleConcurrentCreatorsDoNotReplace(t *testing.T) {
	ctx := context.Background()
	k8s := kubernetesfake.NewSimpleClientset()
	k8s.PrependReactor("get", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(action.GetResource().GroupResource(), egressCAPoolSecretName)
	})
	k8s.PrependReactor("get", "clustertrustbundles", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &certsv1beta1.ClusterTrustBundle{
			ObjectMeta: metav1.ObjectMeta{Name: EgressTrustBundleObjectName},
			Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: "shared trust"},
		}, nil
	})

	t.Run("callers", func(t *testing.T) {
		for i := range 2 {
			t.Run(fmt.Sprintf("caller %d", i), func(t *testing.T) {
				t.Parallel()
				ensureEgressTrustBundle(t, ctx, k8s)
			})
		}
	})

	var creates, updates int
	for _, action := range k8s.Actions() {
		if action.GetResource().Resource != "secrets" {
			continue
		}
		switch action.GetVerb() {
		case "create":
			creates++
		case "update":
			updates++
		}
	}
	if creates != 2 {
		t.Fatalf("Secret create actions = %d, want 2 concurrent attempts", creates)
	}
	if updates != 0 {
		t.Fatalf("Secret update actions = %d, want 0 from EnsureEgressTrustBundle", updates)
	}
}

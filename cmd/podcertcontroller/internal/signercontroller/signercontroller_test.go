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

package signercontroller

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeSigner struct {
	bundles []*certsv1.ClusterTrustBundle
}

func (fakeSigner) SignerName() string { return "example.com/signer" }

func (f fakeSigner) DesiredClusterTrustBundles() ([]*certsv1.ClusterTrustBundle, error) {
	return f.bundles, nil
}

func (fakeSigner) MakeCert(context.Context, *certsv1beta1.PodCertificateRequest) error {
	return nil
}

type assignedHasher struct{}

func (assignedHasher) AssignedToThisReplica(context.Context, string) bool { return true }

func bundle(trust string, labels map[string]string) *certsv1.ClusterTrustBundle {
	return &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "example.com:signer:bundle", Labels: labels},
		Spec:       certsv1.ClusterTrustBundleSpec{SignerName: "example.com/signer", TrustBundle: trust},
	}
}

func TestEnsureBundles(t *testing.T) {
	want := bundle("root-a", map[string]string{"k": "v"})
	for _, tc := range []struct {
		name     string
		existing *certsv1.ClusterTrustBundle
		wantVerb string // the single write expected, or "" for none
	}{
		{name: "missing", wantVerb: "create"},
		{name: "matching", existing: bundle("root-a", map[string]string{"k": "v"})},
		{name: "stale bundle", existing: bundle("root-old", map[string]string{"k": "v"}), wantVerb: "update"},
		{name: "stale labels", existing: bundle("root-a", nil), wantVerb: "update"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			kc.Resources = []*metav1.APIResourceList{{
				GroupVersion: certsv1.SchemeGroupVersion.String(),
				APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}},
			}}
			client, err := clustertrustbundle.NewClient(kc, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if tc.existing != nil {
				if _, err := client.Create(ctx, tc.existing, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			kc.ClearActions()

			c := &Controller{trustBundles: client, hasher: assignedHasher{}, handler: fakeSigner{bundles: []*certsv1.ClusterTrustBundle{want}}}
			c.ensureBundles(ctx)

			var writes []string
			for _, a := range kc.Actions() {
				if v := a.GetVerb(); v == "create" || v == "update" {
					writes = append(writes, v)
				}
			}
			if tc.wantVerb == "" && len(writes) != 0 {
				t.Errorf("writes = %v, want none", writes)
			}
			if tc.wantVerb != "" && (len(writes) != 1 || writes[0] != tc.wantVerb) {
				t.Errorf("writes = %v, want [%s]", writes, tc.wantVerb)
			}

			got, err := client.Get(ctx, want.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Spec != want.Spec || got.Labels["k"] != "v" {
				t.Errorf("stored = %+v, want spec %+v with label k=v", got, want.Spec)
			}
		})
	}
}

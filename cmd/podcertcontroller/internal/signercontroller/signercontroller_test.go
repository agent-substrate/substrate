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
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type bundleSigner struct{ bundle string }

func (s *bundleSigner) SignerName() string { return "example.com/identity" }
func (s *bundleSigner) MakeCert(context.Context, *certsv1beta1.PodCertificateRequest) error {
	return nil
}
func (s *bundleSigner) DesiredClusterTrustBundles() ([]*certsv1beta1.ClusterTrustBundle, error) {
	return []*certsv1beta1.ClusterTrustBundle{{
		ObjectMeta: metav1.ObjectMeta{Name: "example"},
		Spec: certsv1beta1.ClusterTrustBundleSpec{
			SignerName: s.SignerName(), TrustBundle: s.bundle,
		},
	}}, nil
}

type assignedHasher struct{}

func (assignedHasher) AssignedToThisReplica(context.Context, string) bool { return true }

func TestEnsureBundlesUsesSelectedAPI(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			kc := fake.NewSimpleClientset()
			kc.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/" + version,
				APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
			ctbs, err := clustertrustbundle.New(kc)
			if err != nil {
				t.Fatal(err)
			}
			signer := &bundleSigner{bundle: "original"}
			c := &Controller{ctbs: ctbs, handler: signer, hasher: assignedHasher{}}
			c.ensureBundles(t.Context())
			signer.bundle = "rotated"
			c.ensureBundles(t.Context())
			got, err := ctbs.Get(t.Context(), "example")
			if err != nil || got.Spec.TrustBundle != signer.bundle {
				t.Fatalf("bundle after rotation = %v, %v", got, err)
			}
			for _, action := range kc.Actions() {
				if action.GetResource().Resource == "clustertrustbundles" && action.GetResource().Version != version {
					t.Errorf("%s used %s, want %s", action.GetVerb(), action.GetResource().Version, version)
				}
			}
		})
	}
}

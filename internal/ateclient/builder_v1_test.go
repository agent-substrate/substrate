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

package ateclient

import (
	"crypto/x509"
	"testing"

	certsv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestServerTLSConfigV1(t *testing.T) {
	ca := testCAPEM(t, "servicedns-ca")
	clientset := fake.NewSimpleClientset(&certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: "servicedns", Labels: map[string]string{"podcert.ate.dev/canarying": "live"}},
		Spec: certsv1.ClusterTrustBundleSpec{
			SignerName: serviceDNSSignerName, TrustBundle: string(ca),
		},
	})
	clientset.Resources = []*metav1.APIResourceList{{GroupVersion: "certificates.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}}}}
	cfg, err := serverTLSConfig(t.Context(), clientset)
	if err != nil {
		t.Fatal(err)
	}
	want := x509.NewCertPool()
	want.AppendCertsFromPEM(ca)
	if !cfg.RootCAs.Equal(want) {
		t.Fatal("v1 CA not trusted")
	}
}

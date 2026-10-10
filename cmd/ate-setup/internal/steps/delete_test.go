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

package steps

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	certsv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
)

// trustBundle returns a bundle for signer the way the API server requires it
// to be named: the signer name with "/" replaced by ":", then a suffix.
func trustBundle(signer, suffix string) *certsv1.ClusterTrustBundle {
	name := suffix
	if signer != "" {
		name = strings.ReplaceAll(signer, "/", ":") + ":" + suffix
	}
	return &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID("uid-" + name),
			Labels: map[string]string{"podcert.ate.dev/canarying": "live"},
		},
		Spec: certsv1.ClusterTrustBundleSpec{SignerName: signer, TrustBundle: "pem"},
	}
}

// trustBundleKube serves ClusterTrustBundles at version, seeded with bundles.
// An empty version serves no ClusterTrustBundle API at all.
func trustBundleKube(t *testing.T, version string, bundles ...*certsv1.ClusterTrustBundle) (*fake.Clientset, *clustertrustbundle.Client) {
	t.Helper()
	kc := fake.NewSimpleClientset()
	if version == "" {
		return kc, nil
	}
	kc.Resources = []*metav1.APIResourceList{{
		GroupVersion: "certificates.k8s.io/" + version,
		APIResources: []metav1.APIResource{{Name: "clustertrustbundles"}},
	}}
	c, err := clustertrustbundle.NewClient(kc, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range bundles {
		if _, err := c.Create(t.Context(), b, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	kc.ClearActions()
	return kc, c
}

func remainingBundles(t *testing.T, c *clustertrustbundle.Client) []string {
	t.Helper()
	list, err := c.List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, b := range list.Items {
		names = append(names, b.Name)
	}
	slices.Sort(names)
	return names
}

func TestDeleteTrustBundles(t *testing.T) {
	for _, version := range []string{"v1", "v1beta1"} {
		t.Run(version, func(t *testing.T) {
			var ours []*certsv1.ClusterTrustBundle
			for name, signer := range substrateTrustBundles {
				b := trustBundle(signer, "primary-bundle")
				if b.Name != name {
					t.Fatalf("test bundle for %s is named %s, want %s", signer, b.Name, name)
				}
				ours = append(ours, b)
			}
			// Others' bundles stay: ones under our signers with another
			// name, as a CA rotation or another publisher might create, one
			// that shares a domain suffix with ours, one that carries the
			// label our consumers select on, and one with no signer.
			others := []*certsv1.ClusterTrustBundle{
				trustBundle("podidentity.podcert.ate.dev/identity", "next-bundle"),
				trustBundle("egress-mitm.ate.dev/mitm", "foreign-bundle"),
				trustBundle("example.com/signer", "primary-bundle"),
				trustBundle("other.podcert.ate.dev/identity", "primary-bundle"),
				trustBundle("podidentity.podcert.ate.dev/other", "primary-bundle"),
				trustBundle("", "unsigned-bundle"),
			}
			kc, c := trustBundleKube(t, version, append(slices.Clone(ours), others...)...)
			e := &Env{Kube: &kube.Client{Typed: kc}}

			if err := e.DeleteTrustBundles(t.Context()); err != nil {
				t.Fatalf("DeleteTrustBundles() = %v", err)
			}
			var want []string
			for _, b := range others {
				want = append(want, b.Name)
			}
			slices.Sort(want)
			if got := remainingBundles(t, c); !slices.Equal(got, want) {
				t.Errorf("remaining bundles = %q, want %q", got, want)
			}

			// Each delete is pinned to the UID that was listed, and goes to
			// the served version.
			var deleted []string
			for _, a := range kc.Actions() {
				del, ok := a.(ktesting.DeleteActionImpl)
				if !ok {
					continue
				}
				if got := del.GetResource().Version; got != version {
					t.Errorf("delete of %s went to %s, want %s", del.Name, got, version)
				}
				pre := del.DeleteOptions.Preconditions
				if pre == nil || pre.UID == nil || *pre.UID != types.UID("uid-"+del.Name) {
					t.Errorf("delete of %s has preconditions %+v, want UID uid-%s", del.Name, pre, del.Name)
				}
				deleted = append(deleted, del.Name)
			}
			if len(deleted) != len(ours) {
				t.Errorf("deleted %q, want %d bundles", deleted, len(ours))
			}

			// A rerun finds nothing left of ours and changes nothing.
			kc.ClearActions()
			if err := e.DeleteTrustBundles(t.Context()); err != nil {
				t.Fatalf("second DeleteTrustBundles() = %v", err)
			}
			for _, a := range kc.Actions() {
				if a.GetVerb() == "delete" {
					t.Errorf("second run issued %v", a)
				}
			}
			if got := remainingBundles(t, c); !slices.Equal(got, want) {
				t.Errorf("after rerun, remaining bundles = %q, want %q", got, want)
			}
		})
	}
}

// A bundle with one of our names but another signer is not ours. The API
// server does not admit one, so this guards the check itself.
func TestDeleteTrustBundlesNameWithWrongSigner(t *testing.T) {
	foreign := trustBundle("example.com/signer", "primary-bundle")
	foreign.Name = "postgres.podcert.ate.dev:identity:primary-bundle"
	kc, c := trustBundleKube(t, "v1", foreign)
	e := &Env{Kube: &kube.Client{Typed: kc}}
	if err := e.DeleteTrustBundles(t.Context()); err != nil {
		t.Fatalf("DeleteTrustBundles() = %v", err)
	}
	if got := remainingBundles(t, c); !slices.Equal(got, []string{foreign.Name}) {
		t.Errorf("remaining bundles = %q, want %q", got, foreign.Name)
	}
}

func TestDeleteTrustBundlesNoneOfOurs(t *testing.T) {
	foreign := trustBundle("example.com/signer", "primary-bundle")
	kc, c := trustBundleKube(t, "v1", foreign)
	e := &Env{Kube: &kube.Client{Typed: kc}}
	if err := e.DeleteTrustBundles(t.Context()); err != nil {
		t.Fatalf("DeleteTrustBundles() = %v", err)
	}
	for _, a := range kc.Actions() {
		if a.GetVerb() == "delete" {
			t.Errorf("issued %v with none of ours present", a)
		}
	}
	if got := remainingBundles(t, c); !slices.Equal(got, []string{foreign.Name}) {
		t.Errorf("remaining bundles = %q, want %q", got, foreign.Name)
	}
}

// A cluster that serves no ClusterTrustBundle API cannot hold any of ours.
func TestDeleteTrustBundlesAPINotServed(t *testing.T) {
	kc, _ := trustBundleKube(t, "")
	e := &Env{Kube: &kube.Client{Typed: kc}}
	if err := e.DeleteTrustBundles(t.Context()); err != nil {
		t.Fatalf("DeleteTrustBundles() = %v, want nil", err)
	}
	for _, a := range kc.Actions() {
		if a.GetResource().Resource == "clustertrustbundles" {
			t.Errorf("issued %v without a ClusterTrustBundle API", a)
		}
	}
}

func TestDeleteTrustBundlesErrors(t *testing.T) {
	denied := apierrors.NewForbidden(schema.GroupResource{Group: "certificates.k8s.io", Resource: "clustertrustbundles"}, "", fmt.Errorf("denied"))
	for _, tc := range []struct {
		name    string
		verb    string
		err     error
		wantErr bool
	}{
		// Deleted by someone else between the list and the delete.
		{name: "already gone", verb: "delete", err: apierrors.NewNotFound(schema.GroupResource{}, ""), wantErr: false},
		{name: "delete denied", verb: "delete", err: denied, wantErr: true},
		{name: "list denied", verb: "list", err: denied, wantErr: true},
		{name: "discovery fails", verb: "get", err: apierrors.NewServiceUnavailable("unavailable"), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc, _ := trustBundleKube(t, "v1", trustBundle("podidentity.podcert.ate.dev/identity", "primary-bundle"))
			resource := "clustertrustbundles"
			if tc.verb == "get" {
				// Discovery goes through the fake's "get resource" action.
				resource = "resource"
			}
			kc.PrependReactor(tc.verb, resource, func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})
			e := &Env{Kube: &kube.Client{Typed: kc}}
			err := e.DeleteTrustBundles(t.Context())
			if (err != nil) != tc.wantErr {
				t.Fatalf("DeleteTrustBundles() = %v, want error %t", err, tc.wantErr)
			}
		})
	}
}

// Every bundle install waits for must be one teardown deletes.
func TestSubstrateTrustBundlesCoverInstalledBundles(t *testing.T) {
	for _, name := range trustBundleNames {
		if _, ok := substrateTrustBundles[name]; !ok {
			t.Errorf("teardown does not delete ClusterTrustBundle %s", name)
		}
	}
}

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
	"crypto"
	"encoding/json"
	"testing"

	"github.com/go-jose/go-jose/v4"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
)

func generateAuthority(t *testing.T, algorithm string) *localjwtauthority.Authority {
	t.Helper()
	authority, err := localjwtauthority.GenerateAuthority(algorithm, "")
	if err != nil {
		t.Fatalf("GenerateAuthority(%q): %v", algorithm, err)
	}
	return authority
}

// jwtPoolSecret marshals authorities into a pool Secret, the first one active.
func jwtPoolSecret(t *testing.T, authorities ...*localjwtauthority.Authority) *corev1.Secret {
	t.Helper()
	wire, err := localjwtauthority.Marshal(&localjwtauthority.ConcretePool{
		Authorities:      authorities,
		ActiveForSigning: authorities[0].ID,
	})
	if err != nil {
		t.Fatalf("marshal JWT pool: %v", err)
	}
	ref := ActorJWTPoolRef(installdefaults.SystemNamespace)
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, UID: types.UID("pool-uid")},
		Data:       map[string][]byte{"pool": wire},
	}
}

func reconcileJWTPool(t *testing.T, c client.Client) error {
	t.Helper()
	r := &ActorJWKSReconciler{Client: c, SystemNamespace: installdefaults.SystemNamespace}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: ActorJWTPoolRef(installdefaults.SystemNamespace)})
	return err
}

func getJWKSConfigMap(t *testing.T, c client.Client) (*corev1.ConfigMap, bool) {
	t.Helper()
	cm := &corev1.ConfigMap{}
	err := c.Get(context.Background(), ActorJWKSRef(installdefaults.SystemNamespace), cm)
	if k8errors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("get ConfigMap: %v", err)
	}
	return cm, true
}

// publishedKeys parses the ConfigMap's JWK set and indexes it by key ID.
func publishedKeys(t *testing.T, cm *corev1.ConfigMap) map[string]jose.JSONWebKey {
	t.Helper()
	var set jose.JSONWebKeySet
	if err := json.Unmarshal([]byte(cm.Data[actorJWKSKey]), &set); err != nil {
		t.Fatalf("parse %s: %v\n%s", actorJWKSKey, err, cm.Data[actorJWKSKey])
	}
	keys := map[string]jose.JSONWebKey{}
	for _, k := range set.Keys {
		keys[k.KeyID] = k
	}
	return keys
}

func wantPublished(t *testing.T, cm *corev1.ConfigMap, authorities ...*localjwtauthority.Authority) {
	t.Helper()
	keys := publishedKeys(t, cm)
	if len(keys) != len(authorities) {
		t.Errorf("published %d keys, want %d", len(keys), len(authorities))
	}
	for _, a := range authorities {
		k, ok := keys[a.ID]
		if !ok {
			t.Errorf("key %q was not published", a.ID)
			continue
		}
		if !k.IsPublic() {
			t.Errorf("key %q was published with its private key", a.ID)
		}
		if k.Algorithm != a.Algorithm {
			t.Errorf("key %q alg = %q, want %q", a.ID, k.Algorithm, a.Algorithm)
		}
		if pub, ok := a.SigningKey.Public().(interface{ Equal(crypto.PublicKey) bool }); !ok || !pub.Equal(k.Key) {
			t.Errorf("key %q does not match the authority's public key", a.ID)
		}
	}
}

func TestActorJWKSPublishesEveryAuthority(t *testing.T) {
	t.Parallel()
	active, inactive := generateAuthority(t, "ES256"), generateAuthority(t, "RS256")
	secret := jwtPoolSecret(t, active, inactive)
	c := fake.NewClientBuilder().WithObjects(secret).Build()

	if err := reconcileJWTPool(t, c); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	cm, ok := getJWKSConfigMap(t, c)
	if !ok {
		t.Fatal("no ConfigMap was created")
	}
	wantPublished(t, cm, active, inactive)

	owners := cm.GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("ownerReferences = %+v, want the pool Secret only", owners)
	}
	if o := owners[0]; o.Kind != "Secret" || o.Name != secret.Name || o.UID != secret.UID || o.Controller == nil || !*o.Controller {
		t.Errorf("ownerReference = %+v, want controller reference to Secret %s (%s)", o, secret.Name, secret.UID)
	}
}

func TestActorJWKSFollowsPoolRotation(t *testing.T) {
	t.Parallel()
	old := generateAuthority(t, "ES256")
	c := fake.NewClientBuilder().WithObjects(jwtPoolSecret(t, old)).Build()
	if err := reconcileJWTPool(t, c); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}

	next := generateAuthority(t, "ES256")
	current := &corev1.Secret{}
	if err := c.Get(context.Background(), ActorJWTPoolRef(installdefaults.SystemNamespace), current); err != nil {
		t.Fatalf("get pool secret: %v", err)
	}
	current.Data = jwtPoolSecret(t, next, old).Data
	if err := c.Update(context.Background(), current); err != nil {
		t.Fatalf("update pool secret: %v", err)
	}

	if err := reconcileJWTPool(t, c); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	cm, ok := getJWKSConfigMap(t, c)
	if !ok {
		t.Fatal("the ConfigMap disappeared")
	}
	wantPublished(t, cm, next, old)
}

func TestActorJWKSRevertsChangedConfigMap(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(context.Context, client.Client, *corev1.ConfigMap) error
	}{
		{name: "deleted", change: func(ctx context.Context, c client.Client, cm *corev1.ConfigMap) error {
			return c.Delete(ctx, cm)
		}},
		{name: "edited", change: func(ctx context.Context, c client.Client, cm *corev1.ConfigMap) error {
			cm.Data[actorJWKSKey] = `{"keys":[]}`
			return c.Update(ctx, cm)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			authority := generateAuthority(t, "ES256")
			c := fake.NewClientBuilder().WithObjects(jwtPoolSecret(t, authority)).Build()
			if err := reconcileJWTPool(t, c); err != nil {
				t.Fatalf("first Reconcile: %v", err)
			}
			cm, ok := getJWKSConfigMap(t, c)
			if !ok {
				t.Fatal("no ConfigMap was created")
			}
			if err := tc.change(context.Background(), c, cm); err != nil {
				t.Fatalf("change ConfigMap: %v", err)
			}

			if err := reconcileJWTPool(t, c); err != nil {
				t.Fatalf("second Reconcile: %v", err)
			}
			cm, ok = getJWKSConfigMap(t, c)
			if !ok {
				t.Fatal("the ConfigMap was not recreated")
			}
			wantPublished(t, cm, authority)
		})
	}
}

func TestActorJWKSWithoutPool(t *testing.T) {
	t.Parallel()
	c := fake.NewClientBuilder().Build()

	if err := reconcileJWTPool(t, c); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := getJWKSConfigMap(t, c); ok {
		t.Error("a ConfigMap was created without a pool")
	}
}

// ate-idp-server serves the ConfigMap as is, and relying parties cache what it
// serves, so an unreadable pool must leave the last good key set in place.
func TestActorJWKSKeepsLastGoodKeySetOnBadPool(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		data map[string][]byte
	}{
		{name: "missing key", data: map[string][]byte{"not-pool": []byte("{}")}},
		{name: "unparseable", data: map[string][]byte{"pool": []byte("not json")}},
		{name: "no authorities", data: map[string][]byte{"pool": []byte(`{"Authorities":[]}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			authority := generateAuthority(t, "ES256")
			c := fake.NewClientBuilder().WithObjects(jwtPoolSecret(t, authority)).Build()
			if err := reconcileJWTPool(t, c); err != nil {
				t.Fatalf("first Reconcile: %v", err)
			}

			current := &corev1.Secret{}
			if err := c.Get(context.Background(), ActorJWTPoolRef(installdefaults.SystemNamespace), current); err != nil {
				t.Fatalf("get pool secret: %v", err)
			}
			current.Data = tc.data
			if err := c.Update(context.Background(), current); err != nil {
				t.Fatalf("update pool secret: %v", err)
			}

			if err := reconcileJWTPool(t, c); err == nil {
				t.Error("Reconcile accepted an unreadable pool; it should fail and requeue")
			}
			cm, ok := getJWKSConfigMap(t, c)
			if !ok {
				t.Fatal("the ConfigMap was removed by an unreadable pool")
			}
			wantPublished(t, cm, authority)
		})
	}
}

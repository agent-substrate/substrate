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

package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/agent-substrate/substrate/internal/localjwtauthority"
)

func TestNewJWTPoolSecretWithFlagDefaults(t *testing.T) {
	alg := makeJwtPoolCmd.Flags().Lookup("alg").DefValue
	keyID := makeJwtPoolCmd.Flags().Lookup("key-id").DefValue

	secret, gotKeyID, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", alg, keyID)
	if err != nil {
		t.Fatal(err)
	}
	if secret.Namespace != "ate-system" || secret.Name != "actor-id-jwt-pool" {
		t.Errorf("secret = %s/%s, want ate-system/actor-id-jwt-pool", secret.Namespace, secret.Name)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.Authorities) != 1 {
		t.Fatalf("pool has %d authorities, want 1", len(pool.Authorities))
	}
	authority := pool.Authorities[0]
	if authority.Algorithm != "ES256" {
		t.Errorf("Algorithm = %q, want ES256", authority.Algorithm)
	}
	thumbprint, err := localjwtauthority.Thumbprint(authority.SigningKey.Public())
	if err != nil {
		t.Fatal(err)
	}
	if authority.ID != thumbprint || gotKeyID != thumbprint || pool.ActiveForSigning != thumbprint {
		t.Errorf("key ID %q, returned %q, active %q; want all to be the thumbprint %q", authority.ID, gotKeyID, pool.ActiveForSigning, thumbprint)
	}
}

func TestNewJWTPoolSecretExplicitKey(t *testing.T) {
	secret, keyID, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "RS256", "1")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatal(err)
	}
	if keyID != "1" || pool.ActiveForSigning != "1" || pool.Authorities[0].Algorithm != "RS256" {
		t.Errorf("got key %q, active %q, algorithm %q; want 1, 1, RS256", keyID, pool.ActiveForSigning, pool.Authorities[0].Algorithm)
	}
}

func TestNewJWTPoolSecretRejectsUnsupportedAlgorithm(t *testing.T) {
	if _, _, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "HS256", ""); err == nil {
		t.Error("newJWTPoolSecret(HS256) returned nil error")
	}
}

// poolState reads back the key IDs and active key of the pool secret.
func poolState(t *testing.T, kc *fake.Clientset) ([]string, string) {
	t.Helper()
	secret, err := kc.CoreV1().Secrets("ate-system").Get(context.Background(), "actor-id-jwt-pool", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, a := range pool.Authorities {
		ids = append(ids, a.ID)
	}
	return ids, pool.ActiveForSigning
}

func newPoolClientset(t *testing.T) *fake.Clientset {
	t.Helper()
	secret, _, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "ES256", "old")
	if err != nil {
		t.Fatal(err)
	}
	return fake.NewClientset(secret)
}

func TestUpdateJWTPoolRotation(t *testing.T) {
	kc := newPoolClientset(t)
	secrets := kc.CoreV1().Secrets("ate-system")
	ctx := context.Background()
	next, err := localjwtauthority.GenerateAuthority("RS256", "next")
	if err != nil {
		t.Fatal(err)
	}

	for _, step := range []struct {
		name       string
		change     func(*localjwtauthority.ConcretePool) error
		wantIDs    []string
		wantActive string
	}{
		{"add", func(p *localjwtauthority.ConcretePool) error { return p.AddAuthority(next) }, []string{"old", "next"}, "old"},
		{"activate", func(p *localjwtauthority.ConcretePool) error { return p.Activate("next") }, []string{"old", "next"}, "next"},
		{"remove", func(p *localjwtauthority.ConcretePool) error { return p.RemoveAuthority("old") }, []string{"next"}, "next"},
	} {
		if err := updateJWTPool(ctx, secrets, "actor-id-jwt-pool", step.change); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		ids, active := poolState(t, kc)
		if diff := cmp.Diff(step.wantIDs, ids); diff != "" || active != step.wantActive {
			t.Errorf("after %s: active %q, want %q; key IDs (-want +got):\n%s", step.name, active, step.wantActive, diff)
		}
	}
}

func TestUpdateJWTPoolRetriesConflict(t *testing.T) {
	kc := newPoolClientset(t)
	conflicted := false
	kc.PrependReactor("update", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicted {
			return false, nil, nil
		}
		conflicted = true
		// Another writer added a key between our read and write.
		secret, _, err := newJWTPoolSecret("ate-system", "actor-id-jwt-pool", "ES256", "old")
		if err != nil {
			t.Fatal(err)
		}
		pool, err := localjwtauthority.Unmarshal(secret.Data["pool"])
		if err != nil {
			t.Fatal(err)
		}
		other, err := localjwtauthority.GenerateAuthority("ES256", "other")
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.AddAuthority(other); err != nil {
			t.Fatal(err)
		}
		if secret.Data["pool"], err = localjwtauthority.Marshal(pool); err != nil {
			t.Fatal(err)
		}
		if err := kc.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, secret, "ate-system"); err != nil {
			t.Fatal(err)
		}
		return true, nil, k8errors.NewConflict(schema.GroupResource{Resource: "secrets"}, "actor-id-jwt-pool", nil)
	})

	next, err := localjwtauthority.GenerateAuthority("ES256", "next")
	if err != nil {
		t.Fatal(err)
	}
	if err := updateJWTPool(context.Background(), kc.CoreV1().Secrets("ate-system"), "actor-id-jwt-pool", func(p *localjwtauthority.ConcretePool) error {
		return p.AddAuthority(next)
	}); err != nil {
		t.Fatalf("updateJWTPool: %v", err)
	}
	ids, _ := poolState(t, kc)
	if diff := cmp.Diff([]string{"old", "other", "next"}, ids); diff != "" {
		t.Errorf("key IDs (-want +got):\n%s", diff)
	}
}

func TestUpdateJWTPoolLeavesSecretOnError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		secret *corev1.Secret
	}{
		{name: "change refused"},
		{name: "no pool key", secret: &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ate-system", Name: "actor-id-jwt-pool"},
			Data:       map[string][]byte{"other": []byte("x")},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kc := newPoolClientset(t)
			if tc.secret != nil {
				kc = fake.NewClientset(tc.secret)
			}
			err := updateJWTPool(context.Background(), kc.CoreV1().Secrets("ate-system"), "actor-id-jwt-pool", func(p *localjwtauthority.ConcretePool) error {
				return p.RemoveAuthority("old")
			})
			if err == nil {
				t.Error("got nil error")
			}
			for _, action := range kc.Actions() {
				if action.GetVerb() == "update" {
					t.Errorf("secret was written: %v", action)
				}
			}
		})
	}
}

func TestUpdateJWTPoolMissingSecret(t *testing.T) {
	kc := fake.NewClientset()
	err := updateJWTPool(context.Background(), kc.CoreV1().Secrets("ate-system"), "actor-id-jwt-pool", func(*localjwtauthority.ConcretePool) error {
		t.Error("change ran without a pool")
		return nil
	})
	if !k8errors.IsNotFound(err) {
		t.Errorf("err = %v, want NotFound", err)
	}
}

func TestListJWTKeys(t *testing.T) {
	kc := newPoolClientset(t)
	secrets := kc.CoreV1().Secrets("ate-system")
	next, err := localjwtauthority.GenerateAuthority("RS256", "next")
	if err != nil {
		t.Fatal(err)
	}
	if err := updateJWTPool(context.Background(), secrets, "actor-id-jwt-pool", func(p *localjwtauthority.ConcretePool) error {
		return p.AddAuthority(next)
	}); err != nil {
		t.Fatal(err)
	}

	_, pool, err := getJWTPool(context.Background(), secrets, "actor-id-jwt-pool")
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	if err := printJWTKeys(&out, pool); err != nil {
		t.Fatal(err)
	}
	want := "" +
		"ACTIVE   KEY ID   ALGORITHM\n" +
		"*        old      ES256\n" +
		"         next     RS256\n"
	if diff := cmp.Diff(want, out.String()); diff != "" {
		t.Errorf("output (-want +got):\n%s", diff)
	}
}

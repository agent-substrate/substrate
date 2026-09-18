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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
)

func TestUseBundledPostgres(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connString string
		want       bool
	}{
		{name: "no external database", connString: "", want: true},
		{
			name:       "external database configured",
			connString: "postgresql://user@db.example.com:5432/atepg",
			want:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{PostgresConnectionString: tc.connString}}
			if got := e.useBundledPostgres(); got != tc.want {
				t.Errorf("useBundledPostgres() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPostgresConnectionStrings(t *testing.T) {
	t.Run("external DDL defaults to runtime", func(t *testing.T) {
		const dsn = "postgresql://runtime@db.example/atepg"
		e := &Env{Cfg: &config.Config{PostgresConnectionString: dsn}}
		runtimeDSN, ddlDSN, err := e.postgresConnectionStrings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if runtimeDSN != dsn || ddlDSN != dsn {
			t.Fatalf("connection strings = %q, %q; want %q twice", runtimeDSN, ddlDSN, dsn)
		}
	})

	t.Run("bundled credentials are reused", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: SecretPostgresRoles, Namespace: NamespaceAteSystem},
			Data: map[string][]byte{
				"runtime-password": []byte("runtime-secret"),
				"ddl-password":     []byte("ddl-secret"),
			},
		}
		e := &Env{
			Cfg:  &config.Config{},
			Kube: &kube.Client{Typed: fake.NewSimpleClientset(secret)},
		}
		runtimeDSN, ddlDSN, err := e.postgresConnectionStrings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(runtimeDSN, "ateapi_runtime:runtime-secret") || !strings.Contains(ddlDSN, "ateapi_ddl:ddl-secret") || runtimeDSN == ddlDSN {
			t.Fatalf("unexpected bundled connection strings: %q, %q", runtimeDSN, ddlDSN)
		}
		if !strings.Contains(runtimeDSN, "channel_binding=disable") || !strings.Contains(ddlDSN, "channel_binding=disable") {
			t.Fatalf("bundled connection strings do not disable unsupported channel binding: %q, %q", runtimeDSN, ddlDSN)
		}
		runtimeAgain, ddlAgain, err := e.postgresConnectionStrings(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if runtimeAgain != runtimeDSN || ddlAgain != ddlDSN {
			t.Fatal("bundled PostgreSQL credentials changed on the second read")
		}
	})
}

// The StatefulSet lives in a subdirectory that the bundle render does not
// descend into, so DeployAteSystem has to apply it by name. A rename that
// misses Manifest("postgres", "postgres.yaml") would otherwise only surface
// as a failed GKE install.
func TestBundledPostgresManifestExists(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	path := cfg.Manifest("postgres", "postgres.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%s) = %v, want the bundled PostgreSQL manifest", path, err)
	}
	// It must not also sit at the top level, where `ko resolve -f
	// manifests/ate-install` would apply it regardless of the skip.
	stray := filepath.Join(cfg.Root, "manifests", "ate-install", "postgres.yaml")
	if _, err := os.Stat(stray); err == nil {
		t.Errorf("%s exists; the bundle render would apply it even for external databases", stray)
	}
}

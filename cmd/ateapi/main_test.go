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

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConnectStoreRequiresPostgresReadWriteConnectionString(t *testing.T) {
	oldDSN := *postgresReadWriteConnectionString
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldDSN
	})
	*postgresReadWriteConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-read-write-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestLoadFlagsFromEnvEnablesAuthz(t *testing.T) {
	old := *experimentalEnableAuthz
	t.Cleanup(func() { *experimentalEnableAuthz = old })
	*experimentalEnableAuthz = false
	t.Setenv("ATE_API_EXPERIMENTAL_ENABLE_AUTHZ", "true")

	loadFlagsFromEnv()
	if !*experimentalEnableAuthz {
		t.Fatal("ATE_API_EXPERIMENTAL_ENABLE_AUTHZ=true did not enable authorization")
	}
}

// The connection strings are flags, visible in the pod spec and the process
// table, so a password has to come from a passfile. Startup refuses one inline
// before it dials anything.
func TestConnectStoreRejectsInlineSecrets(t *testing.T) {
	oldReadWrite, oldOwner := *postgresReadWriteConnectionString, *postgresOwnerConnectionString
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = oldReadWrite
		*postgresOwnerConnectionString = oldOwner
	})
	const clean = "postgresql://runtime@db.example.internal/atepg?passfile=/run/pg/pgpass"
	for _, tc := range []struct {
		name, readWrite, owner, wantFlag string
	}{
		{"read-write password", "postgresql://runtime:s3cret@db.example.internal/atepg", clean, "--postgres-read-write-connection-string"},
		{"owner password", clean, "host=db.example.internal user=owner password=s3cret", "--postgres-owner-connection-string"},
		{"key passphrase", "host=db.example.internal sslkey=/k.pem sslpassword=s3cret", clean, "--postgres-read-write-connection-string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*postgresReadWriteConnectionString = tc.readWrite
			*postgresOwnerConnectionString = tc.owner
			_, err := connectStore(t.Context())
			if err == nil || !strings.Contains(err.Error(), tc.wantFlag) || !strings.Contains(err.Error(), "passfile") {
				t.Fatalf("connectStore() error = %v, want %s rejected", err, tc.wantFlag)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("connectStore() error %q echoes the secret", err)
			}
		})
	}
}

func TestResolveActorJWTIssuer(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		namespace string
		want      string
		wantErr   bool
	}{
		{name: "unset uses the namespace's idp Service", namespace: "ate-system", want: "https://idp.ate-system.svc"},
		{name: "unset in a relocated install", namespace: "team-a", want: "https://idp.team-a.svc"},
		{name: "set is used as given", flagValue: "https://idp.example.com/prod/", namespace: "ate-system", want: "https://idp.example.com/prod/"},
		{name: "set but invalid", flagValue: "http://idp.example.com", namespace: "ate-system", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveActorJWTIssuer(tt.flagValue, tt.namespace)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveActorJWTIssuer(%q, %q) = %q, want error", tt.flagValue, tt.namespace, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorJWTIssuer(%q, %q) returned error: %v", tt.flagValue, tt.namespace, err)
			}
			if got != tt.want {
				t.Errorf("resolveActorJWTIssuer(%q, %q) = %q, want %q", tt.flagValue, tt.namespace, got, tt.want)
			}
		})
	}
}

func TestPostgresConnectionAttrNeverLogsThePassword(t *testing.T) {
	const password = "hunter2-very-secret"
	render := func(connString string) string {
		var buf bytes.Buffer
		slog.New(slog.NewJSONHandler(&buf, nil)).LogAttrs(context.Background(), slog.LevelInfo, "Final flag values", postgresConnectionAttr("postgres-connection-string", connString))
		return buf.String()
	}

	for name, connString := range map[string]string{
		"uri":   "postgresql://ateapi:" + password + "@db.example.internal:5433/atepg?sslmode=disable",
		"libpq": "host=db.example.internal port=5433 user=ateapi password=" + password + " dbname=atepg sslmode=disable",
		// A password with URI-reserved characters is percent-encoded in the
		// string; neither the encoded nor the decoded form may appear.
		"uri_percent_encoded": "postgresql://ateapi:" + url.QueryEscape(password+"/#@:?") + "@db.example.internal:5433/atepg?sslmode=disable",
		"libpq_quoted":        "host=db.example.internal port=5433 user=ateapi password='" + password + " with space' dbname=atepg sslmode=disable",
	} {
		t.Run(name, func(t *testing.T) {
			got := render(connString)
			if strings.Contains(got, password) {
				t.Fatalf("log line contains the password: %s", got)
			}
			for _, want := range []string{`"host":"db.example.internal"`, `"port":5433`, `"database":"atepg"`, `"user":"ateapi"`, `"password-set":true`, `"tls":false`} {
				if !strings.Contains(got, want) {
					t.Errorf("log line missing %s: %s", want, got)
				}
			}
		})
	}

	t.Run("tls", func(t *testing.T) {
		got := render("postgresql://ateapi:" + password + "@db.example.internal:5432/atepg?sslmode=require")
		if strings.Contains(got, password) || !strings.Contains(got, `"tls":true`) {
			t.Errorf("expected tls true and no password: %s", got)
		}
	})

	t.Run("passwordless", func(t *testing.T) {
		got := render("postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=disable")
		if !strings.Contains(got, `"password-set":false`) {
			t.Errorf("expected password-set false: %s", got)
		}
	})

	t.Run("unparseable", func(t *testing.T) {
		raw := "postgresql://ateapi:" + password + "@[broken"
		got := render(raw)
		if strings.Contains(got, password) || strings.Contains(got, raw) {
			t.Fatalf("log line echoes an unparseable connection string: %s", got)
		}
		if !strings.Contains(got, `"postgres-connection-string":"<invalid pg connection string>"`) {
			t.Errorf("expected the unparseable marker: %s", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		if got := render(""); !strings.Contains(got, `"postgres-connection-string":""`) {
			t.Errorf("expected empty marker: %s", got)
		}
	})
}

// TestLogFlagValuesDoesNotLogThePostgresPassword goes through the real startup
// log call, so a change at the call site (logging the flag directly again)
// fails here even if the helper stays correct.
func TestLogFlagValuesDoesNotLogThePostgresPassword(t *testing.T) {
	const password = "hunter2-very-secret"
	origReadWrite, origOwner := *postgresReadWriteConnectionString, *postgresOwnerConnectionString
	t.Cleanup(func() {
		*postgresReadWriteConnectionString = origReadWrite
		*postgresOwnerConnectionString = origOwner
	})
	*postgresReadWriteConnectionString = "postgresql://runtime:" + password + "@db.example.internal:5432/atepg?sslmode=disable"
	*postgresOwnerConnectionString = "postgresql://owner:" + password + "@db.example.internal:5432/atepg?sslmode=disable"

	var buf bytes.Buffer
	origLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(origLogger) })
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))

	logFlagValues(context.Background())

	got := buf.String()
	if !strings.Contains(got, `"msg":"Final flag values"`) {
		t.Fatalf("startup line not emitted: %s", got)
	}
	if strings.Contains(got, password) {
		t.Fatalf("startup line contains the database password: %s", got)
	}
	for _, key := range []string{"postgres-read-write-connection-string", "postgres-owner-connection-string"} {
		if !strings.Contains(got, `"`+key+`":{"host":"db.example.internal"`) {
			t.Errorf("startup line missing the structured %s summary: %s", key, got)
		}
	}
}

func TestBuildServerTLSConfigWithoutCACertsAllowsCertlessClients(t *testing.T) {
	cfg, err := buildServerTLSConfig(context.Background(), "/nonexistent-cred-bundle.pem", "")
	if err != nil {
		t.Fatalf("buildServerTLSConfig() error = %v", err)
	}
	if cfg.GetConfigForClient != nil {
		t.Fatalf("buildServerTLSConfig() with no CA path set GetConfigForClient, want nil (no client-cert verification configured)")
	}
}

func TestBuildServerTLSConfigRejectsUnreadableCACerts(t *testing.T) {
	_, err := buildServerTLSConfig(context.Background(), "/nonexistent-cred-bundle.pem", filepath.Join(t.TempDir(), "absent.pem"))
	if err == nil {
		t.Fatalf("buildServerTLSConfig() error = nil, want an error for a missing CA file")
	}
}

// TestBuildServerTLSConfigReloadsCACertsWithoutRestart verifies that a
// pod-identity CA rotation on disk is picked up by the next handshake, not
// frozen at the config's construction.
func TestBuildServerTLSConfigReloadsCACertsWithoutRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trust-bundle.pem")
	writeCA(t, path, "ca-one")

	cfg, err := buildServerTLSConfig(context.Background(), "/nonexistent-cred-bundle.pem", path)
	if err != nil {
		t.Fatalf("buildServerTLSConfig() error = %v", err)
	}
	if cfg.GetConfigForClient == nil {
		t.Fatalf("buildServerTLSConfig() with a CA path did not set GetConfigForClient")
	}

	before, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() first call error = %v", err)
	}

	writeCA(t, path, "ca-two")

	after, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() second call error = %v", err)
	}

	if before.ClientCAs.Equal(after.ClientCAs) {
		t.Fatalf("GetConfigForClient() returned the same trust pool after the CA file changed, want the rotated one")
	}
}

// writeCA writes a fresh self-signed certificate (distinguished by cn) to
// path, suitable for AppendCertsFromPEM.
func writeCA(t *testing.T, path, cn string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate() error = %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func TestRejectStorageEnv(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value *string // nil leaves the variable unset
		want  bool    // an error naming the sidecar
	}{
		{name: "unset", value: nil, want: false},
		{name: "s3", value: ptr("s3"), want: true},
		// The old manifests set the default explicitly, so a stale patch
		// can carry it.
		{name: "gcs", value: ptr("gcs"), want: true},
		{name: "empty", value: ptr(""), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// t.Setenv restores the variable after the subtest, which also
			// undoes the Unsetenv.
			t.Setenv("ATE_STORAGE_BACKEND", "")
			os.Unsetenv("ATE_STORAGE_BACKEND")
			if tc.value != nil {
				os.Setenv("ATE_STORAGE_BACKEND", *tc.value)
			}
			err := rejectStorageEnv()
			if got := err != nil && strings.Contains(err.Error(), "snapshot-plugin sidecar"); got != tc.want {
				t.Errorf("rejectStorageEnv() = %v, want error naming the sidecar: %t", err, tc.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

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

package atepg

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/pkg/postgressetup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"sigs.k8s.io/yaml"
)

func TestBundledPostgresCertificateAuthentication(t *testing.T) {
	// Reuse the package's Docker requirement, including its fail-closed CI guard.
	requirePool(t)
	dir := t.TempDir()
	serverCA, err := localca.GenerateCA("service-dns", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	clientCA, err := localca.GenerateCA("postgres", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	otherCA, err := localca.GenerateCA("pod-identity", localca.KeyTypeED25519, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, serverKey := writeTestCertificate(t, dir, "server", serverCA, "postgres", true)
	runtimeCert, runtimeKey := writeTestCertificate(t, dir, "runtime", clientCA, postgressetup.ReadWriteUser, false)
	ownerCert, ownerKey := writeTestCertificate(t, dir, "owner", clientCA, postgressetup.OwnerUser, false)
	adminCert, adminKey := writeTestCertificate(t, dir, "admin", clientCA, "postgres", false)
	wrongCert, wrongKey := writeTestCertificate(t, dir, "wrong-cn", clientCA, "other", false)
	otherCert, otherKey := writeTestCertificate(t, dir, "other-ca", otherCA, postgressetup.ReadWriteUser, false)
	serverRoot := filepath.Join(dir, "server-root.pem")
	clientRoot := filepath.Join(dir, "client-root.pem")
	writeCertTestFile(t, serverRoot, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCA.RootCertificate.Raw}))
	writeCertTestFile(t, clientRoot, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCA.RootCertificate.Raw}))

	// Exercise the shipped HBA rules and TLS configuration, adapting only paths.
	manifest, err := os.ReadFile("../../../../../manifests/ate-install/postgres/postgres.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var cm struct {
		Data map[string]string `json:"data"`
	}
	if err := yaml.Unmarshal([]byte(strings.Split(string(manifest), "\n---")[0]), &cm); err != nil {
		t.Fatal(err)
	}
	conf := strings.NewReplacer(
		"ssl_cert_file = '/run/servicedns.podcert.ate.dev/credential-bundle.pem'", "ssl_cert_file = '/tmp/testcontainers-go/postgres/server.cert'",
		"ssl_key_file = '/run/servicedns.podcert.ate.dev/credential-bundle.pem'", "ssl_key_file = '/tmp/testcontainers-go/postgres/server.key'",
		"/run/postgres.podcert.ate.dev/trust-bundle.pem", "/tmp/testcontainers-go/postgres/ca_cert.pem",
		"/etc/postgresql/pg_hba.conf", "/tmp/testcontainers-go/postgres/pg_hba.conf",
	).Replace(cm.Data["postgresql.conf"])
	configPath := filepath.Join(dir, "postgresql.conf")
	writeCertTestFile(t, configPath, []byte(conf))
	container, err := postgres.Run(t.Context(), "postgres:18-alpine",
		postgres.WithDatabase("atepg"), postgres.WithUsername("postgres"), postgres.WithPassword("unused"),
		postgres.WithSSLCert(clientRoot, serverCert, serverKey), postgres.WithConfigFile(configPath),
		testcontainers.WithFiles(testcontainers.ContainerFile{Reader: strings.NewReader(cm.Data["pg_hba.conf"]), ContainerFilePath: "/tmp/testcontainers-go/postgres/pg_hba.conf", FileMode: 0644}),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	// Run the same bootstrap as ate-setup, then prove it creates passwordless logins.
	script := postgressetup.Script() + fmt.Sprintf(`
 DO $verify$
 BEGIN
  IF EXISTS (SELECT 1 FROM pg_authid WHERE rolname IN ('%s', '%s') AND rolpassword IS NOT NULL) THEN
   RAISE EXCEPTION 'bundled logins have passwords';
  END IF;
 END $verify$;`, postgressetup.OwnerUser, postgressetup.ReadWriteUser)
	const scriptPath = "/tmp/substrate-cert-auth-setup.sql"
	if err := container.CopyToContainer(t.Context(), []byte(script), scriptPath, 0600); err != nil {
		t.Fatal(err)
	}
	command := []string{"psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--username", "postgres", "--dbname", "atepg"}
	command = append(command, postgressetup.DefaultConfig().PSQLArgs()...)
	command = append(command, "--file="+scriptPath)
	code, output, err := container.Exec(t.Context(), command, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	detail, err := io.ReadAll(output)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("bootstrap exited %d: %s", code, detail)
	}
	dsn, err := container.ConnectionString(t.Context(), "sslmode=verify-full", "sslrootcert="+serverRoot)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	certificateDSN := func(user, cert, key, mode string) string {
		u := *parsed
		u.User = url.User(user) // Authentication must succeed without a password.
		query := u.Query()
		if cert != "" {
			query.Set("sslcert", cert)
			query.Set("sslkey", key)
		}
		if mode != "" {
			query.Set("sslmode", mode)
		}
		u.RawQuery = query.Encode()
		return u.String()
	}
	for _, tc := range []struct {
		name, user, cert, key, mode string
		allowed                     bool
	}{
		{name: "runtime login", user: postgressetup.ReadWriteUser, cert: runtimeCert, key: runtimeKey, allowed: true},
		{name: "owner login", user: postgressetup.OwnerUser, cert: ownerCert, key: ownerKey, allowed: true},
		{name: "runtime certificate as owner", user: postgressetup.OwnerUser, cert: runtimeCert, key: runtimeKey},
		{name: "owner certificate as runtime", user: postgressetup.ReadWriteUser, cert: ownerCert, key: ownerKey},
		{name: "wrong CN", user: postgressetup.ReadWriteUser, cert: wrongCert, key: wrongKey},
		{name: "other signer CA", user: postgressetup.ReadWriteUser, cert: otherCert, key: otherKey},
		{name: "administrator login", user: "postgres", cert: adminCert, key: adminKey},
		{name: "no certificate", user: postgressetup.ReadWriteUser},
		{name: "plaintext", user: postgressetup.ReadWriteUser, mode: "disable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			conn, err := pgx.Connect(ctx, certificateDSN(tc.user, tc.cert, tc.key, tc.mode))
			if conn != nil {
				defer conn.Close(ctx)
			}
			if (err == nil) != tc.allowed {
				t.Fatalf("connection error = %v, allowed = %v", err, tc.allowed)
			}
			if tc.allowed {
				var user string
				if err := conn.QueryRow(ctx, "SELECT current_user").Scan(&user); err != nil {
					t.Fatal(err)
				}
				if user != tc.user {
					t.Fatalf("authenticated as %q", user)
				}
			}
		})
	}
	// Match the projected volumes: each client reads its key and certificate
	// from one credential bundle, which is replaced at the same path.
	writeBundle := func(t *testing.T, bundle, cert, key string) {
		t.Helper()
		certPEM, err := os.ReadFile(cert)
		if err != nil {
			t.Fatal(err)
		}
		keyPEM, err := os.ReadFile(key)
		if err != nil {
			t.Fatal(err)
		}
		writeCertTestFile(t, bundle, append(keyPEM, certPEM...))
	}
	runtimeBundle := filepath.Join(dir, "runtime-bundle.pem")
	ownerBundle := filepath.Join(dir, "owner-bundle.pem")
	writeBundle(t, runtimeBundle, runtimeCert, runtimeKey)
	writeBundle(t, ownerBundle, ownerCert, ownerKey)
	// Both pools must work together for migrations and runtime queries, without
	// allowing the runtime certificate to assume the owner's role.
	persistence, err := Connect(t.Context(), ConnectConfig{
		ReadWriteDSN:  certificateDSN(postgressetup.ReadWriteUser, runtimeBundle, runtimeBundle, ""),
		OwnerDSN:      certificateDSN(postgressetup.OwnerUser, ownerBundle, ownerBundle, ""),
		ReadWriteRole: postgressetup.ReadWriteRole,
		OwnerRole:     postgressetup.OwnerRole,
		Schema:        postgressetup.Schema,
	})
	if err != nil {
		t.Fatalf("connecting both certificate pools: %v", err)
	}
	defer persistence.pool.Close()
	defer persistence.Close()
	if _, err := persistence.CreateAtespace(t.Context(), newTestAtespace("cert-auth")); err != nil {
		t.Fatal(err)
	}
	if _, err := persistence.pool.Exec(t.Context(), "CREATE TABLE forbidden (id integer)"); err == nil {
		t.Fatal("runtime certificate allowed schema changes")
	}
	if _, err := persistence.pool.Exec(t.Context(), "SET ROLE "+pgx.Identifier{postgressetup.OwnerRole}.Sanitize()); err == nil {
		t.Fatal("runtime certificate assumed owner role")
	}
	t.Run("rotate credentials and trust roots", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		// Keep a connection open across rotation, and retain configurations that
		// would be used for new connections without the BeforeConnect hook.
		existing, err := persistence.pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer existing.Release()
		staleRuntime := persistence.pool.Config().ConnConfig.Copy()
		staleOwner := persistence.ownerPool.Config().ConnConfig.Copy()

		rotatedServerCA, err := localca.GenerateCA("rotated-service-dns", localca.KeyTypeED25519, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		rotatedClientCA, err := localca.GenerateCA("rotated-postgres", localca.KeyTypeED25519, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		rotatedServerCert, rotatedServerKey := writeTestCertificate(t, dir, "rotated-server", rotatedServerCA, "postgres", true)
		for _, login := range []struct{ user, bundle string }{
			{postgressetup.ReadWriteUser, runtimeBundle},
			{postgressetup.OwnerUser, ownerBundle},
		} {
			cert, key := writeTestCertificate(t, dir, "rotated-"+login.user, rotatedClientCA, login.user, false)
			writeBundle(t, login.bundle, cert, key)
		}
		writeCertTestFile(t, serverRoot, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rotatedServerCA.RootCertificate.Raw}))
		writeCertTestFile(t, clientRoot, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rotatedClientCA.RootCertificate.Raw}))
		for _, file := range []struct{ source, target string }{
			{rotatedServerCert, "/tmp/testcontainers-go/postgres/server.cert"},
			{rotatedServerKey, "/tmp/testcontainers-go/postgres/server.key"},
			{clientRoot, "/tmp/testcontainers-go/postgres/ca_cert.pem"},
		} {
			data, err := os.ReadFile(file.source)
			if err != nil {
				t.Fatal(err)
			}
			if err := container.CopyToContainer(ctx, data, file.target, 0600); err != nil {
				t.Fatal(err)
			}
		}
		code, _, err := container.Exec(ctx, []string{"chown", "postgres:postgres", "/tmp/testcontainers-go/postgres/server.cert", "/tmp/testcontainers-go/postgres/server.key", "/tmp/testcontainers-go/postgres/ca_cert.pem"}, tcexec.Multiplexed())
		if err != nil || code != 0 {
			t.Fatalf("setting rotated TLS file ownership: exit %d, error %v", code, err)
		}
		// Run the shipped reloader against the real server. Only adapt paths
		// and stop after its first successful reload instead of looping forever.
		reloader := strings.NewReplacer(
			"/run/servicedns.podcert.ate.dev/credential-bundle.pem", "/tmp/testcontainers-go/postgres/server.cert",
			"/run/postgres.podcert.ate.dev/trust-bundle.pem", "/tmp/testcontainers-go/postgres/ca_cert.pem",
			"/etc/postgresql/pg_hba.conf", "/tmp/testcontainers-go/postgres/pg_hba.conf",
			`echo "$(date -u +%FT%TZ) reloaded TLS configuration"`, `echo "$(date -u +%FT%TZ) reloaded TLS configuration"; exit 0`,
		).Replace(cm.Data["reload-tls.sh"])
		if err := container.CopyToContainer(ctx, []byte(reloader), "/tmp/reload-tls.sh", 0600); err != nil {
			t.Fatal(err)
		}
		code, output, err := container.Exec(ctx, []string{"sh", "/tmp/reload-tls.sh"}, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		detail, err := io.ReadAll(output)
		if err != nil || code != 0 || !strings.Contains(string(detail), "reloaded TLS configuration") {
			t.Fatalf("TLS reloader: exit %d, error %v, output %s", code, err, detail)
		}

		var user string
		if err := existing.QueryRow(ctx, `SELECT current_user`).Scan(&user); err != nil || user != postgressetup.ReadWriteRole {
			t.Fatalf("existing connection after rotation: user %q, error %v", user, err)
		}
		existing.Release()
		for _, identity := range []struct {
			pool *pgxpool.Pool
			role string
		}{
			{persistence.pool, postgressetup.ReadWriteRole},
			{persistence.ownerPool, postgressetup.OwnerRole},
		} {
			identity.pool.Reset()
			// SIGHUP processing is asynchronous; retry until the server has
			// loaded the replacement certificate and client trust roots.
			for attempt := 0; attempt < 50; attempt++ {
				err = identity.pool.QueryRow(ctx, `SELECT current_user`).Scan(&user)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil || user != identity.role {
				t.Fatalf("fresh pooled connection after rotation: role %q, want %q, error %v", user, identity.role, err)
			}
		}
		// Fresh connections using cached TLS configurations must fail. Even
		// with refreshed server trust, the old client certificates are rejected.
		for _, stale := range []*pgx.ConnConfig{staleRuntime, staleOwner} {
			conn, err := pgx.ConnectConfig(ctx, stale)
			if conn != nil {
				_ = conn.Close(ctx)
			}
			if err == nil {
				t.Fatal("cached server trust accepted the rotated server certificate")
			}
			stale.TLSConfig.RootCAs = rootPool(t, serverRoot)
			conn, err = pgx.ConnectConfig(ctx, stale)
			if conn != nil {
				_ = conn.Close(ctx)
			}
			if err == nil {
				t.Fatal("cached client certificate accepted after client CA rotation")
			}
		}
	})

}

func writeTestCertificate(t *testing.T, dir, name string, ca *localca.CA, cn string, server bool) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{"localhost"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	}
	pool := &localca.ConcretePool{CAs: []*localca.CA{ca}}
	chain, err := pool.CreateCertificate(template, &key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	cert := []byte{}
	for _, der := range chain {
		cert = append(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, fmt.Sprintf("%s-cert.pem", name)), filepath.Join(dir, fmt.Sprintf("%s-key.pem", name))
	writeCertTestFile(t, certPath, cert)
	writeCertTestFile(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return certPath, keyPath
}

func writeCertTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

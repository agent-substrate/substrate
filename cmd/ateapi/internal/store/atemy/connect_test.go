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

package atemy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestConnect_DedicatedPools covers the auxiliary pools only Connect builds
// (the rest of the suite uses NewPersistence, where every operation shares
// the caller's pool) and checks Close releases them but not the caller's.
func TestConnect_DedicatedPools(t *testing.T) {
	requireDB(t)
	ctx := t.Context()

	p, err := Connect(ctx, ConnectConfig{ReadWriteDSN: containerDSN, OwnerDSN: containerDSN, PoolMaxConns: 7})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.DB().Close()
	closed := false
	defer func() {
		if !closed {
			p.Close()
		}
	}()

	if p.watchDB == p.db || !p.ownsWatchDB {
		t.Fatal("Connect must own a dedicated watch pool")
	}
	if p.ownerDB == p.db || p.ownerDB == p.watchDB || !p.ownsOwnerDB {
		t.Fatal("Connect must own a dedicated owner pool")
	}
	for _, tc := range []struct {
		name string
		got  int
		want int
	}{
		{"read/write", p.db.Stats().MaxOpenConnections, 7},
		{"watch", p.watchDB.Stats().MaxOpenConnections, watchPoolMaxConns},
		{"owner", p.ownerDB.Stats().MaxOpenConnections, ownerPoolMaxConns},
	} {
		if tc.got != tc.want {
			t.Errorf("%s pool max connections = %d, want %d", tc.name, tc.got, tc.want)
		}
	}

	clearAll(t, p)
	watch, err := p.WatchWorkers(ctx)
	if err != nil {
		t.Fatalf("WatchWorkers failed: %v", err)
	}
	defer watch.Close()
	if _, err := p.CreateWorker(ctx, newTestWorker("watchpool-worker")); err != nil {
		t.Fatalf("CreateWorker failed: %v", err)
	}
	if event := receive(t, watch); event.Type != store.WorkerEventCreated || event.Worker.GetMetadata().GetName() != "watchpool-worker" {
		t.Fatalf("unexpected event %v %s", event.Type, event.Worker.GetMetadata().GetName())
	}
	watch.Close()

	p.Close()
	closed = true
	if err := p.watchDB.PingContext(ctx); err == nil {
		t.Error("watch pool still open after Close")
	}
	if err := p.ownerDB.PingContext(ctx); err == nil {
		t.Error("owner pool still open after Close")
	}
	if err := p.DB().PingContext(ctx); err != nil {
		t.Errorf("Close closed the caller's read/write pool: %v", err)
	}
}

func TestConnect_DefaultPoolSize(t *testing.T) {
	requireDB(t)
	p, err := Connect(t.Context(), ConnectConfig{ReadWriteDSN: containerDSN, OwnerDSN: containerDSN})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.DB().Close()
	defer p.Close()
	if got, want := p.db.Stats().MaxOpenConnections, storesql.DefaultMaxConns(); got != want {
		t.Errorf("read/write pool max connections = %d, want the default %d", got, want)
	}
}

// TestConnect_SeparatesRuntimeAndDDLPrivileges runs Connect with a
// read/write user that holds only DML privileges and an owner user that
// holds DDL privileges, as a production install does.
func TestConnect_SeparatesRuntimeAndDDLPrivileges(t *testing.T) {
	const database = "atemy_privileges"
	createTestDatabase(t, database)
	admin := openAdmin(t, "mysql")
	ctx := t.Context()
	for _, stmt := range []string{
		`DROP USER IF EXISTS 'atemy_rw'@'%', 'atemy_ddl'@'%'`,
		`CREATE USER 'atemy_rw'@'%' IDENTIFIED BY 'rw-password'`,
		`CREATE USER 'atemy_ddl'@'%' IDENTIFIED BY 'ddl-password'`,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON `" + database + "`.* TO 'atemy_rw'@'%'",
		"GRANT ALL ON `" + database + "`.* TO 'atemy_ddl'@'%'",
	} {
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("preparing users (%s): %v", stmt, err)
		}
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), `DROP USER IF EXISTS 'atemy_rw'@'%', 'atemy_ddl'@'%'`); err != nil {
			t.Errorf("dropping users: %v", err)
		}
	})
	userDSN := func(user, password string) string {
		cfg, err := mysql.ParseDSN(containerDSN)
		if err != nil {
			t.Fatalf("parsing container DSN: %v", err)
		}
		cfg.User, cfg.Passwd, cfg.DBName = user, password, database
		return cfg.FormatDSN()
	}

	p, err := Connect(ctx, ConnectConfig{
		ReadWriteDSN: userDSN("atemy_rw", "rw-password"),
		OwnerDSN:     userDSN("atemy_ddl", "ddl-password"),
		PoolMaxConns: 20,
	})
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	defer p.DB().Close()
	defer p.Close()
	setTestPolicyManager(t, p)

	if _, err := p.CreateAtespace(ctx, newTestAtespace("runtime-write")); err != nil {
		t.Fatalf("read/write operation failed: %v", err)
	}
	if _, err := p.CreateAtespaceAccessPolicy(ctx, "runtime-write", &ateapipb.AccessPolicy{
		Bindings: []*ateapipb.Binding{{Role: authz.RoleEditor, Members: []string{"user:bob"}}},
	}); err != nil {
		t.Fatalf("access policy write through OpenFGA tables failed: %v", err)
	}
	if _, err := p.CreateWorker(ctx, newTestWorker("runtime-worker")); err != nil {
		t.Fatalf("outbox write failed: %v", err)
	}
	if err := p.trimWorkerOutboxOlderThan(ctx, 0); err != nil {
		t.Fatalf("outbox retention failed: %v", err)
	}
	lease, err := p.AcquireLease(ctx, "runtime-lease")
	if err != nil {
		t.Fatalf("AcquireLease failed: %v", err)
	}
	lease.Close()
	if _, err := p.db.ExecContext(ctx, `CREATE TABLE forbidden (id INT)`); err == nil {
		t.Fatal("read/write user created a table")
	}
}

func TestConnect_RejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "bundle.pem")
	caPath := filepath.Join(dir, "ca.pem")
	writeCredentialBundle(t, certPath, caPath, 1)
	notPEM := filepath.Join(dir, "not-pem.txt")
	if err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("writing file: %v", err)
	}
	const dsn = "atemy:hunter2@tcp(127.0.0.1:1)/atemy"

	for _, tc := range []struct {
		name   string
		config ConnectConfig
		want   string
	}{
		{"negative pool size", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, PoolMaxConns: -1}, "must not be negative"},
		{"no owner DSN", ConnectConfig{ReadWriteDSN: dsn}, "owner connection string must not be empty"},
		{"unparsable DSN", ConnectConfig{ReadWriteDSN: "atemy:hunter2@tcp(127.0.0.1:1)/atemy?timeout=forever", OwnerDSN: dsn}, "invalid value"},
		{"read/write DSN without a database", ConnectConfig{ReadWriteDSN: "atemy:hunter2@tcp(127.0.0.1:1)/", OwnerDSN: dsn}, "must name a database"},
		{"owner DSN without a database", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: "atemy:hunter2@tcp(127.0.0.1:1)/"}, "must name a database"},
		{"owner DSN names another database", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: "atemy:hunter2@tcp(127.0.0.1:1)/other"}, "name different databases"},
		{"missing CA file", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, TLS: TLSFiles{CAFile: filepath.Join(dir, "missing.pem")}}, "reading MySQL CA file"},
		{"CA file without certificates", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, TLS: TLSFiles{CAFile: notPEM}}, "holds no PEM certificates"},
		{"certificate without key", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, TLS: TLSFiles{CertFile: certPath}}, "must be set together"},
		{"key without certificate", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, TLS: TLSFiles{KeyFile: certPath}}, "must be set together"},
		{"unloadable key pair", ConnectConfig{ReadWriteDSN: dsn, OwnerDSN: dsn, TLS: TLSFiles{CertFile: notPEM, KeyFile: notPEM}}, "loading MySQL client certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Connect(t.Context(), tc.config)
			if err == nil {
				p.Close()
				t.Fatalf("Connect succeeded, want an error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("Connect error = %v, want one containing %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Errorf("Connect error %q exposes the password", err)
			}
			if errors.Is(err, ErrUnavailable) {
				t.Errorf("Connect error %v is retryable, want a configuration error", err)
			}
		})
	}
}

// TestConnect_UsesTLSFiles shows the files secure real connections: the
// container's auto-generated certificate is not signed by the test CA, so
// the handshake fails.
func TestConnect_UsesTLSFiles(t *testing.T) {
	requireDB(t)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "bundle.pem")
	caPath := filepath.Join(dir, "ca.pem")
	writeCredentialBundle(t, certPath, caPath, 1)

	p, err := Connect(t.Context(), ConnectConfig{ReadWriteDSN: containerDSN, OwnerDSN: containerDSN, TLS: TLSFiles{CAFile: caPath}})
	if err == nil {
		p.DB().Close()
		p.Close()
		t.Fatal("Connect trusted a server certificate the CA file did not sign")
	}
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "certificate") {
		t.Errorf("Connect error = %v, want ErrUnavailable from certificate verification", err)
	}
}

func TestNewConnector_AppliesSessionSettings(t *testing.T) {
	c, err := newConnector("atemy:pw@tcp(db.example:3306)/atemy?multiStatements=true&parseTime=false", TLSFiles{})
	if err != nil {
		t.Fatalf("newConnector failed: %v", err)
	}
	cfg := c.cfg
	if !cfg.ParseTime || cfg.Loc != time.UTC || !cfg.ClientFoundRows || !cfg.InterpolateParams || cfg.MultiStatements {
		t.Errorf("session settings = parseTime:%t loc:%v clientFoundRows:%t interpolateParams:%t multiStatements:%t; want true UTC true true false",
			cfg.ParseTime, cfg.Loc, cfg.ClientFoundRows, cfg.InterpolateParams, cfg.MultiStatements)
	}
	if _, err := Open("atemy:pw@tcp(db.example:3306)/"); err == nil || !strings.Contains(err.Error(), "must name a database") {
		t.Errorf("Open without a database = %v, want a must-name-a-database error", err)
	}
}

// TestOpen_SessionsRunReadCommitted checks the isolation every transaction
// inherits from the session, the level atepg runs at.
func TestOpen_SessionsRunReadCommitted(t *testing.T) {
	db := requireDB(t)
	var isolation string
	if err := db.QueryRowContext(t.Context(), `SELECT @@SESSION.transaction_isolation`).Scan(&isolation); err != nil {
		t.Fatalf("reading session isolation: %v", err)
	}
	if isolation != "READ-COMMITTED" {
		t.Errorf("session isolation = %q, want READ-COMMITTED", isolation)
	}
}

// TestConnector_RereadsTLSFiles covers certificate rotation: a long-lived
// pool must present the certificate on disk when each connection opens, not
// the one that was there at startup.
func TestConnector_RereadsTLSFiles(t *testing.T) {
	dir := t.TempDir()
	bundlePath := filepath.Join(dir, "credential-bundle.pem")
	rootPath := filepath.Join(dir, "trust-bundle.pem")
	writeCredentialBundle(t, bundlePath, rootPath, 1)
	files := TLSFiles{CAFile: rootPath, CertFile: bundlePath, KeyFile: bundlePath}

	c, err := newConnector("atemy:pw@tcp(mysql.ate-system.svc:3306)/atemy", files)
	if err != nil {
		t.Fatalf("newConnector failed: %v", err)
	}
	cfg, err := loadTLSConfig(c.tls, "mysql.ate-system.svc")
	if err != nil {
		t.Fatalf("loadTLSConfig: %v", err)
	}
	if got := clientCertSerial(t, cfg.Certificates); got != 1 {
		t.Fatalf("client certificate serial = %d, want 1", got)
	}
	if cfg.ServerName != "mysql.ate-system.svc" || cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("TLS config server name %q, minimum version %x; want mysql.ate-system.svc and TLS 1.2", cfg.ServerName, cfg.MinVersion)
	}

	writeCredentialBundle(t, bundlePath, rootPath, 2)
	cfg, err = loadTLSConfig(c.tls, "mysql.ate-system.svc")
	if err != nil {
		t.Fatalf("loadTLSConfig after rotation: %v", err)
	}
	if got := clientCertSerial(t, cfg.Certificates); got != 2 {
		t.Errorf("client certificate serial after rotation = %d, want 2", got)
	}
	if !cfg.RootCAs.Equal(rootPool(t, rootPath)) {
		t.Error("root CAs after rotation are not the ones on disk")
	}

	// Each new connection reads the files, so one that has gone missing fails
	// the connection before any network traffic.
	if err := os.Remove(rootPath); err != nil {
		t.Fatalf("removing CA file: %v", err)
	}
	if _, err := c.Connect(t.Context()); err == nil || !strings.Contains(err.Error(), "reading MySQL CA file") {
		t.Errorf("connector.Connect without its CA file = %v, want a CA file error", err)
	}
}

// writeCredentialBundle writes a self-signed certificate with the given serial
// to certPath in the layout of a Kubernetes credential bundle (private key
// first, then the chain) and its certificate alone to rootPath.
func writeCredentialBundle(t *testing.T, certPath, rootPath string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "mysql.ate-system.svc"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("signing certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, append(keyPEM, certPEM...), 0o600); err != nil {
		t.Fatalf("writing credential bundle: %v", err)
	}
	if err := os.WriteFile(rootPath, certPEM, 0o644); err != nil {
		t.Fatalf("writing trust bundle: %v", err)
	}
}

func clientCertSerial(t *testing.T, certs []tls.Certificate) int64 {
	t.Helper()
	if len(certs) != 1 {
		t.Fatalf("got %d client certificates, want 1", len(certs))
	}
	leaf, err := x509.ParseCertificate(certs[0].Certificate[0])
	if err != nil {
		t.Fatalf("parsing client certificate: %v", err)
	}
	return leaf.SerialNumber.Int64()
}

func rootPool(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading trust bundle: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("trust bundle holds no certificates")
	}
	return pool
}

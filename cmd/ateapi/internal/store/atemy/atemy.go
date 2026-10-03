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

// Package atemy is an ate storage backend built on MySQL 8.0 or later. It
// uses only features PlanetScale's Vitess supports: no foreign keys, stored
// routines, triggers, partitioning or CREATE DATABASE, and no DDL outside the
// migrations applied at startup.
//
// Each table holds native SQL columns for fields SQL must operate on
// (primary keys, versions, pagination, update/delete preconditions) plus
// the complete protobuf message, binary-encoded, in a LONGBLOB column.
// Parent and child rows are kept consistent by the transactions that write
// them, in place of foreign keys. TLS is configured through the tls
// connection string parameter, or through ConnectConfig.TLS files that are
// read again for every new connection.
package atemy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/go-sql-driver/mysql"
)

const (
	// watchPoolMaxConns sizes the dedicated watch pool: one connection for the
	// WatchWorkers poller, one for the maintenance loop, and one of headroom so
	// a transiently slow poll can never gate a maintenance pass.
	watchPoolMaxConns = 3
	// Migrations need one connection for the session lock and one for
	// migration work.
	ownerPoolMaxConns = 2
)

// Persistence is a service that stores ate state in MySQL.
type Persistence struct {
	db *sql.DB
	// watchDB serves WatchWorkers pollers, outbox retention and expired-lease
	// cleanup. ownerDB applies migrations.
	watchDB               *sql.DB
	ownerDB               *sql.DB
	ownsWatchDB           bool
	ownsOwnerDB           bool
	policyManager         *authz.PolicyManager
	leaseTTL              time.Duration
	pollFailureCloseAfter time.Duration
	stopMaintenance       context.CancelFunc
	maintenanceDone       chan struct{}
	// watchers are the live WatchWorkers channels.
	watchers storesql.Watchers
}

var _ store.Interface = (*Persistence)(nil)

// ErrUnavailable reports that ateapi could not establish the initial MySQL
// connection. Callers can retry this error before startup.
var ErrUnavailable = errors.New("MySQL is unavailable")

// TLSFiles names PEM files that secure MySQL connections. They are read again
// for every new connection, so rotated certificates take effect without a
// restart.
type TLSFiles struct {
	// CAFile verifies the server certificate. Empty uses the system roots.
	CAFile string
	// CertFile and KeyFile hold the client certificate and key. Both may name
	// the same file. Empty presents no client certificate.
	CertFile string
	KeyFile  string
}

func (f TLSFiles) enabled() bool {
	return f.CAFile != "" || f.CertFile != "" || f.KeyFile != ""
}

// ConnectConfig configures the MySQL pools used by Persistence. The database
// named in each DSN must already exist; atemy never creates one.
type ConnectConfig struct {
	ReadWriteDSN string
	// OwnerDSN applies migrations and needs DDL privileges on the database.
	OwnerDSN     string
	TLS          TLSFiles
	PoolMaxConns int32
}

// Connect opens read/write and owner pools and applies migrations.
func Connect(ctx context.Context, config ConnectConfig) (*Persistence, error) {
	if config.PoolMaxConns < 0 {
		return nil, fmt.Errorf("MySQL pool maximum connections must not be negative")
	}
	if config.OwnerDSN == "" {
		return nil, fmt.Errorf("MySQL owner connection string must not be empty")
	}
	readWrite, err := newConnector(config.ReadWriteDSN, config.TLS)
	if err != nil {
		return nil, err
	}
	owner, err := newConnector(config.OwnerDSN, config.TLS)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL owner connection string: %w", err)
	}
	if readWrite.cfg.DBName != owner.cfg.DBName {
		return nil, fmt.Errorf("MySQL read/write and owner connection strings name different databases")
	}

	maxConns := storesql.DefaultMaxConns()
	if config.PoolMaxConns > 0 {
		maxConns = int(config.PoolMaxConns)
	}
	db := sql.OpenDB(readWrite)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(storesql.ConnMaxLifetime)
	db.SetConnMaxIdleTime(storesql.ConnMaxIdleTime)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("%w: pinging MySQL: %w", ErrUnavailable, err)
	}
	ownerDB := sql.OpenDB(owner)
	ownerDB.SetMaxOpenConns(ownerPoolMaxConns)
	ownerDB.SetMaxIdleConns(0)
	if err := ownerDB.PingContext(ctx); err != nil {
		ownerDB.Close()
		db.Close()
		return nil, fmt.Errorf("%w: ping MySQL owner connection: %w", ErrUnavailable, err)
	}
	watchDB := sql.OpenDB(readWrite)
	watchDB.SetMaxOpenConns(watchPoolMaxConns)
	watchDB.SetMaxIdleConns(watchPoolMaxConns)
	watchDB.SetConnMaxLifetime(storesql.ConnMaxLifetime)
	watchDB.SetConnMaxIdleTime(storesql.ConnMaxIdleTime)

	p, err := newPersistence(ctx, db, watchDB, ownerDB)
	if err != nil {
		watchDB.Close()
		ownerDB.Close()
		db.Close()
		return nil, err
	}
	p.ownsWatchDB = true
	p.ownsOwnerDB = true
	return p, nil
}

// connector opens MySQL connections from a parsed DSN, loading TLS material
// from files for each new connection when TLSFiles are set.
type connector struct {
	cfg *mysql.Config
	tls TLSFiles
}

var _ driver.Connector = (*connector)(nil)

// newConnector parses dsn and applies the session settings atemy relies on.
func newConnector(dsn string, files TLSFiles) (*connector, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing MySQL connection string: invalid value")
	}
	if cfg.DBName == "" {
		return nil, fmt.Errorf("MySQL connection string must name a database")
	}
	// OpenFGA scans TIMESTAMP columns into time.Time.
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	// RowsAffected reports matched rows, as PostgreSQL does, so a guarded
	// UPDATE that rewrites identical bytes still counts as applied.
	cfg.ClientFoundRows = true
	// Interpolating parameters client-side saves the prepare and close round
	// trips the driver would otherwise spend on every parameterized statement.
	cfg.InterpolateParams = true
	cfg.MultiStatements = false
	if files.enabled() {
		if _, err := loadTLSConfig(files, ""); err != nil {
			return nil, err
		}
	}
	return &connector{cfg: cfg, tls: files}, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg := c.cfg
	if c.tls.enabled() {
		host, _, err := net.SplitHostPort(c.cfg.Addr)
		if err != nil {
			host = c.cfg.Addr
		}
		tlsConfig, err := loadTLSConfig(c.tls, host)
		if err != nil {
			return nil, err
		}
		cfg = c.cfg.Clone()
		cfg.TLS = tlsConfig
		// TLS files demand TLS; tls=preferred in the DSN must not turn
		// that into an optional upgrade.
		cfg.AllowFallbackToPlaintext = false
	}
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return conn.Connect(ctx)
}

func (c *connector) Driver() driver.Driver { return &mysql.MySQLDriver{} }

func loadTLSConfig(files TLSFiles, serverName string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if files.CAFile != "" {
		pem, err := os.ReadFile(files.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading MySQL CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("MySQL CA file holds no PEM certificates")
		}
		cfg.RootCAs = pool
	}
	if files.CertFile != "" || files.KeyFile != "" {
		if files.CertFile == "" || files.KeyFile == "" {
			return nil, fmt.Errorf("MySQL client certificate and key files must be set together")
		}
		cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading MySQL client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// Open returns a pool on dsn with the session settings atemy relies on,
// without connecting. Pass the result to NewPersistence.
func Open(dsn string) (*sql.DB, error) {
	c, err := newConnector(dsn, TLSFiles{})
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(c), nil
}

// NewPersistence wraps an already-open database, applying pending migrations.
// Callers that already hold a *sql.DB (e.g. tests using testcontainers) use
// this directly instead of Connect; outbox watch traffic shares the given
// database. Open db with Open.
func NewPersistence(ctx context.Context, db *sql.DB) (*Persistence, error) {
	return newPersistence(ctx, db, db, db)
}

func newPersistence(ctx context.Context, db, watchDB, ownerDB *sql.DB) (*Persistence, error) {
	if err := applyMigrations(ctx, ownerDB); err != nil {
		return nil, err
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(context.Background())
	p := &Persistence{
		db:                    db,
		watchDB:               watchDB,
		ownerDB:               ownerDB,
		leaseTTL:              storesql.DefaultLeaseTTL,
		pollFailureCloseAfter: outboxPollFailureCloseAfter,
		stopMaintenance:       stopMaintenance,
		maintenanceDone:       make(chan struct{}),
	}
	go func() {
		defer close(p.maintenanceDone)
		p.maintenance(maintenanceCtx)
	}()
	return p, nil
}

// Close stops the maintenance loop and waits for it to exit, then closes the
// auxiliary pools if Connect created them. It does not close the main
// database, which the caller owns.
func (p *Persistence) Close() {
	p.stopMaintenance()
	<-p.maintenanceDone
	if p.ownsWatchDB {
		p.watchDB.Close()
	}
	if p.ownsOwnerDB {
		p.ownerDB.Close()
	}
}

// DB returns the underlying MySQL connection pool.
func (p *Persistence) DB() *sql.DB {
	return p.db
}

// SetPolicyManager configures the authorization policy manager that writes
// OpenFGA tuples in the same transaction as access policy and atespace
// mutations. It must be set before the store serves access policy or
// atespace writes.
func (p *Persistence) SetPolicyManager(pm *authz.PolicyManager) {
	p.policyManager = pm
}

// querier is satisfied by *sql.DB and *sql.Tx, letting read helpers run
// either directly against the pool or inside an in-flight transaction.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// txAttempts bounds how many times inTx runs a transaction InnoDB chose as a
// deadlock victim.
const txAttempts = 3

// inTx runs fn in a READ COMMITTED transaction on db, the isolation PostgreSQL
// gives atepg, and commits if fn succeeds. InnoDB resolves a deadlock by
// rolling back one transaction, which can happen even between two inserts of
// one key, so inTx runs fn again from the start up to txAttempts times. fn
// must therefore be safe to repeat, as the store's update contract already
// requires of mutate.
func inTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	var err error
	for range txAttempts {
		if err = runTx(ctx, db, fn); mysqlErrNumber(err) != 1213 {
			break
		}
	}
	return mapLockError(err)
}

func runTx(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

// mapLockError reports a deadlock that outlasted inTx's attempts, or a lock
// wait timeout, as ErrVersionConflict: the write lost to a concurrent one and
// the caller may retry.
func mapLockError(err error) error {
	switch mysqlErrNumber(err) {
	case 1213, 1205:
		return fmt.Errorf("%w: %w", store.ErrVersionConflict, err)
	default:
		return err
	}
}

func isUniqueViolation(err error) bool { return mysqlErrNumber(err) == 1062 }

func mysqlErrNumber(err error) uint16 {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number
	}
	return 0
}

// requireOneRow reports a statement that should have touched exactly one row.
func requireOneRow(res sql.Result, what string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if n != 1 {
		return fmt.Errorf("%s affected %d rows, want 1", what, n)
	}
	return nil
}

const (
	// Paces the maintenance loop (outbox retention and expired leases).
	maintenanceInterval = time.Minute

	// Bounds a maintenance pass to prevent indefinite hangs (e.g., from lock
	// waits). Stalls abort and retry.
	maintenancePassTimeout = 5 * time.Minute
)

// Trims the worker outbox and reaps expired leases on a fixed timer. The two
// are independent: a failure in one still lets the other run.
func (p *Persistence) maintenance(ctx context.Context) {
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		passCtx, cancel := context.WithTimeout(ctx, maintenancePassTimeout)
		if err := p.trimWorkerOutbox(passCtx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "worker outbox retention failed", slog.Any("err", err))
		}
		if deleted, err := p.cleanupExpiredLeases(passCtx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "expired lease cleanup failed", slog.Int64("deleted", deleted), slog.Any("err", err))
		} else if deleted > 0 {
			slog.InfoContext(ctx, "removed expired MySQL leases", slog.Int64("deleted", deleted))
		}
		cancel()
	}
}

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

// Package atepg is an ate storage backend built on PostgreSQL.
//
// Each table holds native SQL columns for fields SQL must operate on
// (primary keys, versions, pagination, update/delete preconditions) plus
// the complete protobuf message, binary-encoded, in a BYTEA column.
// TLS is configured entirely through the connection string passed
// to Connect (standard libpq sslmode/sslrootcert/sslcert/sslkey parameters)
package atepg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/defaults"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Persistence is a service that stores ate state in PostgreSQL.
// watchPoolMaxConns sizes the dedicated watch pool: one connection for the
// WatchWorkers poller, one for the maintenance loop, and one of headroom so a
// transiently slow poll can never gate a maintenance pass.
const (
	watchPoolMaxConns = 3
	watchPoolMinConns = 1
	// Migrations need one connection for Goose's session lock and one for
	// migration work. Outbox maintenance is serial after startup.
	ownerPoolMaxConns = 2
)

type Persistence struct {
	pool *pgxpool.Pool
	// watchPool serves WatchWorkers pollers and expired-lease cleanup.
	// ownerPool applies migrations and maintains outbox partitions.
	watchPool             *pgxpool.Pool
	ownerPool             *pgxpool.Pool
	ownsWatchPool         bool
	ownsOwnerPool         bool
	policyManager         *authz.PolicyManager
	leaseTTL              time.Duration
	pollFailureCloseAfter time.Duration
	stopMaintenance       context.CancelFunc
	maintenanceDone       chan struct{}
	// watchMu guards watchers, the live WatchWorkers channels.
	watchMu  sync.Mutex
	watchers map[chan store.WorkerEvent]struct{}
}

// addWatcher enrolls a WatchWorkers channel to receive locally published events.
func (p *Persistence) addWatcher(ch chan store.WorkerEvent) {
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	p.watchers[ch] = struct{}{}
}

// removeWatcher unenrolls a channel. The caller must call it before closing
// the channel: once it returns, publishLocally can no longer send on it.
func (p *Persistence) removeWatcher(ch chan store.WorkerEvent) {
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	delete(p.watchers, ch)
}

// publishLocally hands a committed event to this process's watchers a poll
// interval ahead of the outbox, one copy each. Sends are non-blocking: a
// watcher with a full buffer is skipped and gets the event from the outbox.
func (p *Persistence) publishLocally(ctx context.Context, payload []byte) {
	p.watchMu.Lock()
	defer p.watchMu.Unlock()
	if len(p.watchers) == 0 {
		return
	}
	event, err := unmarshalWorkerEvent(payload)
	if err != nil {
		slog.ErrorContext(ctx, "decoding locally published worker event failed", slog.Any("err", err))
		return
	}
	for ch := range p.watchers {
		select {
		case ch <- store.WorkerEvent{Type: event.Type, Worker: proto.Clone(event.Worker).(*ateapipb.Worker)}:
		default:
		}
	}
}

var _ store.Interface = (*Persistence)(nil)

// ErrUnavailable reports that ateapi could not establish the initial
// PostgreSQL connection. Callers can retry this error before startup.
var ErrUnavailable = errors.New("PostgreSQL is unavailable")

// ConnectConfig configures the PostgreSQL pools used by Persistence.
type ConnectConfig struct {
	ReadWriteDSN  string
	OwnerDSN      string
	ReadWriteRole string
	OwnerRole     string
	Schema        string
	PoolMaxConns  int32
}

// Connect opens read/write and owner pools. It creates the schema and applies migrations.
func Connect(ctx context.Context, config ConnectConfig) (*Persistence, error) {
	if config.Schema == "" {
		return nil, fmt.Errorf("PostgreSQL schema must not be empty")
	}
	if config.ReadWriteRole == "" {
		return nil, fmt.Errorf("PostgreSQL read/write role must not be empty")
	}
	if config.OwnerRole == "" {
		return nil, fmt.Errorf("PostgreSQL owner role must not be empty")
	}
	if config.PoolMaxConns < 0 {
		return nil, fmt.Errorf("PostgreSQL pool maximum connections must not be negative")
	}
	if config.OwnerDSN == "" {
		return nil, fmt.Errorf("PostgreSQL owner connection string must not be empty")
	}
	readWriteConfig, err := poolConfig(config.ReadWriteDSN, config.ReadWriteRole)
	if err != nil {
		return nil, err
	}
	if config.PoolMaxConns > 0 {
		readWriteConfig.MaxConns = config.PoolMaxConns
	}
	readWriteConfig.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{config.Schema}.Sanitize()
	ownerConfig, err := poolConfig(config.OwnerDSN, config.OwnerRole)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL owner connection string: %w", err)
	}
	ownerConfig.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{config.Schema}.Sanitize()
	ownerConfig.MaxConns = ownerPoolMaxConns
	ownerConfig.MinConns = 0
	ownerConfig.MinIdleConns = 0

	pool, err := pgxpool.NewWithConfig(ctx, readWriteConfig)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: pinging PostgreSQL: %w", ErrUnavailable, err)
	}

	ownerPool, err := pgxpool.NewWithConfig(ctx, ownerConfig)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open PostgreSQL owner pool: %w", err)
	}
	if err := ownerPool.Ping(ctx); err != nil {
		ownerPool.Close()
		pool.Close()
		return nil, fmt.Errorf("%w: ping PostgreSQL owner connection: %w", ErrUnavailable, err)
	}
	if err := createSchema(ctx, ownerPool, config.Schema); err != nil {
		ownerPool.Close()
		pool.Close()
		return nil, err
	}

	watchCfg := readWriteConfig.Copy()
	watchCfg.MaxConns = watchPoolMaxConns
	watchCfg.MinConns = watchPoolMinConns
	watchPool, err := pgxpool.NewWithConfig(ctx, watchCfg)
	if err != nil {
		ownerPool.Close()
		pool.Close()
		return nil, fmt.Errorf("opening PostgreSQL watch pool: %w", err)
	}

	p, err := newPersistence(ctx, pool, watchPool, ownerPool)
	if err != nil {
		watchPool.Close()
		ownerPool.Close()
		pool.Close()
		return nil, err
	}
	p.ownsWatchPool = true
	p.ownsOwnerPool = true
	return p, nil
}

func createSchema(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("starting PostgreSQL schema transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // Commit or the returned error decides the outcome.

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "agent-substrate:create-schema:"+schema); err != nil {
		return fmt.Errorf("locking PostgreSQL schema %q: %w", schema, err)
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema).Scan(&exists); err != nil {
		return fmt.Errorf("checking PostgreSQL schema %q: %w", schema, err)
	}
	if !exists {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
			return fmt.Errorf("creating PostgreSQL schema %q: %w", schema, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing PostgreSQL schema %q: %w", schema, err)
	}
	return nil
}

// poolConfig parses a DSN, assumes the configured role, and refreshes TLS
// material from projected certificate files for each new connection.
func poolConfig(dsn, role string) (*pgxpool.Config, error) {
	if role == "" {
		return nil, fmt.Errorf("PostgreSQL role must not be empty")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing PostgreSQL connection string: invalid value")
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			return fmt.Errorf("assuming PostgreSQL role %q: %w", role, err)
		}
		return nil
	}
	usesTLS := cfg.ConnConfig.TLSConfig != nil
	for _, fallback := range cfg.ConnConfig.Fallbacks {
		usesTLS = usesTLS || fallback.TLSConfig != nil
	}
	if !usesTLS {
		return cfg, nil
	}
	cfg.BeforeConnect = func(_ context.Context, cc *pgx.ConnConfig) error {
		fresh, err := pgx.ParseConfig(dsn)
		if err != nil {
			return fmt.Errorf("re-reading PostgreSQL TLS material: invalid value")
		}
		cc.TLSConfig = fresh.TLSConfig
		cc.Fallbacks = fresh.Fallbacks
		return nil
	}
	return cfg, nil
}

// NewPersistence wraps an already-open pool, applying pending migrations.
// Callers that already hold a pool (e.g. tests using testcontainers) use
// this directly instead of Connect; outbox watch traffic shares the given pool.
func NewPersistence(ctx context.Context, pool *pgxpool.Pool) (*Persistence, error) {
	return newPersistence(ctx, pool, pool, pool)
}

func newPersistence(ctx context.Context, pool, watchPool, ownerPool *pgxpool.Pool) (*Persistence, error) {
	if err := applyMigrations(ctx, ownerPool); err != nil {
		return nil, err
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(context.Background())
	p := &Persistence{
		pool:                  pool,
		watchPool:             watchPool,
		ownerPool:             ownerPool,
		leaseTTL:              defaultLeaseTTL,
		pollFailureCloseAfter: outboxPollFailureCloseAfter,
		stopMaintenance:       stopMaintenance,
		maintenanceDone:       make(chan struct{}),
		watchers:              make(map[chan store.WorkerEvent]struct{}),
	}
	// Cover the partition lead before accepting writes; from then on the
	// maintenance loop keeps partitions ahead of the clock (and the
	// DEFAULT partition catches writes if it ever falls behind).
	bootNow, err := p.outboxNow(ctx)
	if err != nil {
		stopMaintenance()
		return nil, err
	}
	if err := p.createWorkerOutboxPartitions(ctx, outboxPartitionLeadTimes(bootNow)...); err != nil {
		stopMaintenance()
		return nil, err
	}
	go func() {
		defer close(p.maintenanceDone)
		p.maintenance(maintenanceCtx)
	}()
	return p, nil
}

// Close stops the maintenance loop and waits for it to exit,
// then closes the auxiliary pools if Connect created them. It does not close the
// main pool, which the caller owns.
func (p *Persistence) Close() {
	p.stopMaintenance()
	<-p.maintenanceDone
	if p.ownsWatchPool {
		p.watchPool.Close()
	}
	if p.ownsOwnerPool {
		p.ownerPool.Close()
	}
}

// Pool returns the underlying PostgreSQL connection pool.
func (p *Persistence) Pool() *pgxpool.Pool {
	return p.pool
}

// SetPolicyManager configures the authorization policy manager that writes
// OpenFGA tuples in the same transaction as access policy and atespace
// mutations. It must be set before the store serves access policy or
// atespace writes.
func (p *Persistence) SetPolicyManager(pm *authz.PolicyManager) {
	p.policyManager = pm
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting read helpers
// run either directly against the pool or inside an in-flight transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// unmarshalStored decodes a stored proto, dropping fields this binary has no
// descriptor for. This means a newer replica can have written such a field.
// It also backfills defaults to make all resources are properly defaulted, even
// the ones stored before a field with defaults was introduced.
func unmarshalStored(b []byte, m proto.Message) error {
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, m); err != nil {
		return err
	}
	defaults.Apply(m)
	return nil
}

// unmarshalRow is unmarshalStored for a row in a listing. A listing fails as a
// whole on one bad row, so the error names the row.
func unmarshalRow(b []byte, m proto.Message, kind string, id ...string) error {
	if err := unmarshalStored(b, m); err != nil {
		return fmt.Errorf("unmarshaling %s %s: %w", kind, strings.Join(id, "/"), err)
	}
	return nil
}

func setCreateMetadata(metadata *ateapipb.ResourceMetadata) {
	metadata.Uid = uuid.NewString()
	metadata.Version = 1
	metadata.CreateTime = timestamppb.Now()
	metadata.UpdateTime = metadata.CreateTime
}

// validateProtoMetadataMatchesColumns verifies that the metadata in the database
// matches the metadata in the proto.
func validateProtoMetadataMatchesColumns(resource string, metadata *ateapipb.ResourceMetadata, uid string, version int64) error {
	if metadata.GetUid() != uid {
		return fmt.Errorf("%s uid projection %q does not match proto metadata uid %q", resource, uid, metadata.GetUid())
	}
	if metadata.GetVersion() != version {
		return fmt.Errorf("%s version projection %d does not match proto metadata version %d", resource, version, metadata.GetVersion())
	}
	return nil
}

func setUpdateMetadata(newMeta, oldMeta *ateapipb.ResourceMetadata) {
	newMeta.Uid = oldMeta.Uid
	newMeta.Version = oldMeta.Version + 1
	newMeta.CreateTime = oldMeta.CreateTime
	newMeta.UpdateTime = timestamppb.Now()
}

func mapDeleteError(err error, uid string, version int64, precondition store.DeletePreconditions) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("reading after a guarded delete matched nothing: %w", err)
	}
	if err := precondition.Check(&ateapipb.ResourceMetadata{Uid: uid, Version: version}); err != nil {
		return err
	}
	// The row matches the guards now, so it changed between the two statements.
	return store.ErrVersionConflict
}

func isUniqueViolation(err error) bool { return pgErrCode(err) == "23505" }

// isForeignKeyViolation matches both the insert/update-side violation
// (23503, foreign_key_violation) and the delete-side violation PostgreSQL 18
// split out into its own code (23001, restrict_violation, for ON DELETE
// RESTRICT); older PostgreSQL versions report 23503 for both cases.
func isForeignKeyViolation(err error) bool {
	switch pgErrCode(err) {
	case "23503", "23001":
		return true
	default:
		return false
	}
}

func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func pgErrConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

const (
	// Paces the maintenance loop (outbox partitions and expired leases).
	maintenanceInterval = time.Minute

	// Bounds a maintenance pass to prevent indefinite hangs (e.g., from lock waits)
	// which would permanently starve partition creation. Stalls abort and retry.
	maintenancePassTimeout = 5 * time.Minute
)

// Maintains worker_outbox partitions and reaps expired leases on a fixed
// timer. The two are independent: a failure in one still lets the other run.
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
		if err := p.maintainWorkerOutboxPartitions(passCtx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "worker outbox maintenance failed", slog.Any("err", err))
		}
		if deleted, err := p.cleanupExpiredLeases(passCtx); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "expired lease cleanup failed", slog.Int64("deleted", deleted), slog.Any("err", err))
		} else if deleted > 0 {
			slog.InfoContext(ctx, "removed expired PostgreSQL leases", slog.Int64("deleted", deleted))
		}
		cancel()
	}
}

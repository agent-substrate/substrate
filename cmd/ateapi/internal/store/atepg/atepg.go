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
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/authz"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Persistence is a service that stores ate state in PostgreSQL.
// watchPoolMaxConns sizes the dedicated outbox watch pool: one connection
// for the WatchWorkers poller, one for the maintenance loop, and one of headroom
// so a transiently slow poll can never gate a maintenance pass.
const (
	watchPoolMaxConns = 3
	watchPoolMinConns = 1
)

type Persistence struct {
	pool *pgxpool.Pool
	// watchPool serves the outbox side only: the WatchWorkers pollers
	// and the partition-maintenance loop.
	watchPool             *pgxpool.Pool
	ownsWatchPool         bool
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

// Connect opens a pgxpool against dsn, creates schema if necessary, and
// applies pending schema migrations. A dedicated watch pool isolates outbox
// polling and maintenance from writes.
func Connect(ctx context.Context, dsn, schema string) (*Persistence, error) {
	if schema == "" {
		return nil, fmt.Errorf("PostgreSQL schema must not be empty")
	}
	cfg, err := poolConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: pinging PostgreSQL: %w", ErrUnavailable, err)
	}
	if err := createSchema(ctx, pool, schema); err != nil {
		pool.Close()
		return nil, err
	}

	watchCfg := cfg.Copy()
	watchCfg.MaxConns = watchPoolMaxConns
	watchCfg.MinConns = watchPoolMinConns
	watchPool, err := pgxpool.NewWithConfig(ctx, watchCfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("opening PostgreSQL watch pool: %w", err)
	}

	p, err := newPersistence(ctx, pool, watchPool)
	if err != nil {
		watchPool.Close()
		pool.Close()
		return nil, err
	}
	p.ownsWatchPool = true
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
	if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+pgx.Identifier{schema}.Sanitize()); err != nil {
		return fmt.Errorf("creating PostgreSQL schema %q: %w", schema, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing PostgreSQL schema %q: %w", schema, err)
	}
	return nil
}

// poolConfig parses dsn into a pool configuration whose TLS material is read
// from disk again for every new connection.
//
// pgx resolves sslcert, sslkey and sslrootcert once, when the connection
// string is parsed, and pins the result for the life of the pool. The paths in
// use here are projected pod certificates that the kubelet replaces about
// every day, so a long-lived process would keep presenting the client
// certificate it started with, and keep trusting only the CAs it started with,
// until connections started failing. Re-parsing in BeforeConnect costs one
// small file read per new connection and picks up every rotation.
func poolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing PostgreSQL connection string: %w", err)
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
			return fmt.Errorf("re-reading PostgreSQL TLS material: %w", err)
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
	return newPersistence(ctx, pool, pool)
}

func newPersistence(ctx context.Context, pool, watchPool *pgxpool.Pool) (*Persistence, error) {
	if err := applyMigrations(ctx, pool); err != nil {
		return nil, err
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(context.Background())
	p := &Persistence{
		pool:                  pool,
		watchPool:             watchPool,
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
		p.outboxMaintenance(maintenanceCtx)
	}()
	return p, nil
}

// Close stops the outbox maintenance loop and waits for it to exit,
// then closes the watch pool if Connect created one. It does not close the
// main pool, which the caller owns.
func (p *Persistence) Close() {
	p.stopMaintenance()
	<-p.maintenanceDone
	if p.ownsWatchPool {
		p.watchPool.Close()
	}
}

// Pool returns the underlying PostgreSQL connection pool.
func (p *Persistence) Pool() *pgxpool.Pool {
	return p.pool
}

// SetPolicyManager configures the authorization policy manager used to clean up
// OpenFGA tuples in the same transaction as resource deletions.
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

// TODO: EOL this in favor of setCreateMetadata
func newCreateMetadata(atespace, name string) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{
		Atespace:   atespace,
		Name:       name,
		Uid:        uuid.NewString(),
		Version:    1,
		CreateTime: now,
		UpdateTime: now,
	}
}

func setCreateMetadata(metadata *ateapipb.ResourceMetadata) {
	metadata.Uid = uuid.NewString()
	metadata.Version = 1
	metadata.CreateTime = timestamppb.Now()
	metadata.UpdateTime = metadata.CreateTime
}

// TODO: EOL this in favor of setUpdateMetadata
func newUpdateMetadata(current *ateapipb.ResourceMetadata) *ateapipb.ResourceMetadata {
	metadata := proto.Clone(current).(*ateapipb.ResourceMetadata)
	metadata.Version++
	metadata.UpdateTime = timestamppb.Now()
	return metadata
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

# atepg

The PostgreSQL store. For migrations, follow `docs/dev/postgresql-schema-evolution.md`.

- Keep transactions at PostgreSQL's default `READ COMMITTED`. Writers to a hot `workers` row queue on `SELECT ... FOR UPDATE` instead of failing with SQLSTATE `40001`.
- Let creates rely on primary and foreign keys, and map violations with `isUniqueViolation` and `isForeignKeyViolation`.
- Write each update and delete with one of these patterns, as nearby code does:
  - Single-row update: read on `p.pool`, then `UPDATE ... WHERE uid = ... AND version = ...`. A concurrent write returns `store.ErrVersionConflict`.
  - Single-table delete: check the preconditions in one guarded `DELETE ... RETURNING`, then classify no rows as the other deletes do.
  - Multi-table write, or a delete that checks preconditions in Go: open a `pgx.Tx` and lock the root row first, with `SELECT ... FOR UPDATE` or the `INSERT` or `DELETE` that writes it.
- Lock parents before children: `workers` before `actors`, `worker_assignments` and `worker_outbox`; `atespaces` before `atespace_access_policies`; access policies before `tuple` and `changelog`; `worker_outbox` before `worker_outbox_default`.
- Run `SELECT ... FOR UPDATE` only inside a `pgx.Tx`. On `p.pool` it runs in autocommit, and the lock drops when the statement ends.
- Pass the active `pgx.Tx` to `authz.PolicyManager` methods so tuple writes join the transaction.
- Don't expose `pgx.Tx` on `store.Interface`. Callers that need to serialize operations use `AcquireLease`.
- Test a concurrency change by racing two callers, for example by blocking inside `mutate`, and check the returned `store.Err*`. Wait on `pg_stat_activity` or `pg_blocking_pids`, not on sleeps.

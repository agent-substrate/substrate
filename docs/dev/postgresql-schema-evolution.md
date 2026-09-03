# PostgreSQL schema evolution

`ateapi` applies PostgreSQL migrations before it becomes ready. During a rolling update, the previous binary continues to serve requests while the new binary changes the schema.

Goose commits one migration at a time. A failed migration run can leave any completed migration prefix in place. These rules keep the previous binary safe with every migration prefix.

## Compatibility contract

- Keep every migration prefix compatible with reads and writes from the previous binary.
- Treat every migration file boundary as a durable database state.
- Do not depend on a later migration to make an earlier prefix compatible.
- Let the new binary migrate the schema from the previous release.
- Make the new binary wait for all pending migrations before readiness.
- Use explicit column lists in `SELECT` and `INSERT` statements.

## Schema change rules

Prefer additive changes. Add a table, column, or index before the new binary uses it.

Add a column as nullable or give it a compatible database default. Do not make previous binary inserts fail because they omit a new column.

Do not remove or rename a table or column that the previous binary uses. Do not change its type or meaning in an incompatible way.

Do not tighten a constraint while the previous binary can write a value that violates it. Do not remove a default that the previous binary requires.

If two schema structures hold the same data, keep them consistent while both binaries can write. Backfill existing rows before the new binary requires the new structure.

Do not run a large data backfill during startup. Propose a separate migration process before you add such a change.

## Expand and contract

Use an expand and contract sequence for a schema replacement or removal:

1. In release N, add the new structure and keep the old structure.
2. In release N+1, stop using the old structure.
3. In release N+2, remove the old structure.

This sequence keeps the previous binary compatible during a rollout and a temporary binary rollback. A binary rollback does not roll back the schema.

## Migration file rules

Store migration files in `cmd/ateapi/internal/store/atepg/migrations`.

- Use the next sequential `NNNNNN_name.sql` filename.
- Add exactly one `-- +goose Up` annotation.
- Use SQL migrations only.
- Do not add down migrations.
- Do not use `NO TRANSACTION` or `ENVSUB`.
- Do not add SQL transaction control statements.
- Do not use `IF NOT EXISTS` for a schema change.
- Keep each startup migration short.

Before the first stable v1 release, developers can change or squash migration files. Recreate a development database after its migration history changes.

After the first stable v1 release, do not change or delete a released migration file. Add a new migration to correct it.

Do not edit the `schema_migrations` ledger manually.

## Before you submit

1. Identify the previous binary reads and writes that use the changed objects.
2. Check those operations against every new migration prefix.
3. Use expand and contract when one prefix would break an operation.
4. Add or update a test for the schema behavior.
5. Run the migration verifier and PostgreSQL store tests.

```sh
hack/verify/postgresql-migrations.sh
go test ./cmd/ateapi/internal/store/atepg
```

# MySQL Configuration

## Overview
Substrate can keep its state in MySQL 8.0 or later instead of PostgreSQL. Set `ATE_API_STORE_BACKEND=mysql` (or `--store-backend=mysql` when running `ateapi` directly); the default is `postgres`. The MySQL backend uses only features that PlanetScale's Vitess supports, so it also runs on a PlanetScale database.

There is no bundled MySQL. The operator provides the server and creates the database. `ateapi` creates its tables at startup but never creates the database.

`ateapi` checks the server at startup and refuses to start in these cases:

- `VERSION()` is older than 8.0 or names MariaDB. Vitess reports a version such as `8.0.40-Vitess`, which passes.
- `auto_increment_increment` is not 1, as on multi-primary replication.
- The session `sql_mode` includes neither `STRICT_TRANS_TABLES` nor `STRICT_ALL_TABLES`.
- Substrate tables exist without a `schema_migrations` ledger, as with PostgreSQL.

## Connection strings
Connection strings use the [go-sql-driver/mysql DSN format](https://github.com/go-sql-driver/mysql#dsn-data-source-name) and must name a database:

```
substrate_rw:<password>@tcp(mysql.example.com:3306)/substrate?tls=true
```

`ateapi` always sets `parseTime=true`, `loc=UTC`, `clientFoundRows=true`, and `interpolateParams=true`, turns off `multiStatements`, and sets the session's `transaction_isolation` to `READ-COMMITTED`. These values replace any set in the DSN.

OpenFGA writes `TIMESTAMP` columns with `NOW()`, which follows the session time zone. Run a self-hosted server with a UTC time zone. PlanetScale fixes it to UTC.

`ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING` (or `--mysql-read-write-connection-string`) and `ATE_API_MYSQL_OWNER_CONNECTION_STRING` (or `--mysql-owner-connection-string`) are both required by `ateapi`, as with PostgreSQL. The installer sets the owner DSN to the read/write DSN when you leave it unset. When running `ateapi` directly, pass the DSNs as flags or use `@env` flags to read these environment variables; the installer manifest already uses `@env`.

Startup logs the host, port, database, user, and whether TLS is on for each DSN. It never logs the password.

### Owner and read/write connections
Migrations use the owner connection. Application queries, OpenFGA authorization queries, worker watch polling, and background cleanup use the read/write connection. Both DSNs must name the same database.

The owner user needs DDL privileges on the database (such as `CREATE`, `ALTER`, `INDEX`, and `DROP`) and `SELECT`, `INSERT`, `UPDATE`, and `DELETE`. The read/write user needs only `SELECT`, `INSERT`, `UPDATE`, and `DELETE`. `ateapi` does not switch roles on MySQL, so each connection runs with the privileges of its DSN user.

For a self-managed server:

```sql
CREATE DATABASE substrate;
CREATE USER 'substrate_owner'@'%' IDENTIFIED BY '<owner-password>';
CREATE USER 'substrate_rw'@'%' IDENTIFIED BY '<read-write-password>';
GRANT ALL PRIVILEGES ON substrate.* TO 'substrate_owner'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON substrate.* TO 'substrate_rw'@'%';
```

Every table with text columns declares `utf8mb4`, and Substrate's key columns compare byte for byte. The database default character set does not affect Substrate.

### TLS
Without TLS settings, connections are unencrypted. There are two ways to turn TLS on.

- Add `tls=true` to the DSN. The driver verifies the server certificate against the system roots and the DSN host name.
- Set `ATE_API_MYSQL_TLS_CA_FILE`, `ATE_API_MYSQL_TLS_CERT_FILE`, or `ATE_API_MYSQL_TLS_KEY_FILE` (or the matching `--mysql-tls-*-file` flags). Setting any of them turns on TLS with that material and replaces the DSN's `tls` setting.

With the TLS files, an empty CA file uses the system roots. The server certificate must always match the DSN host name, even with a CA file. That is stricter than PostgreSQL's `sslmode=verify-ca`, which checks only the certificate chain.

The client certificate and key must be set together; both may name the same file. `ateapi` reads the files again for every new connection, so rotated certificates apply without a restart. Open connections keep the certificate they started with.

The TLS files always require TLS. A `tls=preferred` setting in the DSN does not let the driver fall back to an unencrypted connection when the files are set.

### Pool sizing
`ATE_API_MYSQL_POOL_MAX_CONNS` (or `--mysql-pool-max-conns`) caps the read/write pool, which Substrate and OpenFGA share. It must be a positive integer. When unset, the limit is the larger of 4 and the CPU count, the same default the PostgreSQL pool uses.

Connections are replaced after 1 hour, or after 30 minutes idle, as with PostgreSQL.

The owner pool is capped at 2 connections and keeps none idle. The watch pool is capped at 3 connections and uses the read/write DSN.

Each `ateapi` replica can therefore open at most the read/write limit plus 5 connections. Set the limit so that every replica fits within the server's connection limit.

## Startup and migrations
`ateapi` applies pending migrations before it becomes ready. Replicas serialize migration runs with a named lock (`GET_LOCK`) scoped to the database and wait up to 5 minutes for another replica's run. `ateapi` retries the initial connection while the database is unreachable.

MySQL commits each DDL statement on its own, so a migration file is not atomic. If a migration fails partway through a file, the statements before the failure stay applied and the ledger does not record the file. Each statement in the current migrations is idempotent (`CREATE TABLE IF NOT EXISTS` and seed rows that tolerate a rerun), so restarting `ateapi` completes the file.

Do not edit the `schema_migrations` ledger manually.

## PlanetScale
Use a PlanetScale database on Vitess. Create the database in PlanetScale first, and name it in both DSNs.

**Keyspace.** Substrate targets an unsharded keyspace. It defines no VSchema or vindexes, and its transactions write several tables that must commit together. Vitess commits multi-shard transactions on a best-effort basis by default.

Vitess also routes `GET_LOCK` to the first shard of the first keyspace.

**Passwords.** PlanetScale grants access through password roles. The owner DSN needs an `admin` password, since only that role can change the schema. The read/write DSN can use a `readwriter` password.

**Safe migrations.** A branch with safe migrations enabled rejects direct DDL. `ateapi` runs DDL at startup whenever a migration is pending, including the first install. Keep safe migrations off on the branch that `ateapi` uses.

Do not apply Substrate migrations through a deploy request. The ledger would not record them, and `ateapi` would try to apply them again.

**Locks and limits.** The migration lock and the OpenFGA setup lock use `GET_LOCK` at startup, and both lock names are scoped to the database. Vitess keeps a session that took a lock on a reserved connection until it disconnects, so `ateapi` closes each lock connection after unlocking. The OpenFGA setup lock waits until it is free, as PostgreSQL's advisory lock does.

PlanetScale ends a transaction after 20 seconds, which `ateapi` reports as an internal error rather than a version conflict. Count each replica as the read/write pool size plus 5 connections against the database's connection limit.

**Foreign keys.** Substrate declares no foreign keys, so the PlanetScale foreign key setting does not affect it.

**TLS.** PlanetScale server certificates are signed by a common system root. Use `tls=true` in the DSN and leave the CA file unset.

CI runs the store contract suite and the outbox rollback probe through `vitess/vttestserver` with foreign keys disallowed (`go test -tags vitess -run TestVitess ./cmd/ateapi/internal/store/atemy/`).

See PlanetScale's documentation on [safe migrations](https://planetscale.com/docs/vitess/schema-changes/safe-migrations), [password roles](https://planetscale.com/docs/vitess/security/password-roles), [secure connections](https://planetscale.com/docs/vitess/connecting/secure-connections), and [system limits](https://planetscale.com/docs/vitess/planetscale-system-limits), and the Vitess [locking functions](https://vitess.io/docs/reference/query-serving/locking-functions) reference.

## Installer configuration
`ate-setup` and `hack/install-ate.sh` read these variables:

```sh
export ATE_API_STORE_BACKEND=mysql
export ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING='substrate_rw:<password>@tcp(mysql.example.com:3306)/substrate?tls=true'
export ATE_API_MYSQL_OWNER_CONNECTION_STRING='substrate_owner:<password>@tcp(mysql.example.com:3306)/substrate?tls=true'
export ATE_API_MYSQL_POOL_MAX_CONNS=20
./hack/install-ate.sh --deploy-ate-system
```

The installer skips the bundled PostgreSQL. It stores the DSNs and the backend choice in the `ate-api-server-secret-envvars` Secret. The pool size and the TLS file paths go in the `ate-api-server-envvars` ConfigMap.

`ATE_API_MYSQL_SERVER_CA_FILE` names a local PEM file with the server CA. The installer publishes it as the `mysql-server-ca` Secret, which `ate-api-server` mounts at `/run/mysql-server-ca/server-ca.pem`, and sets `ATE_API_MYSQL_TLS_CA_FILE` to that path.

`ATE_API_MYSQL_TLS_CA_FILE`, `ATE_API_MYSQL_TLS_CERT_FILE`, and `ATE_API_MYSQL_TLS_KEY_FILE` name files already inside the `ate-api-server` pod. The installer passes them through unchanged, in the role of the `sslrootcert`, `sslcert`, and `sslkey` paths a PostgreSQL DSN names. Set the certificate and key together, and set `ATE_API_MYSQL_SERVER_CA_FILE` or `ATE_API_MYSQL_TLS_CA_FILE`, not both.

For example, point the certificate and key at the pod identity bundle, `/run/podidentity.podcert.ate.dev/credential-bundle.pem`, for a server that trusts the pod identity CA.

Each backend rejects the other's non-empty variables; exported empty ones are allowed. The Cloud SQL helpers in `tools/setup-gcp` support PostgreSQL only.

A redeploy with `ATE_API_STORE_BACKEND` unset fails on a cluster that records `mysql`; set the variable again on every deploy. See [`cmd/ate-setup/differences.md`](../cmd/ate-setup/differences.md) for details.

## Differences from PostgreSQL
These differences follow from MySQL and Vitess features, and they affect operation.

- **Worker outbox.** Each worker write takes an `AUTO_INCREMENT` seq in its last statement, so worker writes do not wait on each other, as on PostgreSQL. A seq is assigned before its write commits, so a watch can see a later seq first. The watch delivers rows past the gap and keeps each skipped seq pending, delivering it when it commits.
- **Pending seqs.** A seq pending for more than 2 seconds is probed with a `FOR SHARE NOWAIT` read. The watch drops it only when the probe finds no row and no writer holding it, which means the write rolled back.
- **Event order.** Events for one worker arrive in order, because writes to one worker take its row lock in turn. Events for different workers can arrive out of seq order.
- **Subscribe window.** A new watch tracks seqs still committing among rows written in the last 5 minutes. A write that took its seq earlier than that and commits after the watch starts is not delivered to that watch.
- **Outbox durability.** The MySQL outbox table is durable, so a restart of the same server loses no events. PostgreSQL uses unlogged partitions and resyncs watchers after a restart.
- **Outbox retention.** Background cleanup deletes the outbox rows older than 15 minutes that it read, in batches. The replica that locks the trim row with `SKIP LOCKED` runs the pass and the others skip it, as with PostgreSQL's elected retention. PostgreSQL drops whole partitions.
- **Parent and child rows.** There are no foreign keys. A transaction that writes a child row takes a shared lock on its parent, and a parent delete takes an exclusive lock first.
- **Leases.** Acquiring a lease takes over an expired row with an `UPDATE`, then inserts a new row if none exists. PostgreSQL uses one conditional upsert.
- **Transactions.** As on PostgreSQL, transactions run at `READ COMMITTED`. InnoDB can pick a transaction as a deadlock victim even when two writes only race to insert the same key, so the store runs it again, up to 3 attempts in all. A deadlock that persists returns a version conflict, which callers may retry.
- **Lock wait timeouts.** A lock wait timeout returns the database error as is, as a lock wait that hits a deadline does on PostgreSQL.
- **Watch resync.** A watch closes so its consumer resyncs from the primary tables when the server changes (`@@server_uuid`, as after a failover), or when the outbox moves behind its cursor (lost writes). It also closes when it falls behind retention, when retention deletes a row it was waiting for, or when it has more than 4096 pending seqs.
- **Policy members.** OpenFGA's MySQL schema holds a tuple user in 256 characters, so the MySQL store rejects an access policy member longer than that once encoded, as an invalid argument. PostgreSQL accepts members up to the API limit of 512 characters.
- **Migrations.** A failed migration file can be partly applied. The current files are idempotent, so a restart completes them. See [Startup and migrations](#startup-and-migrations).

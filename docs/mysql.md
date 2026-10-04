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

When the installer defaults the owner DSN to the read/write DSN, the read/write user runs migrations and needs the owner privileges.

For a self-managed server:

```sql
CREATE DATABASE substrate;
CREATE USER 'substrate_owner'@'%' IDENTIFIED BY '<owner-password>';
CREATE USER 'substrate_rw'@'%' IDENTIFIED BY '<read-write-password>';
GRANT ALL PRIVILEGES ON substrate.* TO 'substrate_owner'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON substrate.* TO 'substrate_rw'@'%';
```

Every table with text columns declares `utf8mb4`, and Substrate's key columns compare byte for byte. The database default character set does not affect Substrate.

## TLS
Without TLS settings, connections are unencrypted. There are two ways to turn TLS on.

- Add `tls=true` to the DSN. The driver verifies the server certificate against the system roots and the DSN host name.
- Set `ATE_API_MYSQL_TLS_CA_FILE`, the client certificate and key (`ATE_API_MYSQL_TLS_CERT_FILE` and `ATE_API_MYSQL_TLS_KEY_FILE`), or all three. The matching `--mysql-tls-*-file` flags work too. These settings turn on TLS with that material and replace the DSN's `tls` setting.

When the CA file is unset, the system roots verify the server certificate. The server certificate must always match the DSN host name, even with a CA file. That is stricter than PostgreSQL's `sslmode=verify-ca`, which checks only the certificate chain.

The client certificate and key must be set together, and `ateapi` refuses to start with only one. Both may name the same file.

`ateapi` reads the files again for every new connection, so rotated certificates apply without a restart. Open connections keep the certificate they started with.

The TLS files always require TLS. A `tls=preferred` setting in the DSN does not let the driver fall back to an unencrypted connection when the files are set.

## Pool sizing
The read/write pool, which Substrate and OpenFGA share, holds up to the larger of 4 and the CPU count. Connections are replaced after 1 hour, or after 30 minutes idle, as with PostgreSQL.

The owner pool is capped at 2 connections and keeps none idle. The watch pool is capped at 3 connections and uses the read/write DSN.

Each `ateapi` replica can therefore open at most the read/write limit plus 5 connections. Set the limit so that every replica fits within the server's connection limit.

## Startup and migrations
`ateapi` applies pending migrations before it becomes ready. Replicas serialize migration runs with a named lock (`GET_LOCK`) scoped to the database and wait up to 5 minutes for another replica's run. `ateapi` retries the initial connection while the database is unreachable.

The next startup reruns a migration file that failed partway through. See [Schema evolution](dev/schema-evolution.md#mysql-migrations) for the migration rules.

## PlanetScale
Use a PlanetScale database on Vitess. Create the database in PlanetScale first, and name it in both DSNs.

**Keyspace.** Substrate targets an unsharded keyspace. It defines no VSchema or vindexes, and its transactions write several tables that must commit together. Vitess commits multi-shard transactions on a best-effort basis by default.

**Passwords.** PlanetScale grants access through password roles. The owner DSN needs an `admin` password, since only that role can change the schema. The read/write DSN can use a `readwriter` password.

**Safe migrations.** A branch with safe migrations enabled rejects direct DDL. `ateapi` runs DDL at startup whenever a migration is pending, including the first install. Keep safe migrations off on the branch that `ateapi` uses.

Do not apply Substrate migrations through a deploy request. The ledger would not record them, and `ateapi` would try to apply them again.

**Locks and limits.** The migration lock and the OpenFGA setup lock use `GET_LOCK` at startup. Vitess keeps a session that took a lock on a reserved connection until it disconnects, so `ateapi` closes each lock connection after unlocking. The OpenFGA setup lock waits until it is free, as PostgreSQL's advisory lock does.

PlanetScale ends a transaction after 20 seconds, which `ateapi` reports as an internal error rather than a version conflict. Size the pool against the database's connection limit as described in [Pool sizing](#pool-sizing).

**Foreign keys.** Substrate declares no foreign keys, so the PlanetScale foreign key setting does not affect it.

**TLS.** PlanetScale server certificates are signed by a common system root. Use `tls=true` in the DSN and leave the CA file unset.

See PlanetScale's documentation on [safe migrations](https://planetscale.com/docs/vitess/schema-changes/safe-migrations), [password roles](https://planetscale.com/docs/vitess/security/password-roles), [secure connections](https://planetscale.com/docs/vitess/connecting/secure-connections), and [system limits](https://planetscale.com/docs/vitess/planetscale-system-limits), and the Vitess [locking functions](https://vitess.io/docs/reference/query-serving/locking-functions) reference.

## Installer configuration
`ate-setup` and `hack/install-ate.sh` read these variables:

```sh
export ATE_API_STORE_BACKEND=mysql
export ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING='substrate_rw:<password>@tcp(mysql.example.com:3306)/substrate?tls=true'
export ATE_API_MYSQL_OWNER_CONNECTION_STRING='substrate_owner:<password>@tcp(mysql.example.com:3306)/substrate?tls=true'
./hack/install-ate.sh --deploy-ate-system
```

`hack/install-ate.sh --help` lists the TLS variables. See [`cmd/ate-setup/differences.md`](../cmd/ate-setup/differences.md#known-differences-worth-flagging) for how the installer handles the server CA, the backend conflict check, and a redeploy with `ATE_API_STORE_BACKEND` unset. The Cloud SQL helpers in `tools/setup-gcp` support PostgreSQL only.

## Differences from PostgreSQL
These differences follow from MySQL and Vitess features, and they affect operation.

- **Worker outbox.** Each worker write takes an `AUTO_INCREMENT` seq in its last statement, so worker writes do not wait on each other. A seq is assigned before its write commits, so a watch can see a later seq first.
- **Pending seqs.** The watch delivers rows past a gap and keeps each skipped seq pending until it commits. A seq pending for more than 2 seconds is probed with a `FOR SHARE NOWAIT` read. The watch drops it only when the probe finds no row and no writer holding it, which means the write rolled back.
- **Event order.** Events for one worker arrive in order, because writes to one worker take its row lock in turn. Events for different workers can arrive out of seq order.
- **Subscribe window.** A new watch tracks seqs still committing among rows written in the last 5 minutes. A write that took its seq earlier than that and commits after the watch starts is not delivered to that watch.
- **Outbox durability.** The MySQL outbox table is durable, so a restart of the same server loses no events. This assumes `innodb_flush_log_at_trx_commit=1`, the default. PostgreSQL uses unlogged partitions and resyncs watchers after a restart.
- **Outbox retention.** Background cleanup deletes the outbox rows older than 15 minutes that it read, in batches. The replica that locks the trim row with `SKIP LOCKED` runs the pass and the others skip it, as with PostgreSQL's elected retention. PostgreSQL drops whole partitions.
- **Parent and child rows.** There are no foreign keys. A transaction that writes a child row takes a shared lock on its parent, and a parent delete takes an exclusive lock first.
- **Leases.** Acquiring a lease takes over an expired row with an `UPDATE`, then inserts a new row if none exists. PostgreSQL uses one conditional upsert. Releasing a lease expires its row instead of deleting it, because inserters racing for a deleted key deadlock in InnoDB.
- **Transactions.** Transactions run at `READ COMMITTED`. InnoDB can pick a transaction as a deadlock victim even when two writes only race to insert the same key, so the store runs it again, up to 5 more times.
- **Deadlock retries.** The retries back off exponentially from 10ms with jitter, about 0.3 to 0.6 seconds in all, which gives the winning transaction time to commit. A deadlock that persists returns a version conflict, which callers may retry.
- **Lock wait timeouts.** A lock wait timeout returns the database error as is, as a lock wait that hits a deadline does on PostgreSQL.
- **Watch resync.** A watch closes so its consumer resyncs from the primary tables when the server changes (`@@server_uuid`, as after a failover), or when the outbox moves behind its cursor (lost writes). It also closes when it falls behind retention, when retention deletes a row it was waiting for, or when it has more than 4096 pending seqs.
- **Policy members.** OpenFGA's MySQL schema holds a tuple user in 256 characters, so the MySQL store rejects an access policy member longer than that once encoded, as an invalid argument. PostgreSQL accepts members up to the API limit of 512 characters.

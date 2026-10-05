-- Postgres bootstrap for Agent Substrate on RDS.
--
-- Run once, as the RDS master user, after `setup-aws.sh rds` completes:
--
--   psql "postgres://substrate_admin:<password>@<rds-endpoint>:5432/substrate?sslmode=verify-full" \
--     -f tools/setup-aws/postgres-bootstrap.sql
--
-- The master password is whatever you set via RDS_MASTER_PASSWORD. If you'd
-- like verify-full, download the RDS CA bundle:
--   https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

-- Two NOLOGIN roles so schema ownership and R/W access are decoupled from the
-- login principal. ate-api-server runs as substrate_readwrite.
CREATE ROLE substrate_owner NOLOGIN;
CREATE ROLE substrate_readwrite NOLOGIN;

-- RDS Postgres gives the master user `rds_superuser`, not true superuser —
-- which means CREATE SCHEMA ... AUTHORIZATION substrate_owner fails with
-- "must be able to SET ROLE substrate_owner" unless the master is a member
-- of that role. Grant it membership so the next statement works.
GRANT substrate_owner TO CURRENT_USER;

-- Schema that ate-api-server reads/writes (matches ATE_API_POSTGRES_SCHEMA
-- default = 'substrate'). Owned by substrate_owner so DDL is kept out of the
-- app's hands.
CREATE SCHEMA substrate AUTHORIZATION substrate_owner;

-- Default privileges: anything substrate_owner creates in this schema is
-- automatically accessible to substrate_readwrite without further GRANTs.
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO substrate_readwrite;
ALTER DEFAULT PRIVILEGES FOR ROLE substrate_owner IN SCHEMA substrate
  GRANT USAGE, SELECT ON SEQUENCES TO substrate_readwrite;
GRANT USAGE ON SCHEMA substrate TO substrate_readwrite;

-- ---------------------------------------------------------------------------
-- Choose ONE login path for ate-api-server:
-- ---------------------------------------------------------------------------

-- (A) Password login. Simplest; works with the DSN that
--     `setup-aws.sh summary` prints out of the box.
--     The DSN uses this role directly as the login principal.
ALTER ROLE substrate_readwrite WITH LOGIN PASSWORD 'CHANGE_ME';
-- Then set:
--   ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING=
--     postgres://substrate_readwrite:CHANGE_ME@<endpoint>:5432/substrate?sslmode=verify-full

-- (B) IAM-authenticated login. More secure; requires that the pod mint a short-
--     lived token via `aws rds generate-db-auth-token` and inject it as the
--     password at connect time. This is the AWS analog of Cloud SQL's
--     `--auto-iam-authn`, but there is no stock proxy — expect to deploy a
--     tiny sidecar that refreshes the token every ~10 minutes.
--
--     Uncomment to use:
-- CREATE USER ate_api_server;              -- IAM user name = DB user name
-- GRANT rds_iam TO ate_api_server;         -- RDS magic role
-- GRANT substrate_readwrite TO ate_api_server;
--     Then the IRSA role (`<CLUSTER_NAME>-ate-api-server`, already attached
--     by `setup-aws.sh rds-grant`) authorizes `rds-db:connect` on the
--     `dbuser:<DbiResourceId>/ate_api_server` ARN.

-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
-- \du           -- roles should list substrate_owner, substrate_readwrite, (ate_api_server)
-- \dn           -- schemas should list 'substrate' owned by substrate_owner

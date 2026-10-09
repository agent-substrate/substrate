# Installing Agent Substrate as an operator

Every setting the installer reads can be supplied three ways: a flag, an
environment variable, or a configuration document. This page describes the
document, because it is the only one of the three an operator owns as a file,
reviews in a change, and reuses across runs.

## The document

```yaml
apiVersion: install.ate.dev/v1alpha1
kind: SubstrateInstall

namespace: ate-system
atenet:
  dataplane: envoy
images:
  repo: ghcr.io/agent-substrate/substrate
  tag: v0.4.1
```

Pass it with `--config`, or name it in `ATE_CONFIG`:

```bash
ate-setup deploy ate-system --config substrate-install.yaml
ATE_CONFIG=substrate-install.yaml hack/install-ate.sh --deploy-ate-system
```

`ATE_CONFIG` is how the document reaches the shell installer, which forwards
only a fixed set of flags and otherwise passes settings by inheriting the
environment.

A worked example is at [docs/examples/substrate-install.yaml](examples/substrate-install.yaml).

### Keys are nested; the table below is flat

A key written `ateapi.postgres.schema` is nested in the document:

```yaml
ateapi:
  postgres:
    schema: public
```

An unrecognized key is rejected rather than ignored, naming the file:

```
Error: install.yaml: unknown setting "ateapi.postgres.schemas"
```

A typo would otherwise leave the install running on the default with nothing
on screen to say so.

## Precedence

Lowest to highest:

```
default  <  environment  <  configuration file  <  flag
```

The file outranks the environment deliberately: the file is the artifact an
operator wrote and can review, and ambient state left in a shell must not
silently outrank it. A flag outranks both because it was typed for this run.

A channel that does not mention a setting leaves the one below it in place, so
a document need only name what differs from the defaults.

## Empty values

A setting a channel mentions is set, whatever it is set to. An empty value is
a value:

```yaml
namespace: ""
```

```bash
export ATE_NAMESPACE=      # the same thing
ate-setup deploy ate-system --namespace=
```

All three clear the setting rather than falling through to the default. That
is how a setting is removed from an installation: write its zero value and
re-apply.

Only an *absent* setting falls through. In the shell that distinction is the
difference between `export FOO=` and `unset FOO`, and the two do not mean the
same thing here:

| Shell                           | Meaning                          |
|---------------------------------|----------------------------------|
| `unset ATE_NAMESPACE`           | not set; the layer below applies |
| `export ATE_NAMESPACE=`         | set to empty                     |
| `export ATE_NAMESPACE=ate-prod` | set to `ate-prod`                |

A script that exports an empty variable to mean "unset" is configuring the
setting. The report names the channel each value came from, so when that
happens it is visible rather than mysterious.

For a setting that is not a string, an empty value is not a value of its kind
and is reported as such -- `ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER=` is
rejected, naming the variable, rather than quietly ignored.

## Every run reports what it resolved

Every setting is listed with the channel that supplied it:

```
configuration: 6 set, 35 default
  ateapi.expectedJWTIssuer                    ""            (from default)
  ateapi.postgres.readWrite.connectionString  <set>         (from ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING)
  ...
  atenet.dataplane                            agentgateway  (from ATE_ATENET_DATAPLANE)
  benchmark.sandboxClass                      microvm       (from --sandbox-class)
  benchmark.workerCount                       7             (from config.benchmark.workerCount)
  clusterSize                                 size0         (from default)
  context                                     kind-kind     (from the --kind profile)
  namespace                                   ate-prod      (from config.namespace)
```

An install that succeeds with the wrong value never produces an error, so this
report is the only place an inherited variable or a forgotten file becomes
visible -- and a value that came from a default is worth seeing for the same
reason, because next release it may be a different default.

A setting marked secret in the table below prints as `<set>` when something
supplied it, with the channel still named so you know where to change it. One
nothing supplied prints its default, which is empty: no credential appears
either way.

To list only what was set:

```bash
ate-setup deploy ate-system --no-report-defaults
export ATE_NO_REPORT_DEFAULTS=1          # the same, for a shell that runs it repeatedly
```

```
configuration: 6 set, 35 default
  ateapi.postgres.readWrite.connectionString  <set>         (from ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING)
  atenet.dataplane                            agentgateway  (from ATE_ATENET_DATAPLANE)
  benchmark.sandboxClass                      microvm       (from --sandbox-class)
  benchmark.workerCount                       7             (from config.benchmark.workerCount)
  context                                     kind-kind     (from the --kind profile)
  namespace                                   ate-prod      (from config.namespace)
```

## Errors name the channel

```
Error: atenet.dataplane must be envoy or agentgateway, got "nginx" (from config.atenet.dataplane)
Error: at least one of ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING or
       config.ateapi.postgres.readWrite.connectionString must be set, got none
```

## Command-scoped settings

Most settings apply to every command and are flags on the root. A few belong
to one command and are flags only there — but they are still reachable from
the environment and the file, so one document can describe a whole install:

```yaml
csi:
  setup: nfs
benchmark:
  workerCount: 4
```

## Install runs record what they used

A `deploy` command that installs Substrate writes the settings it resolved to
a file, so a later run can reproduce it without anyone remembering what was
typed. `deploy demo` and `deploy benchmarks` write nothing: they deploy on top
of the system with only their own settings.

```
recorded this configuration in ~/.cache/ate-setup/installs/prod.yaml
```

Records go under the user cache directory, or wherever `record.dir` points.
Nothing reads them on its own -- a record only takes effect when you pass it
to `--config`.

| Outcome   | Path                                             | Retention                                                  |
|-----------|--------------------------------------------------|------------------------------------------------------------|
| Succeeded | `<record.dir>/installs/<context>.yaml`           | replaced each run: a cluster has one current configuration |
| Failed    | `<record.dir>/failed/<context>-<timestamp>.yaml` | kept: each attempt is its own artifact                     |

### Retrying a failed install

A run that fails prints the record it wrote and the command that replays it:

```
Error: while reading kubeconfig: context "prod" does not exist
the settings this run used were written to ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
retry with: ate-setup deploy atenet --config ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
1 secret setting(s) were not written and must be supplied again: ateapi.postgres.readWrite.connectionString
```

The record is ordinary `--config` input. Nothing has to be stripped from it
first: the `cluster:` block holding the timestamp and outcome is skipped when
the file is read back.

**A retry is not a complete replay.** Secrets are never written, so the last
line above lists what to supply again:

```bash
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgres://...'
ate-setup deploy atenet --config ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
```

A failed run changes nothing about later runs. Its record is inert until you
name it -- a value that caused the failure must not become the default that
reaches the cluster on the next success.

### Reusing a successful install

The same applies to the success record, which is the one to keep:

```bash
# Rebuild a cluster the way it was, or set up a second one to match.
ate-setup deploy ate-system --config ~/.cache/ate-setup/installs/prod.yaml
```

Point `record.dir` at a checkout to keep these under version control, which is
how several operators share one configuration and how an install survives a
cluster that no longer exists:

```bash
export ATE_RECORD_DIR=~/ops/substrate-installs
```

Records hold no credentials, so they are safe to commit. They are written
readable only by you regardless.

### What a record looks like

```yaml
apiVersion: install.ate.dev/v1alpha1
kind: SubstrateInstall
cluster:
  context: prod
  outcome: succeeded
  substrateVersion: v0.4.1
  writtenAt: "2026-09-29T23:14:38Z"
  writtenBy: ate-setup v0.4.1
atenet:
  dataplane: agentgateway
context: prod
namespace: ate-prod
```

`cluster.substrateVersion` is the Substrate release the run installed: the
version stamped into the binaries it built, or the image tag of a prebuilt
install. `cluster.writtenBy` is the `ate-setup` that wrote the file.

Only settings something supplied are recorded. A setting left on its default
is left out on purpose, so it keeps following the default if a later release
changes it.

## Settings

Defaults are also shown by `ate-setup --help` and by `--help` on the owning
command. A setting with no default is unset unless something supplies it.

| Key                                          | Environment variable                            | Flag                                               | Default               | Applies to                               |
|----------------------------------------------|-------------------------------------------------|----------------------------------------------------|-----------------------|------------------------------------------|
| `actorJWT.algorithm`                         | `ACTOR_JWT_ALGORITHM`                           | `--actor-jwt-algorithm`                            | `ES256`               | (global)                                 |
| `ateapi.expectedJWTIssuer`                   | `EXPECTED_JWT_ISSUER`                           | `--ateapi-expected-jwt-issuer`                     | —                     | (global)                                 |
| `ateapi.postgres.cloudsql.gsa`               | `ATE_API_POSTGRES_CLOUDSQL_GSA`                 | `--ateapi-postgres-cloudsql-gsa`                   | —                     | (global)                                 |
| `ateapi.postgres.cloudsql.iamAuth`           | `ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH`            | `--ateapi-postgres-cloudsql-iam-auth`              | —                     | (global)                                 |
| `ateapi.postgres.cloudsql.instance`          | `ATE_API_POSTGRES_CLOUDSQL_INSTANCE`            | `--ateapi-postgres-cloudsql-instance`              | —                     | (global)                                 |
| `ateapi.postgres.cloudsql.ipType`            | `ATE_API_POSTGRES_CLOUDSQL_IP_TYPE`             | `--ateapi-postgres-cloudsql-ip-type`               | —                     | (global)                                 |
| `ateapi.postgres.owner.connectionString`     | `ATE_API_POSTGRES_OWNER_CONNECTION_STRING`      | — (no flag)                                        | — (secret)            | (global)                                 |
| `ateapi.postgres.owner.role`                 | `ATE_API_POSTGRES_OWNER_ROLE`                   | `--ateapi-postgres-owner-role`                     | `substrate_owner`     | (global)                                 |
| `ateapi.postgres.poolMaxConns`               | `ATE_API_POSTGRES_POOL_MAX_CONNS`               | `--ateapi-postgres-pool-max-conns`                 | —                     | (global)                                 |
| `ateapi.postgres.readWrite.connectionString` | `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` | — (no flag)                                        | — (secret)            | (global)                                 |
| `ateapi.postgres.readWrite.role`             | `ATE_API_POSTGRES_READ_WRITE_ROLE`              | `--ateapi-postgres-read-write-role`                | `substrate_readwrite` | (global)                                 |
| `ateapi.postgres.schema`                     | `ATE_API_POSTGRES_SCHEMA`                       | `--ateapi-postgres-schema`                         | —                     | (global)                                 |
| `ateapi.postgres.serverCAFile`               | `ATE_API_POSTGRES_SERVER_CA_FILE`               | `--ateapi-postgres-server-ca-file`                 | —                     | (global)                                 |
| `ateapi.postgres.storageClass`               | `ATE_API_POSTGRES_STORAGE_CLASS`                | `--ateapi-postgres-storage-class`                  | —                     | (global)                                 |
| `atenet.dataplane`                           | `ATE_ATENET_DATAPLANE`                          | `--atenet-dataplane`                               | `envoy`               | (global)                                 |
| `atenet.egress.additionalExtprocService`     | `ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE`         | `--experimental-additional-egress-extproc-service` | —                     | (global)                                 |
| `atenet.egress.credentialProvider`           | `ATE_CREDENTIAL_PROVIDER`                       | `--credential-provider`                            | —                     | (global)                                 |
| `benchmark.actorMemory`                      | `BENCHMARK_ACTOR_MEMORY`                        | `--benchmark-actor-memory`                         | —                     | (global)                                 |
| `benchmark.sandboxClass`                     | `ATE_BENCHMARK_SANDBOX_CLASS`                   | `--sandbox-class`                                  | `gvisor`              | `deploy benchmarks`, `delete benchmarks` |
| `benchmark.workerCount`                      | `ATE_BENCHMARK_WORKER_COUNT`                    | `--worker-count`                                   | `1`                   | `deploy benchmarks`, `delete benchmarks` |
| `clusterSize`                                | `ATE_INSTALL_CLUSTER_SIZE`                      | `--cluster-size`                                   | `size0`               | (global)                                 |
| `context`                                    | `KUBECTL_CONTEXT`                               | `--context`                                        | —                     | (global)                                 |
| `cordonControlPlane`                         | `ATE_INSTALL_CORDON_CONTROL_PLANE`              | `--cordon-control-plane`                           | `false`               | (global)                                 |
| `csi.setup`                                  | `SETUP_CSI`                                     | `--setup-csi`                                      | `none`                | `deploy ate-system`                      |
| `demo.anthropicAPIKey`                       | `ANTHROPIC_API_KEY`                             | — (no flag)                                        | — (secret)            | (global)                                 |
| `demo.counter.storageClass`                  | `STORAGE_CLASS`                                 | `--storage-class`                                  | `standard`            | `deploy demo counter`                    |
| `demo.counter.withExternalVolume`            | `ATE_DEMO_COUNTER_WITH_EXTERNAL_VOLUME`         | `--with-external-volume`                           | `false`               | `deploy demo counter`                    |
| `docker.buildFlags`                          | `DOCKER_BUILD_FLAGS`                            | `--docker-build-flags`                             | —                     | (global)                                 |
| `e2e.envoyConcurrency`                       | `E2E_ENVOY_CONCURRENCY`                         | `--e2e-envoy-concurrency`                          | —                     | (global)                                 |
| `gcp.clusterLocation`                        | `CLUSTER_LOCATION`                              | `--gcp-cluster-location`                           | —                     | (global)                                 |
| `gcp.clusterName`                            | `CLUSTER_NAME`                                  | `--gcp-cluster-name`                               | —                     | (global)                                 |
| `gcp.projectID`                              | `PROJECT_ID`                                    | `--gcp-project-id`                                 | —                     | (global)                                 |
| `images.repo`                                | `ATE_IMAGE_REPO`                                | `--image-repo`                                     | —                     | (global)                                 |
| `images.tag`                                 | `ATE_IMAGE_TAG`                                 | `--image-tag`                                      | —                     | (global)                                 |
| `kindCluster.enabled`                        | `ATE_INSTALL_KIND`                              | `--kind`                                           | `false`               | (global)                                 |
| `kindCluster.name`                           | `KIND_CLUSTER_NAME`                             | `--kind-cluster-name`                              | `kind`                | (global)                                 |
| `ko.defaultPlatforms`                        | `KO_DEFAULTPLATFORMS`                           | `--ko-default-platforms`                           | —                     | (global)                                 |
| `ko.dockerRepo`                              | `KO_DOCKER_REPO`                                | `--ko-docker-repo`                                 | —                     | (global)                                 |
| `kubeconfig`                                 | `KUBECONFIG`                                    | `--kubeconfig`                                     | —                     | (global)                                 |
| `namespace`                                  | `ATE_NAMESPACE`                                 | `--namespace`                                      | `ate-system`          | (global)                                 |
| `otlpEndpoint`                               | `ATE_OTLP_ENDPOINT`                             | `--otlp-endpoint`                                  | —                     | (global)                                 |
| `podcert.workersPerSigner`                   | `ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER`        | `--podcert-workers-per-signer`                     | `0`                   | (global)                                 |
| `record.dir`                                 | `ATE_RECORD_DIR`                                | `--record-dir`                                     | —                     | (global)                                 |
| `rolloutTimeout`                             | `ATE_INSTALL_ROLLOUT_TIMEOUT`                   | `--rollout-timeout`                                | `1m0s`                | (global)                                 |
| `storage.bucketName`                         | `BUCKET_NAME`                                   | `--storage-bucket-name`                            | —                     | (global)                                 |

## Secrets

Credentials should come from the environment, not from a document that gets
committed:

```bash
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgres://...'
ate-setup deploy ate-system --config substrate-install.yaml
```

These settings have no flag. A command line is visible in `ps` to every user
on the host and is written to shell history, so a credential is supplied
through the environment or through a file you can permission.

The run report prints `<set>` for these rather than the value.

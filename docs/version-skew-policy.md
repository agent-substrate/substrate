# Version skew policy

The [compatibility policy](https://docs.google.com/document/d/1obSIvfcafLNniLYTQCcT2eCgxHqa2AQ3Ga7YTsju49s/edit?usp=sharing&resourcekey=0-5VqJ13Vj1QctB7SnnfmtkQ) says what may change between two releases.

<!-- TODO: Link the compatibility policy in the repository once it is checked in. -->

This document says which releases of the substrate components may run together, and the order in which they are upgraded so that only supported combinations occur. The [upgrade runbook](upgrade.md) implements it and may add restrictions of its own.

## Scope

Two releases are compatible when every change between them is compatible under the compatibility policy. This document covers moving an installation between two compatible releases, and the mixture of components that exists while that happens. Between incompatible releases, reinstall.

This policy applies from 1.0.0. From 1.0.0, the patch and minor releases of one major are compatible.

Below, N and N+1 are two consecutive compatible releases. All components of a release are built from one commit and carry one version string.

Patch releases add no API fields, public or internal, and no schema migrations.

## Components

* `ate-api-server`: serves the public API, calls `atelet`, talks to the database.
* `ate-idp-server`: publishes the keys that verify actor JWTs. It shares only the actor JWT key pool with `ate-api-server`, and is upgraded with it.
* `ate-controller`: renders WorkerPools into Deployments, registers workers with `ate-api-server`.
* `atelet`: one pod per node (DaemonSet), drives the `ateworker` processes on its node.
* Workers: the pods of a WorkerPool, running `ateworker` and the actors.
* `atenet`: router and egress gateway.
* `podcertificate-controller`: signs the certificates components present to each other.
* Clients: `kubectl-ate` and anything else using the public API.

See more in [architecture.md](architecture.md).

## Supported version skew

The components are ordered: `podcertificate-controller`, `atelet`, workers, `ate-api-server`, `ate-controller`, `atenet`, clients.

* A component may be one release newer than any component after it in this order.
* A component must not be newer than any component before it.
* While a component rolls, its own replicas, nodes or pods are at N and N+1.
  * The API schema is not shared. Until every `ate-api-server` replica is at N+1, a request can reach an N replica, which rejects fields new in N+1. So clients must not use fields new in N+1 until `ate-api-server` finishes rolling.
  * The database schema is shared. The first N+1 `ate-api-server` replica migrates it, so `ate-api-server` replicas at N and N+1 use the N+1 database schema while `ate-api-server` rolls.

So during an upgrade from N to N+1, a new `atelet` drives old `ateworker` processes on its node, a new `atelet` calls an old `ate-api-server`, an old `ate-controller` renders Deployments for new workers, and old `ate-controller` and clients talk to a new `ate-api-server`.

The reverse pairs are not supported: in particular, a worker must not be newer than the `atelet` on its node, and `ate-api-server` must not be newer than any worker. `ate-api-server` reaches the workers through `atelet`, and a new `ate-api-server` may rely on new worker behavior behind existing fields, so the workers go first.

## Requirement of a release

The skew rules above hold only if every release keeps the following.

### Worker pod template

`ate-controller` upgrades after the workers. So the workers start the `ateworker` N+1 image with the Deployment `ate-controller` N renders.

Within a major, `ate-controller` renders the same worker pod template for the same WorkerPool. New `ateworker` behavior comes from the image or from a new WorkerPool field, not from the template. So `ateworker` accepts the flags and environment the template passes for the whole major, and no WorkerPool rolls at `ate-controller`'s step, in an upgrade or a rollback.

### API fields

These rules say what a component does with a protobuf field it does not know.

Servers:

* `ate-api-server` rejects a request that carries a field it does not know, with `INVALID_ARGUMENT`.
* `ate-api-server` drops the fields it does not know when it reads a stored object, and serves the reduced object. Such a field was written by a newer replica, during `ate-api-server`'s roll or before a rollback, and this release cannot validate it. Rejecting it would make the object unreadable. Keeping it would serve a field this release cannot validate.
* `atelet` rejects fields it does not know. Its callers, `ate-api-server` and `ateworker`, are never newer than it during an upgrade or a rollback in the supported order, so an unknown field means the upgrade order was not followed.
* `ateworker` rejects fields it does not know. Its caller, `atelet`, can be one release newer, so `atelet` sends a new field only on releases after `ateworker` learns it (two-step releases).

Clients:

* A client that reads an object, changes it and writes it back keeps the unknown fields. An older client must not strip fields that a newer server still understands.
* `kubectl-ate` rejects a manifest file that has fields it does not know, as the server does for a create.

Wherever a caller can be newer than its server, the caller sends a new field only one release after the server learns it (two-step releases). Today that is `atelet` calling `ate-api-server` and `ateworker`.

### CRDs

The N+1 CRDs are applied with `ate-controller` N+1. Components that upgrade before it read the N CRDs, so users must not set new CRD fields until the upgrade completes.

Between compatible releases, a CRD change only adds optional fields. The CRDs serve one version with no conversion. So a rollback from N+1 that applies the N CRDs again removes every field N does not know from every object.

### SandboxConfig

Each sandbox class has a default SandboxConfig, such as `gvisor-default`, that lists the sandbox versions a cluster may run. A release that changes a sandbox version adds the new one, points `defaultVersion` at it, and keeps the previous release's version unless that version has a known vulnerability. 

A kept version's assets must stay fetchable, so a release does not overwrite them. The upgrade applies these lists. That does not change the pod template, so workers do not roll.

An actor that starts without memory state, new or cold booting, gets the default version. An actor restoring memory state gets the version its snapshot was taken with, and stays on it until it next starts without memory state. If that version is gone, it cold boots, keeping its files and losing its memory. Rollback applies the N lists again, which removes the N+1 versions.

<!-- TODO: Link the SandboxConfig upgrade guide, which covers the details. -->

### Database schema

The schema is updated with `ate-api-server`, and migrations follow [postgresql-schema-evolution.md](dev/postgresql-schema-evolution.md): every migration keeps working for the previous `ate-api-server` binary. Migrations are never rolled back.

### Data on nodes and in snapshots

This covers Substrate's own data only. The sandbox runtime's checkpoint files follow the runtime's own rules: a restore across runtime versions keeps only the file systems.

This section is about data one release writes and another release reads, whether that is a different component or the same component after an upgrade or a rollback. Substrate's data comes in two kinds, by how long it lives.

* Data on the node: the directory layout and the files `atelet` keeps there.
  * What N writes, N+1 can read. What N+1 writes, N can read.
  * So a layout change takes two releases: N+1 reads the old and the new layout but still writes the old one, and N+2 writes the new one. This keeps a rollback possible.
  * `atelet` passes the per-actor directories to `ateworker` in the RPC, so only `atelet` knows the layout. The sockets `atelet` and `ateworker` use to reach each other do not change within a major.
* Data in snapshots: `manifest.json`, which `atelet` writes and reads.
  * Within a major, a release reads the snapshot data any earlier release of that major wrote.
  * A new field must be safe to ignore. A release ignores manifest fields it does not know, so a snapshot taken at N+1 still restores after a rollback to N.

### System bundle

The upgrade applies the rest of the system bundle last, and a rollback does not undo it. So a component at N+1 must work with these resources at N, and a component at N must work with them at N+1.

Changes that only take something away, such as a NetworkPolicy that denies traffic, can go in this step. Anything a new component needs to run, such as a new Secret or a rule that allows its traffic, ships in that component's own step.

## Supported upgrade order

To move from N to N+1, in this order, waiting for each step to finish:

1. `podcertificate-controller`.
2. `atelet`, node by node.
3. Every WorkerPool's `spec.workerImage`, which rolls its workers.
4. `ate-api-server` and `ate-idp-server`. The first N+1 `ate-api-server` replica migrates the schema.
5. The CRDs, then `ate-controller`.
6. `atenet`.
7. The SandboxConfig admission policy and the default SandboxConfig.
8. The rest of the substrate system bundle: what no step above deploys.
9. Clients.

Rollback from N+1 to N is the same list in reverse, with release N, except that the schema migration and step 8 are not undone. Clients stop sending N+1 fields before `ate-api-server` goes back, and `atelet` goes back only after every worker is back at N. Rolling back further than N is not supported.

### Skipping releases

Patch releases can be skipped: `x.y.1` can upgrade straight to `x.y.5`.

Minor releases cannot: to go from `x.1` to `x.3`, upgrade to `x.2` first. The two-release steps and the database schema evolution both assume each upgrade moves one minor release.

## Outside this policy

The Kubernetes version, the database engine, the node OS, the CSI drivers and the actor images are not changed by the Agent Substrate upgrade.

## Testing

Before a release is cut, an installation at the previous compatible release is upgraded to it with actors running and rolled back, in the order above.

## Exceptions

As in the compatibility policy.

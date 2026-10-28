# SandboxConfig upgrade

This guide covers the SandboxConfig part of an upgrade: step 4d of the [upgrade runbook](upgrade.md), and its rollback. Run it from the runbook's shell, with `NEW_RELEASE` set and `~/ate-upgrade` in place.

Each release creates its own SandboxConfig per sandbox class, `substrate-<release>-gvisor` and `substrate-<release>-microvm`. Configs from earlier releases stay in the cluster. A WorkerPool without a `configRef` uses the newest config of its class with the `sandboxconfig.ate.dev/is-class-default` annotation. A pool with a `configRef` uses that config. [Class default](api-guide.md#class-default) in the API guide has the full rules. Moving a pool to another config does not roll its workers.

New actors start on the default version of their pool's config. A suspended actor restores its memory only on a version with the same assets as the one its snapshot was taken on. If the pool's config has no such version, the actor resumes without its memory, keeping its files. The [version skew policy](version-skew-policy.md#sandboxconfig) says which versions a release keeps.

## Upgrade

### gVisor

```bash
go run ./cmd/ate-setup deploy sandboxconfig --config ~/ate-upgrade/new.yaml
```

This applies the SandboxConfig admission policy and creates `substrate-$NEW_RELEASE-gvisor` as the gVisor class default. gVisor pools without a `configRef` move to it. Pools pinned to a config with `configRef` stay on it until you change their `configRef`.

### microVM

If the cluster runs microVM pools, also run this from the new checkout:

```bash
BUCKET_NAME=$(sed -n 's/^ *bucketName: //p' ~/ate-upgrade/new.yaml) VERSION=$NEW_RELEASE hack/install-microvm-deps.sh --install
```

The script reads neither `new.yaml` nor the image tag, so the variables give it the bucket and the release. It creates `substrate-$NEW_RELEASE-microvm` the same way as the gVisor config. It first stages the assets at fixed paths in the bucket. If the release changes them, earlier microVM configs no longer match the bucket, and atelet cannot fetch their assets on nodes that have not cached them. MicroVM pools pinned to an earlier config, or left on one by `--create-with-default-annotation=false`, then need to move to the new config.

### Create the configs without moving pools

Add `--create-with-default-annotation=false` to both commands above. Later runs leave the configs as they are, including `deploy ate-system` in step 4e of the runbook. Later, make each config the class default:

```bash
kubectl annotate sandboxconfig substrate-$NEW_RELEASE-gvisor sandboxconfig.ate.dev/is-class-default=true
kubectl annotate sandboxconfig substrate-$NEW_RELEASE-microvm sandboxconfig.ate.dev/is-class-default=true
```

### Check

Each pool's `CONFIG` column shows the config it uses:

```bash
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,CLASS:.spec.sandboxClasses[0].name,CONFIG:.status.sandboxClasses[0].configRef.name'
```

## Roll back

Roll back the SandboxConfigs first in the runbook's [Roll back step 4](upgrade.md#roll-back-step-4), from the old checkout.

Actors suspended on a version the old release's config does not list resume without their memory. To keep restoring snapshots taken on a version new in this release, copy that version's entry from the new release's config into the old one before you go on.

If the release changed the microVM assets, stage the old ones from the old checkout with `hack/microvm-assets/assemble.sh` and the stage script, as `hack/install-microvm-deps.sh --install` runs them.

Then remove the class-default annotation from the new release's configs. Skip a line if the upgrade did not create that config.

```bash
kubectl annotate sandboxconfig substrate-$NEW_RELEASE-gvisor sandboxconfig.ate.dev/is-class-default-
kubectl annotate sandboxconfig substrate-$NEW_RELEASE-microvm sandboxconfig.ate.dev/is-class-default-
```

The old release's config still has the annotation, so it is the class default again, and pools without a `configRef` move back to it. Pools pinned to a new release's config stay on it until you change their `configRef`.

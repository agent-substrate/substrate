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

Each pool's `IN-USE` column shows the config it uses. `PINNED` is the config its `configRef` names, and `PENDING` is a config it is switching to.

```bash
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,PINNED:.spec.sandboxClasses[*].configRef.name,IN-USE:.status.sandboxClasses[*].configRef.name,PENDING:.status.sandboxClasses[*].pendingConfigRef.name'
```

## Delete earlier configs

Once the upgrade is done, delete the configs of earlier releases that no pool uses, so they do not pile up. Check first that no pool names the config in any column of [Check](#check). A config that a pool uses is not deleted until no pool does. If you roll back later, [Roll back](#roll-back) creates the old release's config again.

```bash
kubectl delete sandboxconfig <name>
```

## Roll back

Rollback runs from the old checkout, in two parts around the components in the runbook's [Roll back step 4](upgrade.md#roll-back-step-4).

### Move pools off the new configs

List the pools as in [Check](#check). Change or remove the `configRef` of each pool whose `PINNED` column names a new release's config. A config is not deleted while a pool uses it, so a pinned pool blocks the delete below.

List the configs and their class-default annotations:

```bash
kubectl get sandboxconfigs -o custom-columns='NAME:.metadata.name,CLASS:.spec.sandboxClass,CLASS-DEFAULT:.metadata.annotations.sandboxconfig\.ate\.dev/is-class-default,CREATED:.metadata.creationTimestamp'
```

Each class needs a config other than the new release's with `CLASS-DEFAULT` set to `true`, usually the old release's config. Otherwise pools without a `configRef` stay on the new config, and the delete below does not finish.

If a class has none and the list has the old release's config of that class, annotate that config:

```bash
kubectl annotate sandboxconfig substrate-$OLD_RELEASE-gvisor sandboxconfig.ate.dev/is-class-default=true
kubectl annotate sandboxconfig substrate-$OLD_RELEASE-microvm sandboxconfig.ate.dev/is-class-default=true
```

If the old release's gVisor config is gone, create it. It gets the annotation:

```bash
go run ./cmd/ate-setup deploy sandboxconfig --config ~/ate-upgrade/old.yaml
```

This also applies the old admission policy before the new config is gone. If the delete then does not finish, see [Restore the old admission policy](#restore-the-old-admission-policy).

If the cluster runs microVM pools, run this. It stages the old release's microVM assets, and creates the old release's microVM config with the annotation if it is gone.

```bash
BUCKET_NAME=$(sed -n 's/^ *bucketName: //p' ~/ate-upgrade/old.yaml) VERSION=$OLD_RELEASE hack/install-microvm-deps.sh --install
```

Then delete the new release's configs:

```bash
kubectl delete sandboxconfig substrate-$NEW_RELEASE-gvisor substrate-$NEW_RELEASE-microvm --ignore-not-found --wait=false
```

A config being deleted is never the class default, so pools without a `configRef` move to the newest other config with the annotation. ate-controller finishes the delete once no pool uses the config. `--wait=false` returns before that, because if the new ate-controller is broken, the delete only finishes after the runbook restores the old one.

If the old release's config does not list the version an actor was suspended on, the actor resumes without its memory, keeping its files.

### Restore the old admission policy

Run this after `deploy ate-controller` in the runbook's [Roll back step 4](upgrade.md#roll-back-step-4). Wait for the new release's configs to be gone, then apply the old release's policy:

```bash
kubectl wait --for=delete sandboxconfig/substrate-$NEW_RELEASE-gvisor sandboxconfig/substrate-$NEW_RELEASE-microvm --timeout=5m
kubectl apply --server-side --force-conflicts --field-manager=ate-setup -f manifests/ate-install/sandboxconfig-validation.yaml
```

ate-controller finishes a delete by removing the config's finalizer, which is an update. The new policy checks only versions that an update adds or whose assets it changes. An older policy may check them, and reject the update if the new config fails its rules. So the wait comes first.

If the wait times out and a pool still names a new config in [Check](#check), fix that pool. If no pool does, the old policy rejected the update, which can happen after `deploy sandboxconfig` above applied it early. Apply the new release's policy, then run the commands above again:

```bash
kubectl apply --server-side --force-conflicts --field-manager=ate-setup -f ~/ate-upgrade/new/manifests/ate-install/sandboxconfig-validation.yaml
```

Apply the policy even if the upgrade created no config, because step 4d applies it first.

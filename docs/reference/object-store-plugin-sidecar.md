---
title: "Object Store Plugin Sidecar"
linkTitle: "Object Store Plugin"
weight: 7
description: >
  What the install guarantees a provider's object store plugin that replaces the in-tree objectstore-plugin sidecars.
---

# Object Store Plugin Sidecar Contract

Substrate moves snapshots and sandbox assets through two `objectstore-plugin`
sidecars. They serve the
[object store plugin API](../../api/objectstore/v1/objectstore.proto). The
in-tree plugin supports GCS and S3. A provider can ship its own plugin image
and install it with `ate-setup` without changing substrate.

This page lists what the install keeps stable for such a plugin. Everything
else in the two pod specs, and everything about the in-tree plugin, stays
ours to change in any release.

## Installing a provider plugin

Install with `--storage-backend=external` and a kustomize Component that
replaces the sidecars:

```sh
go run ./cmd/ate-setup deploy ate-system \
  --storage-backend=external \
  --component=path/to/your-component \
  --snapshot-location=SCHEME://BUCKET/PREFIX/ \
  --microvm-asset-location=SCHEME://BUCKET/ASSET_PREFIX
```

Upload the `microvm` sandbox class's guest assets to the
`--microvm-asset-location` first, as [microVM assets](#microvm-assets) shows.

Pass the same `--storage-backend` and `--component` to every later `deploy`
command, such as `deploy atelet` or `deploy apiserver`. `ate-setup` does not
read them back from the cluster, so without them it renders the in-tree
sidecars again. A `--config` file keeps them in one place.

[`docs/examples/object-store-provider-component`](../examples/object-store-provider-component/kustomization.yaml)
is a complete example with a placeholder image. `ate-setup`'s render tests
compose it, so a change to this contract fails them.

Under `external`, `ate-setup`:

- removes `ATE_STORAGE_BACKEND` and the `AWS_*` variables from both sidecars;
- sets no snapshot transfer service account and derives no snapshot location,
  so set `--snapshot-location` to a URI your plugin serves;
- stages no microVM assets (see [microVM assets](#microvm-assets));
- refuses to install without a `--component`.

The component patches each sidecar:

- Select each target with `target:` and namespace `ate-system`. Each apply
  renders only part of the control plane.
- `ate-setup` composes the component into every control plane apply, so any
  `resources:` it lists are applied each time too.
- Set `image`, `args` and any `env` your plugin needs.
- Replace `startupProbe` with `$patch: replace`. The in-tree probe runs the
  in-tree binary's `healthcheck` subcommand, which your image need not have.
  Kubernetes starts atelet and ate-api-server only once the sidecar's probe
  passes, so make it pass once the plugin serves on its socket.
- Pin your image by digest. `ate-setup` resolves only `ko://` references,
  whether it builds from source or installs from `--image-repo`. It applies
  other image references as written, and adds `--preview` only to containers
  with a substrate `ko://` image.

## Sidecars

| Workload | Socket | Services | Caller |
| :--- | :--- | :--- | :--- |
| DaemonSet `atelet` | `/run/objectstore-plugin/node.sock` | `NodeProvider`, `AssetProvider` | atelet |
| Deployment `ate-api-server` | `/run/objectstore-plugin/control.sock` | `ControlProvider` | ate-api-server |

- Each sidecar is the init container named `objectstore-plugin`, with
  `restartPolicy: Always`. It outlives the main container, so transfers in
  flight while the main container drains can finish.
- The plugin serves gRPC on the socket in the `objectstore-plugin` volume,
  which both containers mount at `/run/objectstore-plugin`. The proto
  comments define each RPC and the codes it returns.
- Local paths in `NodeProvider` and `AssetProvider` requests are under
  `/var/lib/ate`. The node sidecar mounts it from the host with
  `HostToContainer` propagation, and runs as root with every capability
  dropped, like atelet.

## Certificates

The sidecars mount these projected volumes:

| Sidecar | Volume | Path | Files |
| :--- | :--- | :--- | :--- |
| node | `servicedns-ca` | `/run/servicedns.podcert.ate.dev` | `trust-bundle.pem` |
| control | `servicedns` | `/run/servicedns.podcert.ate.dev` | `credential-bundle.pem`, `trust-bundle.pem` |
| control | `podidentity` | `/run/podidentity.podcert.ate.dev` | `credential-bundle.pem`, `trust-bundle.pem` |

- `trust-bundle.pem` under `/run/servicedns.podcert.ate.dev` holds the CAs of
  the `servicedns.podcert.ate.dev/identity` signer. It verifies the
  certificates ate-api-server and ate-idp-server serve.
- The `servicedns` credential bundle is the ate-api-server pod's serving
  certificate for its `api` Service, `api.ate-system.svc`.
- The `podidentity` credential bundle is the ate-api-server pod's identity.
  With it the control sidecar can call ate-api-server at `127.0.0.1:443`,
  with server name `api.ate-system.svc`. It holds the `api_server_sidecar`
  role, which may only get actors.

## Exchange port

The control sidecar may serve TCP port 8445, named `exchange`. The `api`
Service forwards its `exchange` port 8445 to it, and the `ate-api-server`
NetworkPolicy admits that port only from atelet pods. Node sidecars reach
it at `api.ate-system.svc:8445`. Serve TLS with the `servicedns` credential
bundle, and verify it on the node with the `servicedns-ca` trust bundle.

The protocol on the port is the plugin's own. The in-tree plugin runs its
credential exchange there, and that protocol is private.

## Actor JWTs

Each `NodeProvider` request carries the actor's JWT, with the audience
`objectstore-plugin.ate.dev`, for plugins that authorize storage per actor.
[Actor JWTs](../guides/actor-jwts.md) describes the token and how to verify
it.

- The control sidecar gets the issuer ate-api-server mints with in
  `ATE_API_ACTOR_JWT_ISSUER`. The node sidecar gets no issuer setting, so pass
  it in your component.
- The default issuer, `https://idp.ate-system.svc`, serves a certificate the
  servicedns trust bundle above verifies. Both sidecars mount it. See
  [Reaching the issuer](../guides/actor-jwts.md#reaching-the-issuer).

## microVM assets

`ate-setup` stages the `microvm` sandbox class's guest assets only on `gcs`
and `s3`. Under `external`, upload them yourself and pass their location with
`--microvm-asset-location`. Installing the `microvm` class fails without it.

The `microvm` SandboxConfig fetches each asset from
`LOCATION/ARCH/SHA256` through the node plugin, so:

1. Get `cloud-hypervisor`, `virtiofsd`, `vmlinux` and `rootfs.img` for each
   node architecture (`amd64`, `arm64`).
   [Running the microVM runtime locally](../dev/microvm-local.md) says where they come
   from. A `gcs` or `s3` install assembles them into
   `bin/microvm-assets/ARCH/` of the checkout.
2. Check each file's SHA-256 against its entry in
   [`sandboxconfig-microvm.yaml.tmpl`](../../manifests/ate-install/sandboxconfig-microvm.yaml.tmpl).
   The template names `vmlinux` `kata-kernel` and `rootfs.img` `kata-image`.
3. Upload each file to `LOCATION/ARCH/SHA256`.

The location must not overlap a snapshot location, because the plugin must
refuse to serve snapshots as assets. Your node plugin must serve the
location's scheme through `AssetProvider`.

## Not part of the contract

These may change in any release. Re-render your component against each
release.

- The in-tree plugin's subcommands, flags and environment.
- Other containers, volumes, resources and settings of the two pods.
- The in-tree credential exchange protocol, and the Go packages under
  `internal/`.

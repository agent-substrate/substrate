---
title: "Image Provider Plugin"
linkTitle: "Image Provider Plugin"
weight: 12
docsite: false
description: >
  How atelet prepares container images through a node's image provider plugin.
---

# Image Provider Plugin

By default atelet pulls each container and image volume image into the
node-local image cache. A node can also run an image provider plugin, a
DaemonSet that prepares image layers itself, for example by loading them lazily
from a remote store. The plugin API and a library for writing plugins live in
[ai-on-gke/image-provider-plugin](https://github.com/ai-on-gke/image-provider-plugin).

The plugin is a preview feature. atelet prepares images through it only when
the `ImageProviderPlugin` [preview gate](../dev/preview-gates.md) is on.

## Configuration

atelet talks to the plugin over the Unix socket that `--image-provider-socket`
names, `/run/ate/image-provider/plugin.sock` by default. The atelet DaemonSet
mounts only the node's `/run/ate/image-provider`, read-only. An empty flag
value turns the plugin off. atelet uses the plugin only while its socket
exists, so you can install and remove a plugin without restarting atelet.

atelet and the plugin roll out independently. Within the v1alpha1 API, changes
only add fields and enum values. A breaking change gets a new proto package,
and a plugin on another package answers `UNIMPLEMENTED`, so atelet pulls into
the image cache instead.

The plugin returns one read-only rootfs directory per image, with all layers
applied, under `/var/lib/ate/image-provider`. atelet sends that path as its
mount root on every call and creates it at startup. The worker stacks the
rootfs as an overlayfs lowerdir. The plugin DaemonSet must mount `/var/lib/ate`
with `Bidirectional` propagation, so that atelet and the worker pods see its
mounts, and pass `--mount-root=/var/lib/ate/image-provider`. The plugin may
also serve other clients, each under its own mount root.

## Enabling the preview gate

Install with the `ImageProviderPlugin` gate, for example
`ATE_PREVIEW=ImageProviderPlugin` with ate-setup. Turn the gate on only after
every WorkerPool runs the release that has it. atelet then writes overlay specs
at version 2 for plugin images, which older workers reject.

With the gate off, atelet pulls every image into the image cache and takes no
new plugin leases. It still releases, reconciles and refreshes the leases its
actors hold, and still suspends them when the plugin drains. ate-setup passes
`ATE_PREVIEW` to the worker pods too, so changing it rolls every WorkerPool.
Actors that resume after the roll use the image cache.

Before you roll atelet back to a release without the gate, turn the gate off
and wait until no actor holds a plugin image. An older atelet cannot read the
version 2 overlay specs: its image cache GC skips every pass while one is on
the node, and an older worker cannot mount one.

## Preparing an image

For each image a Run or Restore needs, atelet:

1. Serves a digest ref straight from the image cache when the cache holds the
   whole image.
2. Otherwise, with the gate on, asks the plugin with `CanProvide`, and if it
   serves the image, calls `PrepareImage` with the registry credentials atelet
   resolves for it. `CanProvide` gets no credentials. The calls time out after
   5 seconds and 1 minute.
3. Pulls into the image cache when the plugin declines the image, fails, times
   out, or returns a digest other than the one the ref pins.

atelet records each request on `ate.imageprovider.requests` and
`ate.imageprovider.duration`. See [Observability](observability.md).

## Leases

The plugin keeps an image's layers while any lease on it is left. atelet names
each lease `<actor UID>/<container>`, or `<actor UID>/<container>/<volume>` for
an image volume, and records it in the bundle's `rootfs-overlay.json`.

atelet releases an actor's leases when the actor checkpoints or terminates, and
when a Run or Restore fails. Each time the plugin's socket appears, atelet
sends `ReconcileImages` with the leases the overlay specs on the node record.
The plugin drops atelet's other leases and leaves other clients' leases alone.

When the plugin reports `AUTH_REFRESH_REQUIRED` for a lease, atelet sends fresh
registry credentials with `RefreshAuth` every minute, and right after
`ReconcileImages`.

## Draining the plugin

A stopping plugin reports `PLUGIN_STATE_DRAINING` and serves no new images, so
new activations use the image cache. While it drains, atelet asks the control
plane to suspend each running actor on the node that holds a lease. The actor
resumes later from its snapshot.

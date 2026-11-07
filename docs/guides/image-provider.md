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

## Configuration

atelet talks to the plugin over the Unix socket that `--image-provider-socket`
names, `/run/ate/image-provider.sock` by default. The atelet DaemonSet mounts
the node's `/run/ate` for it. An empty flag value turns the plugin off. atelet
uses the plugin only while its socket exists, so you can install and remove a
plugin without restarting atelet.

The plugin returns host directories, one per layer, that the worker stacks as
overlayfs lowerdirs as they are. Whiteouts in them must use the overlayfs
format. Worker pods see only `/var/lib/ate`, so the plugin must put the
directories there.

## Preparing an image

For each image a Run or Restore needs, atelet:

1. Serves a digest ref straight from the image cache when the cache holds the
   whole image.
2. Otherwise asks the plugin with `CanProvide`, and if it serves the image,
   calls `PrepareImage` with the registry credentials atelet resolves for it.
3. Pulls into the image cache when the plugin declines the image or fails.

atelet records each request on `ate.imageprovider.requests` and
`ate.imageprovider.duration`. See [Observability](observability.md).

## Leases

The plugin keeps an image's layers while any lease on it is left. atelet names
each lease `<actor UID>/<container>`, or `<actor UID>/<container>/<volume>` for
an image volume, and records it in the bundle's `rootfs-overlay.json`.

atelet releases an actor's leases when the actor checkpoints or terminates, and
when a Run or Restore fails. Each time the plugin's socket appears, atelet
sends `ReconcileImages` with the leases the overlay specs on the node record,
and the plugin drops every other lease.

When the plugin reports `AUTH_REFRESH_REQUIRED` for a lease, atelet sends fresh
registry credentials with `RefreshAuth` every minute, and right after
`ReconcileImages`.

## Draining the plugin

A stopping plugin reports `PLUGIN_STATE_DRAINING` and serves no new images, so
new activations use the image cache. While it drains, atelet asks the control
plane to suspend each running actor on the node that holds a lease. The actor
resumes later from its snapshot.

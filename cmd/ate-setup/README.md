# ate-setup

Installs and tears down Agent Substrate on a Kubernetes cluster.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
go build -o bin/ate-setup ./cmd/ate-setup
```

`ate-setup` is the installer. `hack/install-ate.sh` is a shim over it, kept so
that existing command lines and CI jobs keep working; it holds no install logic
of its own.

## Installing pre-built images

By default every image is built from the checkout with `ko`, which is what a
developer wants and what the shell installer always did. To install images
that are already published instead, name the registry that holds them and the
tag they share:

```
ate-setup deploy ate-system \
  --credential-provider='{"name":"k8s.io"}' \
  --image-repo registry.example.com/substrate \
  --image-tag vX.Y.Z
```

The registry has to hold every image the install deploys. The images a release
publishes to `ghcr.io/agent-substrate/substrate` are not enough on their own;
see [`docs/dev/releasing.md`](../../docs/dev/releasing.md#release-images). An
install that needs an image the registry lacks fails naming it.

Nothing is built and no registry is pushed to; the manifests still come from
the checkout, which should be the tag being installed. Each reference is pinned
to the digest its tag names, so the registry has to be readable from here as
well as from the cluster.

- [`commands.md`](commands.md) — every command with its `hack/install-ate.sh`
  equivalent.
- [`differences.md`](differences.md) — where the Go port deliberately behaves
  differently from the shell scripts it replaced, and what was reproduced
  exactly.
- [`cli-diff.md`](cli-diff.md) — what a `hack/install-ate.sh` user can observe
  after the switch.

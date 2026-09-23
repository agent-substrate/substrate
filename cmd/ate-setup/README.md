# ate-setup

Installs and tears down Agent Substrate on a Kubernetes cluster.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
make build-ate-setup    # builds bin/ate-setup
```

`ate-setup` is the installer. `hack/install-ate.sh` is a shim over it, kept so
that existing command lines and CI jobs keep working; it holds no install logic
of its own.

## Installing a release

By default every image is built from the checkout with `ko`, which is what a
developer wants and what the shell installer always did. To install published
images instead, name the registry they were pushed to:

```
ate-setup deploy ate-system \
  --image-repo registry.example.com/substrate \
  --image-tag v0.0.0
```

Nothing is built and no registry is pushed to; the manifests still come from the
checkout. Each reference is pinned to the digest its tag names, so the registry
has to be readable from here as well as from the cluster.

- [`commands.md`](commands.md) — every command with its `hack/install-ate.sh`
  equivalent.
- [`differences.md`](differences.md) — where the Go port deliberately behaves
  differently from the shell scripts it replaced, and what was reproduced
  exactly.
- [`cli-diff.md`](cli-diff.md) — what a `hack/install-ate.sh` user can observe
  after the switch.

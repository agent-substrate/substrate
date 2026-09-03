# ate-setup

Installs and tears down Agent Substrate on a Kubernetes cluster.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
make build-ate-setup    # builds bin/ate-setup
```

`ate-setup` is a Go port of `hack/install-ate.sh` and the scripts it sources.
Both work today and can be used against the same cluster.

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
- [`differences.md`](differences.md) — where the port deliberately behaves
  differently, and what was reproduced exactly.

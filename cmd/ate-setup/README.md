# ate-setup

Installs and tears down Agent Substrate on a Kubernetes cluster.

```
go run ./cmd/ate-setup [global flags] <command> [flags]
make build-ate-setup    # builds bin/ate-setup
```

`ate-setup` is a Go port of `hack/install-ate.sh` and the scripts it sources.
Both work today and can be used against the same cluster.

- [`commands.md`](commands.md) — every command with its `hack/install-ate.sh`
  equivalent.
- [`differences.md`](differences.md) — where the port deliberately behaves
  differently, and what was reproduced exactly.

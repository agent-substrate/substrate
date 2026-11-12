---
title: "OpenAPPA"
linkTitle: "OpenAPPA"
weight: 19
description: >
  Check actors' MCP tool calls against what they have already read, at the egress gateway.
---

# OpenAPPA

[OpenAPPA](https://github.com/archestra-ai/OpenAPPA) is an information-flow
policy engine. With `--openappa`, `ate-setup` deploys it as the egress gateway's
additional `ext_proc` filter (see [Extending Substrate](../security/extensibility.md)).
It reads the bodies of the MCP `tools/call` requests an actor sends, and their
responses. It refuses a call that would send data to someone who may not
already see it. See the [OpenAPPA docs](https://www.openappa.com/docs/substrate)
for how the policy works.

## Enable it

On an installed cluster, redeploy the dataplane with it:

```sh
go run ./cmd/ate-setup deploy atenet --credential-provider='{"name":"k8s.io"}' --openappa
```

`--openappa` works on `deploy ate-system` and `deploy atenet`, on the envoy
dataplane only. It deploys `deployment/openappa` into `ate-system`, with a
Service and a NetworkPolicy that admits only the egress gateway. It also points
the additional ext_proc filter at that Service, lets the filter buffer bodies,
and sets its timeout to 60s unless
`--experimental-additional-egress-extproc-timeout` is given. Naming a different `--experimental-additional-egress-extproc-service`
is refused.

A redeploy without `--openappa` removes OpenAPPA. It keeps the policy
ConfigMap and the credentials Secret.

## Policy

The policy is `appa.toml` in `configmap/openappa-policy`.

- Without `--openappa-policy`, the first deploy installs the bundled policy,
  [`manifests/openappa/appa.toml`](../../manifests/openappa/appa.toml). It
  includes OpenAPPA's GitHub battery. Later deploys leave the ConfigMap
  alone, so you can edit it in place.
- With `--openappa-policy=PATH`, every deploy replaces the ConfigMap with that
  file.

A deploy restarts OpenAPPA when the policy changed.

## MCP servers and egress policy

`--openappa-mcp-servers` lists the MCP servers OpenAPPA checks, as
`HOST=NAME` pairs. The default is `api.githubcopilot.com=github`.

OpenAPPA only sees requests the gateway can read. Allow each MCP host in the
actor's `EgressPolicy` with an `https` rule, not `tls_passthrough` (see
[Egress Traffic](egress-traffic.md)). Allow only the MCP hosts, so the actor
cannot send data around OpenAPPA.

## Provider credentials

The GitHub battery calls the GitHub API to learn who may read a repository.
Give it a token in the optional `openappa-provider-credentials` Secret:

```sh
kubectl -n ate-system create secret generic openappa-provider-credentials \
  --from-literal=APPA_PROVIDER_GITHUB_TOKEN=<token>
kubectl -n ate-system rollout restart deployment/openappa
```

The runtime reads it at startup, so restart it after a change.

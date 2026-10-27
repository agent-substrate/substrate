# Extending Substrate

Substrate ships with built-in sandboxing, actor identity, and ingress and egress gateways. Security vendors and platform teams can plug their own capabilities into the gateways, either to **augment** the built-in controls or to **replace** them.

| Path | Ingress | Egress |
| --- | --- | --- |
| **Augment:** add your `ext_proc` callout to Substrate's gateway | Planned ([#3259](https://github.com/agent-substrate/substrate/issues/3259)) | Available (experimental) |
| **Replace:** bring your own gateway | Planned ([#3258](https://github.com/agent-substrate/substrate/issues/3258)) | Partial |

## Augment: add an ext_proc callout

You can add your own [`ext_proc`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) service. Substrate gateways call it on every request.

- **Egress:** set `--experimental-additional-egress-extproc-service` (see [Operator install](operator-install.md#settings)). This may be converted to an API in the future. Your service runs after Substrate has authenticated the actor and applied its `EgressPolicy`. It currently receives the request headers and the actor's identity, and it can deny the request or add headers; this can be changed if necessary. Today it sees headers only, not bodies.
- **Ingress:** planned.

## Replace: bring your own gateway

- **Egress:** you can point actors at your own policy enforcement point (PEP) instead of `atenet-egress`. Your PEP must meet the [Network Egress Contract](network-egress.md): it must verify the actor's certificate and reject any traffic it can't attribute to a running actor. Honoring Substrate's `EgressPolicy` is encouraged but not required. The installer doesn't have a bring-your-own mode yet.
- **Ingress:** planned. `atenet-router` does more than filter traffic: it wakes suspended actors and routes requests to their workers. A replacement router will need to call Substrate, either through the Substrate ext_proc or via the Substrate API, for those steps.

If you replace a gateway, the authentication and authorization it enforced become your responsibility.

## Get help with integrations

We want to help you integrate, and we're discussing other ways to extend Substrate beyond gateways. Talk to us in [#substrate-dev](https://cloud-native.slack.com/archives/C0B6M3E2J3D) on CNCF Slack, [open a GitHub issue](https://github.com/agent-substrate/substrate/issues/new), or join the weekly [community meeting](../README.md#community).

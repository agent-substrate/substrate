---
title: "Extending Substrate"
linkTitle: "Extending Substrate"
weight: 3
description: >
  Augment or replace the built-in ingress and egress gateways with custom policy enforcement points.
---

# Extending Substrate

Substrate ships with built-in sandboxing, actor identity, and ingress and egress gateways. Security vendors and platform teams can plug their own capabilities into the gateways, either to **augment** the built-in controls or to **replace** them.

| Path | Ingress | Egress |
| --- | --- | --- |
| **Augment:** add your `ext_proc` callout to Substrate's gateway | Planned ([#3259](https://github.com/agent-substrate/substrate/issues/3259)) | Available (experimental) |
| **Replace:** bring your own gateway | Planned ([#3258](https://github.com/agent-substrate/substrate/issues/3258)) | Partial |

## Augment: add an ext_proc callout

You can add your own [`ext_proc`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) service. Substrate gateways call it on every request.

- **Egress:** set `--experimental-additional-egress-extproc-service` (see [Operator install](../guides/operator-install.md#settings)). This may be converted to an API in the future. Your service runs after Substrate has authenticated the actor and applied its `EgressPolicy`. It receives the request headers and the actor's identity, and it can deny the request or add headers. Envoy waits 2s for each response from it; `--experimental-additional-egress-extproc-timeout` changes that.
  - **Bodies:** with `--experimental-additional-egress-extproc-allow-mode-override`, your service can read the request and response bodies of the requests it picks. To pick one, return a `mode_override` in the `request_headers` response with `response_header_mode: SEND`, `request_body_mode: BUFFERED` and `response_body_mode: BUFFERED`. Envoy ignores any other override. It then sends the response headers, the whole request body and the whole response body, and your service can change them too.
  - Envoy holds a buffered body until it ends, and fails the request if the body outgrows its buffer limit. Pick only requests whose bodies end, such as an MCP `tools/call` POST. A long-lived stream such as an SSE `GET` never finishes, so leave those in the default mode.
  - For example, [OpenAPPA](https://github.com/archestra-ai/OpenAPPA) buffers each MCP `tools/call` this way. It checks the call against the data the actor has already read, and refuses it before it leaves the cluster. `--openappa` deploys it and sets these flags; see [OpenAPPA](../guides/openappa.md).
- **Ingress:** planned.

## Replace: bring your own gateway

- **Egress:** you can point actors at your own policy enforcement point (PEP) instead of `atenet-egress`. Your PEP must meet the [Network Egress Contract](../guides/network-egress.md): it must verify the actor's certificate and reject any traffic it can't attribute to a running actor. Honoring Substrate's `EgressPolicy` is encouraged but not required. The installer doesn't have a bring-your-own mode yet.
- **Ingress:** planned. `atenet-router` does more than filter traffic: it wakes suspended actors and routes requests to their workers. A replacement router will need to call Substrate, either through the Substrate ext_proc or via the Substrate API, for those steps.

If you replace a gateway, the authentication and authorization it enforced become your responsibility.

## Get help with integrations

We want to help you integrate, and we're discussing other ways to extend Substrate beyond gateways. Talk to us in [#substrate-dev](https://cloud-native.slack.com/archives/C0B6M3E2J3D) on CNCF Slack, [open a GitHub issue](https://github.com/agent-substrate/substrate/issues/new), or join the weekly [community meeting](../../README.md#community).

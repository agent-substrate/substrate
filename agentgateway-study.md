# AgentGateway egress study

**Target:** `egress-client-certs` at `488e52ca`.

**Goal:** Build a concrete model of how the AgentGateway dataplane replaces Envoy for ingress and egress.

**Prerequisites:** Read YAML and follow a request across a few configuration blocks.

**Approach:** Answer each exercise in two to five sentences before reading the listed implementation detail.

## Running example

An actor in `team-a` dials `https://api.example.com`.

Atunnel captures the connection and opens its authenticated CONNECT tunnel to `atenet-egress`.

Compare what AgentGateway can see before and after TLS termination.

## 1. Identify the two gateway roles

**Read:** `manifests/ate-install/components/agentgateway/configmap.yaml`, from `atenet-router-agentgateway-config` through `atenet-egress-agentgateway-substrate-config`.

Draw four boxes for an external caller, `atenet-router`, an actor, and `atenet-egress`.

Label which AgentGateway configuration owns ingress and which owns egress.

Explain why the two configurations have different uses of `dynamic` backends.

**Expected artifact:** A four-box request diagram with one arrow per hop.

## 2. Trace an actor's cleartext HTTP egress

**Read:** The `atenet-egress-agentgateway-substrate-config` `frontendPolicies`, `binds`, and HTTP listener.

For an actor that dials an IP address on port 80, list the order in which `connect`, `substrateEgressActorResolution`, `substrateEgress`, and the dynamic backend matter.

State what information decides the eventual backend target.

State which component consults `ate-api-server`.

**Expected artifact:** A numbered call chain with the identity and destination data available at each step.

## 3. Separate the three TLS connections

**Read:** `manifests/ate-install/components/agentgateway/configmap.yaml` and `manifests/ate-install/components/agentgateway-egress/kustomization.yaml`.

Name the TLS connection between atunnel and egress.

Name the TLS connection the actor expects when it dials an intercepted HTTPS origin.

Name the TLS connection AgentGateway originates to that origin.

For each one, identify the certificate and key paths in the configuration.

Then explain what `tls.mode: dynamicCa` does and what it does not do.

**Expected artifact:** A three-row table with client, server, certificate purpose, and configuration path.

## 4. Locate policy and secret injection

**Read:** `cmd/ate-setup/internal/steps/overlay.go`, especially `patchAgentgatewayEgressInject`, and the `#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS` marker in the AgentGateway egress component.

Explain why the installer modifies a marker in the AgentGateway configuration instead of starting the Envoy `ext_proc` sidecar.

Identify the URI authority that selects a configured credential provider.

Predict what happens if a policy needs a provider but the install selected `{"enabled":false}`.

**Expected artifact:** A short explanation of the credential-provider trust boundary.

## 5. Diagnose the actor-client-certificate gap

**Read:** `internal/e2e/suites/egresscredinject/client_certificate_test.go` through the AgentGateway skip, then revisit the AgentGateway HTTP backend configuration.

List the conditions a shared egress gateway would need before it can present an actor-specific client certificate to an origin.

Compare those conditions with the static `backendTLS` file paths AgentGateway has today.

Explain why the existing `dynamicCa` facility does not by itself solve outbound actor mTLS.

**Expected artifact:** A one-paragraph design constraint, not an implementation proposal.

## Capstone

Assume AgentGateway gained a dynamic upstream-client-certificate provider.

Describe the minimum trusted inputs that provider would receive.

Describe the cache key needed to prevent one actor incarnation from using another actor's authenticated upstream connection.

Name one behavior that certificate rotation cannot change for an already-open HTTP/2 stream.

**Optional reading:** `internal/e2e/atenet_dataplane.go` for the deliberate observable differences between Envoy and AgentGateway test lanes.

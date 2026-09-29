---
title: "MITM & Egress Policies"
linkTitle: "MITM & Egress Policies"
weight: 2
description: >
  Actor identity certificates, default-deny egress policy enforcement, and TLS man-in-the-middle trust bundles.
---

Agent Substrate treats all code running inside an actor sandbox as **untrusted**. Outbound network access is governed by three cryptographic and policy mechanisms that work together:

1. **Per-Actor Cryptographic Identity (`ActorIdentity` X.509 Certificates)**
2. **Default-Deny Egress Policies (`EgressPolicy`)**
3. **TLS Man-in-the-Middle (MITM) Interception (`sdsmint` & `egress-mitm.ate.dev`)**

> [!TIP]
> Looking for step-by-step operational instructions? See the consolidated **[Egress How-to Guide](../../guides/egress/)** or try the hands-on **[Pluggable Egress Networking Tutorial](../../tutorials/egress/)**.

---

## 1. Actor Identity & mTLS Tunneling

When an actor is activated on a worker pod, `atunnel` generates an ephemeral private key inside the worker's trusted network namespace and requests a short-lived X.509 client certificate from the control plane via the node-local `atelet`.

* **Private Key Isolation:** The private key never enters the actor sandbox; it remains inside `atunnel`.
* **Certificate Claims:** The certificate chains to the cluster's `actor-id-ca-pool` root CA and carries:
  * A SPIFFE URI SAN (`spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>`).
  * An `ActorIdentity` X.509 extension (`OID 1.3.6.1.4.1.11129.2.12.2`) encoding the actor's **atespace**, **name**, **UID**, and **purpose** (`atunnel`).
* **Live UID Verification:** Before accepting an outbound `HTTP/1.1 CONNECT` tunnel, the egress gateway (`atenet-egress`) verifies the client certificate chain and queries `ate-api-server` to confirm that the actor exists, is currently in `ACTOR_STATE_RUNNING`, and has the exact **UID** certified in the extension. Deleting and recreating an actor under the same name invalidates any previous certificate.

---

## 2. Egress Policy Model (`EgressPolicy`)

By default, the egress gateway enforces a **default-deny** posture: an actor without an `EgressPolicy` cannot open any outbound tunnel (except DNS to port 53 when the local DNS relay is active).

Each actor has at most one `EgressPolicy` in its atespace, evaluated in rule order (**first match wins**):

* **Cleartext HTTP & Terminated HTTPS Rules:** Evaluated per HTTP request against the request's `Host` header and destination port. Crucially, when authorization is granted based on a hostname rule, the gateway routes traffic to the **authorized hostname** rather than trusting the raw IP address the actor originally dialed.
* **Address / CIDR Rules:** For opaque TCP or pass-through TLS connections, rules are evaluated at `CONNECT` time against the destination IP and port.

### Example: Defining an Egress Policy

```yaml
rules:
  - http:
      hostnames: ["api.example.com", "*.internal.example.org"]
      ports:
        all: {}
  - https:
      hostnames: ["api.anthropic.com", "storage.googleapis.com"]
```

Apply or update the policy using [`kubectl-ate`](../../reference/kubectl-ate/#egress-policies):

```bash
kubectl ate create egress-policy <actor-name> -a <atespace> -f policy.yaml
```

---

## 3. TLS MITM Interception (`sdsmint`)

To inspect, filter, or apply L7 policy to outbound HTTPS traffic, Agent Substrate supports optional **TLS man-in-the-middle (MITM) interception** on the egress gateway (`--experimental-use-sdsmint`).

1. **Per-SNI Certificate Minting:** When an actor initiates a TLS connection through the `CONNECT` tunnel, the `sdsmint` egress gateway terminates the actor's TLS handshake and presents a dynamically minted leaf certificate for the requested SNI, signed by the gateway's internal MITM CA (`egress-mitm-ca-pool`), before re-originating TLS to the upstream destination.
2. **Trust Bundle Projection:** Because the gateway's MITM CA is not a public root CA, actors making HTTPS requests must trust the `egress-mitm.ate.dev` bundle projected via a `systemInfo` volume:

```yaml
volumes:
  - name: system-info
    systemInfo:
      dataSources:
        - trustBundle:
            name: egress-mitm.ate.dev
            path: trust-bundle.pem
```

3. **Fail-Closed Startup:** If an `ActorTemplate` declares a `trustBundle` volume and the backing `ClusterTrustBundle` is missing, empty, or unparseable, `atelet` refuses to start or restore the actor rather than running it with broken or unverified TLS trust anchors.

For full runtime environment variable configurations (`SSL_CERT_FILE`, `SSL_CERT_DIR`, `NODE_EXTRA_CA_CERTS`, `REQUESTS_CA_BUNDLE`) and troubleshooting, see **[Egress: Enabling TLS MITM Interception & Trust Bundles](../../guides/egress/#3-enabling-tls-mitm-interception--trust-bundles)**.

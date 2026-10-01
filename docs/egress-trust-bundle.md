# Enabling man-in-the-middle (MITM) interception for Actor Egress policy

## Overview

Traffic leaving an actor bound for a destination, like example.com, *always*
transits the egress gateway, but the handling is different based on the type of
egress policy rule that is allowing the traffic.

Connections allowed via a `tls_passthrough` rule is just forwarded through the
egress gateway, with no modifications.  The TLS connections is established
directly between the actor and the external destination, so the egress gateway
has no visibility into the connection (beyond metadata such as the destination).

Connections allowed via an `https` rule have TLS terminated at the egress
gateway, so that the gateway can inspect the requests and inject credentials.
The gateway then open a new TLS connection to the real destination, and sends
the modified requests.

The egress gateway does the TLS termination using server certificates it mints
on-the-fly using a built-in CA.  In order for the interception to work, the code
you run inside the actor must be explicitly configured to trust this built-in
CA.  Substrate can automatically inject the root certificates for this CA into
the actor's filesystem using a SystemInfo volume with a TrustBundle data source.

Note: DNS traffic from the actor (both TCP and UDP) is not captured by atunnel
and forwarded to the gateway.  Instead, the DNS traffic is allowed to directly
exit the worker pod and be answered by the host cluster's configured DNS server.

## Configuring a workload to trust the man-in-the-middle CA

Add a `systemInfo` volume with a `trustBundle` data source, and mount it:

```yaml
apiVersion: ate.dev/v1alpha1
kind: ActorTemplate
metadata:
  name: my-actor
  namespace: my-namespace
spec:
  volumes:
  - name: system-info
    systemInfo:
      dataSources:
      # This file will contain trust anchors for both the MITM CA and standard
      # public WebPKI CAs, meaning it will work for actors that use `https` rules,
      # `tls_passthrough` rules, or a mix.
      - trustBundle:
          names:
          - egress-mitm.ate.dev
          - system-roots.ate.dev
          path: trust-bundle.pem
  containers:
  - name: app
    image: ...
    volumeMounts:
    - name: system-info
      mountPath: /run/ate/egress-trust-bundle/
```

Substrate only offers a few built-in trust bundles with well-known names:
* `egress-mitm.ate.dev` — the egress gateway CA.
* `system-roots.ate.dev` — A selection of public CA root certificates provided
  by Substrate.  In official Substrate images, this is the Mozilla Root Store as
  shipped on Debian (consumed via the distroless-static base image).

You then need to configure your application to use the trust-bundle file.  In
general, this is application specific, but many runtimes respect the
SSL_CERT_FILE environment variables.  Here are some common scenarios and the
configuration they support

| Runtime/Library | Setting | Notes |
|---|---|---|
| Go | SSL_CERT_FILE=/run/ate/egress-trust-bundle/trust-bundle.pem |  |
| Python (most things) | SSL_CERT_FILE=/run/ate/egress-trust-bundle/trust-bundle.pem | Anything using OpenSSL's default paths: `ssl`, `urllib`, `aiohttp`, `psql` ... |
| Python (requests) | REQUESTS_CA_BUNDLE=/run/ate/egress-trust-bundle/trust-bundle.pem | |
| pip (uses requests) | REQUESTS_CA_BUNDLE=/run/ate/egress-trust-bundle/trust-bundle.pem | |
| Node.js | SSL_CERT_FILE=/run/ate/egress-trust-bundle/trust-bundle.pem | |
| Deno | DENO_CERT=/run/ate/egress-trust-bundle/trust-bundle.pem | |
| curl | SSL_CERT_FILE=/run/ate/egress-trust-bundle/trust-bundle.pem ||
| git (HTTPS remotes) | GIT_SSL_CAINFO=/run/ate/egress-trust-bundle/trust-bundle.pem ||

Note: If your application relies on an additional private CA built into the
system trust store of your base container image (for example, if your enterprise
setup has an additional layer of TLS man-in-the-middle for traffic leaving your
network), then you will find that whether or not it is respected depends on the
software you are running inside your actor.  For example, Deno and Python
requests treat their environment variable overrides as *completely replacing*
the system trust store, where as most things that support SSL_CERT_FILE treat it
as *additive* to the system trust store.  If you need to support this scenario,
you will need to use an entrypoint wrapper to build a file containing the
substrate-provided roots along with your custom roots (or directly configure
this setup in your application's startup logic).

## Verify

`demos/egress/egress-template.yaml.tmpl` is a complete working template that
does exactly this. Deploy it:

```bash
./hack/install-ate.sh --deploy-demo-egress
```

Then drive an actor's egress at an HTTPS URL an `https` rule allows and
confirm it returns a response rather than a certificate error. The minted leaf
chains to no public root, so a `200` is positive evidence that the projected
bundle did the validating.

## Operational notes

* Problems with trust bundles will block actor startup.  Error messages:
  - `... unknown trust bundle "foo"`: You tried to use an unknown trust bundle name.
  - `... trust bundle "foo": unusable contents ...`: Your substrate installation is misconfigured.
* Certificate errors are likely problems with the actor, not the gateway: If you
  see an error message like `... certificate signed by unknown authority ...`,
  the most likely cause is that your actor is not configured to trust the MITM
  CA correctly.
* This CA configuration does not help the actor authentication to the
  destination.  For that, refer to our documentation on egress credential
  injection. 

## See also

* [API Configuration Guide](api-guide.md) — the full `systemInfo` volume
  reference.
* `demos/egress/README.md` — how tunneled egress and actor-identity
  authentication fit together.
* `cmd/atenet/internal/router/README.md` — the gateway side of the MITM leg.

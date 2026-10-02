# Enabling man-in-the-middle (MITM) interception for Actor Egress policy

The egress gateway terminates the TLS connections an actor's `https` rules
allow and re-originates them. The certificate the actor sees is then not the
origin's: it is a per-SNI leaf the gateway minted, which chains to the
gateway's own CA and to no public root. An actor that validates against only
the public roots rejects it, and every such request fails with a
certificate error.

Connections a `tls_passthrough` rule allows are not terminated. The actor sees
the origin's own chain and verifies it against the public roots. An actor whose
policy mixes the two therefore has to trust **both** the gateway CA and the
public roots.

This guide covers how to project the gateway's CA into an actor's filesystem
and how to add it to the actor's trust store without losing the public roots.

Substrate discovers the ClusterTrustBundle API at startup, preferring
`certificates.k8s.io/v1` and using `certificates.k8s.io/v1beta1` only when
the stable resource is not served. The API must be enabled on the cluster;
discovery errors stop startup rather than trigger a fallback. Trust-bundle
reads, writes, and watches use the discovered version.

## DNS and egress policy

Actors send DNS queries to a relay at their sandbox's default gateway. The
relay forwards UDP and TCP DNS to the worker pod's configured resolvers,
without passing through the external egress gateway or checking egress policy.
There is currently no per-actor setting to disable this relay or filter queries.

DNS remains available when no egress gateway is configured. Other outbound TCP
connections are captured by atunnel and refused in that configuration.

## When you need this

You need it when the actor makes **HTTPS** (or any TLS) requests: the egress
gateway terminates them with a leaf it mints from its CA, which chains to no
public root.

## Project the bundle

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
      # The trust anchors for the per-SNI leaves the egress gateway mints.
      - trustBundle:
          name: egress-mitm.ate.dev
          path: trust-bundle.pem
  containers:
  - name: app
    image: ...
    volumeMounts:
    - name: system-info
      mountPath: /run/ate   # the bundle lands at /run/ate/trust-bundle.pem
```

`trustBundle.name` selects a bundle substrate knows how to fetch.

`trustBundle.path` is relative to the root of the volume, so the file's absolute path is
`mountPath` + `path`. It must be a clean relative Unix path: no leading or
trailing `/`, no `//`, `.`, or `..` segments, and at most 16 segments. A
`systemInfo` volume takes at most 8 data sources and their paths must not repeat.

The projected PEM contains `CERTIFICATE` blocks only, deduplicated and
deliberately shuffled — order carries no meaning, so do not write anything that
depends on the first block being a particular certificate.

## Point the runtime at it

Projecting the file is not enough; each TLS stack has to be told to use it.
The rule is **append, never replace**: the gateway CA is added to the public
roots, not substituted for them. A trust store that holds only the gateway CA
rejects every `tls_passthrough` origin.

### Runtimes with an additive setting

These need one environment variable and no other work.

| Runtime | Setting | Why it keeps the public roots |
|---|---|---|
| Go, stock `net/http` | `SSL_CERT_FILE=/run/ate/trust-bundle.pem` | Go reads that file in place of the default bundle, then still scans `/etc/ssl/certs`. |
| Anything on OpenSSL's default verify paths: Python `ssl`, `urllib`, `aiohttp`, `psql`, ... | `SSL_CERT_FILE=/run/ate/trust-bundle.pem` | Same: the default certificate directory is still scanned. |
| Node.js | `NODE_EXTRA_CA_CERTS=/run/ate/trust-bundle.pem` | Adds to Node's bundled roots. |
| Deno | `DENO_CERT=/run/ate/trust-bundle.pem` | Adds to Deno's bundled roots. |

Never set `SSL_CERT_DIR`. It replaces the default directory list, and with it
the public roots.

```yaml
    env:
    - name: SSL_CERT_FILE
      value: /run/ate/trust-bundle.pem
```

A Go program can also skip the variable and append the projected PEM to the
pool returned by `x509.SystemCertPool()`.

### Runtimes that take a bundle file

These treat the file they are given as the whole trust store, so they need a
file that holds the public roots **and** the gateway CA.

| Runtime | Setting |
|---|---|
| Python `requests` | `REQUESTS_CA_BUNDLE` (it ignores `SSL_CERT_FILE`) |
| Python `httpx` | `SSL_CERT_FILE`, or `httpx.Client(verify=...)` |
| pip | `PIP_CERT` |
| curl | `CURL_CA_BUNDLE` |
| git over HTTPS | `GIT_SSL_CAINFO` |

Build that file at start with an entrypoint wrapper. Set the wrapper as the
container's `command` and the real program as `args`; the projection is
read-only, so write the result somewhere writable.

```sh
#!/bin/sh
set -eu
sys=/etc/ssl/certs/ca-certificates.crt   # RHEL family: /etc/pki/tls/certs/ca-bundle.crt
ca=/run/ate/trust-bundle.pem
out=/tmp/ca-bundle.pem
{ cat "$sys"; echo; cat "$ca"; } > "$out"
export SSL_CERT_FILE="$out" REQUESTS_CA_BUNDLE="$out" CURL_CA_BUNDLE="$out" GIT_SSL_CAINFO="$out" PIP_CERT="$out"
export NODE_EXTRA_CA_CERTS="$ca" DENO_CERT="$ca"
exec "$@"
```

The `echo` keeps two PEM blocks from running together when the system bundle
lacks a trailing newline.

Java reads none of these variables. Copy the JDK's `cacerts` to a writable
path, import each certificate from the bundle with `keytool -importcert` (one
call per certificate), and pass
`-Djavax.net.ssl.trustStore=<copy>` through `JAVA_TOOL_OPTIONS`.

An image without a shell cannot run a wrapper. Use the in-code route for Go, or
pick a runtime from the first table.

Do not append the bundle to certifi's own `cacert.pem`, and do not bake it into
the image at build time: the first breaks on the next package upgrade, the
second ties the image to one cluster's CA and breaks on rotation.

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

**A bundle that does not resolve fails the actor start.** If the name is not on
the allowlist, the backing ClusterTrustBundle is missing, or the bundle is empty
or unparseable, the actor does not start — an actor that declared a trust bundle
must not run without one.

atelet logs it on the node that was going to host the actor, as the `err` field
of the interceptor's `Handle RPC` record, at INFO, with
`method=/atelet.AteomHerder/Run` (or `/atelet.AteomHerder/Restore` when a
suspended actor is coming back):

```
while populating system-info volume "system-info": system-info projection "trust-bundle.pem": trust bundle "egress-mitm.ate.dev": ClusterTrustBundle "egress-mitm.ate.dev:mitm:primary-bundle" not found
```

ateapi surfaces the same text to the caller that asked for the actor, wrapped
once by the resume step and once by gRPC:

```
while creating workload from spec: rpc error: code = Internal desc = while populating system-info volume "system-info": system-info projection "trust-bundle.pem": trust bundle "egress-mitm.ate.dev": ClusterTrustBundle "egress-mitm.ate.dev:mitm:primary-bundle" not found
```

That is the common case: projecting `egress-mitm.ate.dev` on an install where
nothing has created the `egress-mitm-ca-pool` Secret the bundle derives from. `system-info` is the volume's `name` from your
template and `trust-bundle.pem` its `path`, so those two vary with what you
wrote. The other failure modes differ only in the innermost clause:

| Cause | Innermost clause |
|---|---|
| Name not on the allowlist | `trust bundle "my-own-bundle" is not supported by this deployment (supported: egress-mitm.ate.dev)` |
| Bundle present but empty or unparseable | `trust bundle "egress-mitm.ate.dev": unusable ClusterTrustBundle "egress-mitm.ate.dev:mitm:primary-bundle": …` |

**A certificate error is a trust-store problem, not a gateway one.** Substrate
cannot see how the actor loaded the bundle, so the first signal is the
application's own error: `x509: certificate signed by unknown authority` from
Go, `CERTIFICATE_VERIFY_FAILED` from Python, exit code 60 from curl.

| Symptom | Likely cause |
|---|---|
| Fails on an `https`-rule host, `tls_passthrough` hosts work | The gateway CA is missing: projection absent, wrong path, or a variable the runtime does not read |
| Fails on a `tls_passthrough` host, `https`-rule hosts work | The public roots are missing: `SSL_CERT_DIR` is set, or a bundle-file variable points at the projection alone |
| Both fail | Wrong file, or an image with no system bundle at the path the wrapper reads |
| Worked, then fails after a resume | The CA rotated and the process still holds its old pool; restart the process |

**Rotation is picked up on the next resume.** atelet re-resolves the bundle on
both `Run` and `Restore`, so a suspended actor gets the current anchors when it
comes back. A long-running actor that never suspends keeps the copy made when it
started — and a process that has already loaded the file into memory (Go caches
its system pool after first use) will not see a change on disk either way. A
wrapper-built union file is likewise rebuilt only on a cold start. Plan CA
rotation around a resume, with an overlap window that covers the actors that do
not suspend.

**The bundle is not a substitute for authenticating the actor.** It lets the
actor verify the *gateway*. It says nothing to an origin about which actor is
calling; see `cmd/atenet/internal/router/README.md` for that direction.

## See also

* [API Configuration Guide](api-guide.md) — the full `systemInfo` volume
  reference.
* `demos/egress/README.md` — how tunneled egress and actor-identity
  authentication fit together.
* `cmd/atenet/internal/router/README.md` — the gateway side of the MITM leg.

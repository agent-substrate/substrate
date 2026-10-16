# Actor JWTs

ate-api-server mints short-lived JWTs that identify an actor, and
ate-idp-server publishes the keys that verify them. A relying party, such as
a cloud provider's token exchange or a service the actor calls, trusts a token
by checking it against the issuer's OpenID Connect discovery document. This
page describes what a token contains and how a relying party verifies it.

Tokens come from the `Control.MintActorJwt` RPC. The egress gateway calls it
to attach an actor JWT to an actor's outbound requests.

## Verifying a token

A relying party is configured with the issuer URL it trusts and the audience
it expects. To verify a token:

1. Check that the `iss` claim equals the trusted issuer.
2. Fetch `<issuer>/.well-known/openid-configuration` and check that its
   `issuer` equals the same value.
3. Fetch the document's `jwks_uri` and take the key whose `kid` matches the
   token header. Check the signature with that key and its `alg`.
4. Check that `aud` contains the expected audience, and check `exp` and `nbf`
   against the current time.

When a token names a `kid` the relying party has not seen, it should refetch
the key set: a new key is published before it signs anything.

## The token

### Header

| Field | Value |
|-------|-------|
| `typ` | `JWT` |
| `alg` | The signing key's algorithm: `ES256`, or `RS256` |
| `kid` | The signing key's ID. Treat it as an opaque string that matches one key in the key set |

### Claims

| Claim | Value |
|-------|-------|
| `iss` | The issuer URL. See [The issuer](#the-issuer) |
| `sub` | `actor/<atespace>/<name>` |
| `aud` | The audiences the caller requested, always a JSON array |
| `exp` | Expiry, in Unix seconds |
| `nbf` | Five minutes before `iat`, to allow for clock skew |
| `iat` | Issue time, in Unix seconds |
| `jti` | A random token ID |
| `actor.ate.dev` | An object with the actor's `atespace`, `actorName`, and `actorUID` |

The caller chooses the lifetime with `expiration_seconds`, which must be
between 300 and 3600. The RPC also returns `expire_time`, equal to `exp`. A
request names between 1 and 16 audiences of up to 512 characters each.
Substrate adds no audience of its own.

The subject names the actor, not one incarnation of it: an actor deleted and
recreated under the same name gets the same `sub`. A relying party that has to
tell the two apart can bind to the `actorUID` in `actor.ate.dev`, which
differs. Atespace and actor names are each up to 63 characters, so `sub` can
be up to 133 bytes long.

### Example

A decoded token, with an example key ID and UID:

```json
{
  "typ": "JWT",
  "alg": "ES256",
  "kid": "9XGqai5pBy8t4HsHIxcu68X4_e4fOg7MYyKYMHc7WPA"
}
```

```json
{
  "iss": "https://idp.ate-system.svc",
  "sub": "actor/team-a/agent-1",
  "aud": ["https://api.example.com"],
  "exp": 1760401800,
  "nbf": 1760399700,
  "iat": 1760400000,
  "jti": "MZ4RHTOXLJ6GSVNQ2YAKE7WB3C",
  "actor.ate.dev": {
    "atespace": "team-a",
    "actorName": "agent-1",
    "actorUID": "4f9c3a1e-8d2b-4c77-9e0a-6b5f1d2c3e4a"
  }
}
```

## The issuer

The issuer is `https://` followed by a host, with no path, query, or
fragment, for example `https://idp.ate-system.svc` or
`https://cluster-a.idp.example.com`. A port is allowed. ate-api-server puts it
in `iss`, and ate-idp-server serves the discovery document and key set at
fixed paths under it.

The installer sets it through the `actorJWT.issuer` setting
(`ACTOR_JWT_ISSUER`, or `--actor-jwt-issuer`; see
[operator-install.md](operator-install.md#settings)). It defaults to
`https://idp.<namespace>.svc`, the in-cluster URL of ate-idp-server. The
installer gives the same value to ate-api-server, through the
`ate-api-server-envvars` ConfigMap, and to ate-idp-server, as its `--issuer`
flag, so the token and the discovery document cannot name different issuers.
A changed issuer reaches both on the next `ate-setup deploy ate-system`.

Tokens signed before a change keep the old `iss` until they expire, at most
an hour later. A relying party that trusts only the new issuer rejects them
for that time.

## Discovery document and key set

ate-idp-server answers `GET` and `HEAD` on two paths, both as
`application/json`. The discovery document, at
`<issuer>/.well-known/openid-configuration`:

```json
{
  "issuer": "https://idp.ate-system.svc",
  "jwks_uri": "https://idp.ate-system.svc/openid/v1/jwks",
  "response_types_supported": ["id_token"],
  "subject_types_supported": ["public"],
  "id_token_signing_alg_values_supported": ["ES256"],
  "claims_supported": ["iss", "sub", "aud", "exp", "nbf", "iat", "jti", "actor.ate.dev", "worker-for-actor.ate.dev"]
}
```

`id_token_signing_alg_values_supported` lists each algorithm in the signing
pool. There is no `authorization_endpoint`, because the issuer has no login
flow.

The key set, at `<issuer>/openid/v1/jwks`, is `{"keys": [...]}` with one
entry per key in the pool, sorted by `kid`. Each entry has `kty`, `kid`,
`use` (`sig`), `alg`, and the public key: `crv` (`P-256`), `x`, and `y` for
an ES256 key, or `n` and `e` for an RS256 key. The set includes keys that are
not signing, so tokens signed by an older key keep verifying during a
rotation.

Both documents are built from the signing pool on every request.
ate-idp-server rereads the pool at most once a minute.

## Reaching the issuer

ate-idp-server runs in the system namespace behind the `idp` Service on port
443. It serves HTTPS with a certificate from the
`servicedns.podcert.ate.dev/identity` signer, which names
`idp.<namespace>.svc`. A verifier inside the cluster trusts that certificate
through the signer's ClusterTrustBundle, projected the way
[ate-controller.yaml](../manifests/ate-install/ate-controller.yaml) mounts it.

A relying party outside the cluster cannot resolve a Service name. To serve
one, put an Ingress or load balancer with a publicly trusted certificate on a
host you own, route `/.well-known/openid-configuration` and `/openid/v1/jwks`
to the `idp` Service over HTTPS, and set the issuer to `https://<that host>`.
The installer does not create the Ingress, the DNS record, or the
certificate.

## Signing keys and rotation

The signing keys live in the `actor-id-jwt-pool` Secret. One key signs at a
time, and the key set publishes all of them.

The installer creates the pool with one ES256 key. Set `actorJWT.algorithm`
(`ACTOR_JWT_ALGORITHM`) to `RS256` for a 4096-bit RSA key instead, for
relying parties that don't support ES256; the setting only affects a pool the
installer creates. A key's ID defaults to the base64url SHA-256 of its public
key's PKIX encoding, without padding. Keys created with an explicit ID keep
it.

Rotate with the `kubectl ate admin` commands, against the pool Secret
(`--name actor-id-jwt-pool --secret-namespace ate-system`):

1. `add-jwt-key` adds a key that does not sign yet. ate-idp-server publishes
   it once the updated Secret reaches its pods and it rereads the pool.
2. `activate-jwt-key --key-id <new>` makes it the signing key, once relying
   parties have refetched the key set.
3. `remove-jwt-key --key-id <old>` removes the old key, once every token it
   signed has expired.

`list-jwt-keys` shows each key's ID and algorithm and marks the one that
signs. `add-jwt-key` takes `--alg` and `--key-id` like `make-jwt-pool`.

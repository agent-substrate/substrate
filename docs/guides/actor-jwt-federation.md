---
title: "Federating Actor JWTs"
linkTitle: "Actor JWT Federation"
weight: 17
description: >
  Serve the actor JWT issuer publicly and set up a relying party outside the cluster to trust it.
---

# Federating actor JWTs

A relying party outside the cluster, such as a cloud provider's workload
identity federation, can trust actor JWTs by fetching the issuer's discovery
document and key set. This page serves the issuer where it can reach them and
lists what to configure on the relying party. [actor-jwts.md](actor-jwts.md)
describes the token and how a relying party verifies it.

## Serve the issuer publicly

A relying party outside the cluster fetches the discovery document over the
internet, so the default in-cluster issuer, `https://idp.<namespace>.svc`,
does not work.

1. Pick a host you own, for example `idp.example.com`, and put an Ingress,
   Gateway, or load balancer on it with a publicly trusted WebPKI certificate.
2. Route `/.well-known/openid-configuration` and `/openid/v1/jwks` to the `idp`
   Service in the system namespace, port 443, over HTTPS. The pods present a
   certificate from the cluster's `servicedns.podcert.ate.dev/identity`
   signer, not a public CA, so have the load balancer trust that signer's
   ClusterTrustBundle, or skip backend verification if it can't.
3. Point the load balancer's health check at `/healthz` over HTTPS. The other
   paths return 404.
4. Redeploy with the issuer set. Pass the record of your last install so the
   run keeps its other settings; the flag overrides the record:

   ```sh
   ate-setup deploy ate-system \
     --config ~/.cache/ate-setup/installs/<context>.yaml \
     --actor-jwt-issuer=https://idp.example.com
   ```

   The record doesn't include secrets, such as database connection strings, so
   you need to pass secrets again. See
   [Install runs record what they used](operator-install.md#install-runs-record-what-they-used)
   for where records are kept.

5. Check from outside the cluster that the document names the same issuer:

   ```sh
   curl -s https://idp.example.com/.well-known/openid-configuration | jq -r .issuer
   ```

Tokens signed before the change keep the old `iss` for up to an hour.

## Set up the relying party

Configure the relying party with:

- The issuer, exactly as the discovery document names it.
- The audience it expects. The egress policy's `actorJwt.audiences` has to
  include it; see [Actor JWTs](egress-credential-injection.md#actor-jwts) in
  the egress guide.
- Authorization rules for the subjects that should have access. An actor's
  `sub` is `actor/<atespace>/<name>`.

If the relying party doesn't accept ES256,
[switch the pool to RS256](actor-jwt-key-rotation.md#changing-the-algorithm).

To check the setup, have an egress policy inject a token for the relying
party's audience into a request to a server you control, and present that
token to the relying party. Substrate can't yet present an actor JWT to a
cloud token exchange endpoint itself; see
[Known limitations](actor-jwts.md#known-limitations).

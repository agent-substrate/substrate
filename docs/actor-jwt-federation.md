# Federating actor JWTs with Google Cloud

Google Cloud Workload Identity Federation can trust an external OpenID Connect
issuer: its Security Token Service fetches the issuer's discovery document and
key set, verifies a JWT presented to it, and returns a short-lived Google
credential for the identity the JWT names. This page sets up that trust for
actor JWTs, so an actor can be granted Google Cloud access by name instead of
through a long-lived key. [actor-jwts.md](actor-jwts.md) describes the token.

Substrate does not present actor JWTs to the Security Token Service itself.
The egress gateway [injects them into request headers](egress-credential-injection.md),
and the service takes the token in the request body. The setup ends with the
exchange request, which you can send by hand to check it. It needs an actor
JWT for the provider's audience; to get one, have an egress policy inject it
into a request to a server you control.

## Serve the issuer publicly

Google fetches the discovery document from the internet, so the default
in-cluster issuer, `https://idp.<namespace>.svc`, does not work.

1. Pick a host you own, for example `idp.example.com`, and put an Ingress,
   Gateway, or load balancer on it with a publicly trusted certificate.
2. Route `/.well-known/openid-configuration` and `/openid/v1/jwks` to the `idp`
   Service in the system namespace, port 443, over HTTPS. The pods present a
   certificate from the cluster's `servicedns.podcert.ate.dev/identity`
   signer, not a public CA, so have the load balancer trust that signer's
   ClusterTrustBundle, or skip backend verification if it can't.
3. Point the load balancer's health check at `/healthz` over HTTPS. The other
   paths return 404.
4. Set the issuer and redeploy:

   ```sh
   ate-setup deploy ate-system --actor-jwt-issuer=https://idp.example.com
   ```

5. Check from outside the cluster that the document names the same issuer:

   ```sh
   curl -s https://idp.example.com/.well-known/openid-configuration | jq -r .issuer
   ```

Tokens signed before the change keep the old `iss` for up to an hour.

## Set up Workload Identity Federation

Workload Identity Federation accepts ES256 and RS256 tokens. Create a pool and
an OIDC provider in it that trusts the issuer:

```sh
PROJECT_NUMBER=123456789012
POOL=projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/substrate

gcloud iam workload-identity-pools create substrate --location=global
gcloud iam workload-identity-pools providers create-oidc actors \
  --location=global \
  --workload-identity-pool=substrate \
  --issuer-uri=https://idp.example.com \
  --attribute-mapping="google.subject=assertion.sub.extract('actor/{actor}'),attribute.atespace=assertion['actor.ate.dev'].atespace" \
  --attribute-condition="assertion.sub.startsWith('actor/')"
```

- `google.subject` is limited to 127 bytes and `sub` can be 133, so the
  mapping drops the `actor/` prefix: an actor's Google subject is
  `<atespace>/<name>`.
- The condition admits only actor tokens, not `worker-for-actor` ones.
- Without `--allowed-audiences`, the token's `aud` must be the provider's
  name, `https://iam.googleapis.com/$POOL/providers/actors`.

Grant a role to one actor, or to every actor in an atespace:

```sh
gcloud storage buckets add-iam-policy-binding gs://my-bucket \
  --role=roles/storage.objectViewer \
  --member="principal://iam.googleapis.com/$POOL/subject/team-a/agent-1"

gcloud storage buckets add-iam-policy-binding gs://my-bucket \
  --role=roles/storage.objectViewer \
  --member="principalSet://iam.googleapis.com/$POOL/attribute.atespace/team-a"
```

Some Google APIs don't accept federated identities directly. For those, grant
the same member `roles/iam.workloadIdentityUser` on a service account and
impersonate it.

Check the setup by exchanging a token at the Security Token Service. A
response with an `access_token` means Google trusts it:

```sh
jq -n --arg aud "//iam.googleapis.com/$POOL/providers/actors" --arg token "$TOKEN" '{
  grantType: "urn:ietf:params:oauth:grant-type:token-exchange",
  audience: $aud,
  scope: "https://www.googleapis.com/auth/cloud-platform",
  requestedTokenType: "urn:ietf:params:oauth:token-type:access_token",
  subjectTokenType: "urn:ietf:params:oauth:token-type:jwt",
  subjectToken: $token
}' | curl -s https://sts.googleapis.com/v1/token -H 'Content-Type: application/json' -d @-
```


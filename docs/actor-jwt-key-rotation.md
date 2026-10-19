# Rotating the actor JWT signing key

ate-api-server signs actor JWTs with the active key in the `actor-id-jwt-pool`
Secret, and ate-idp-server publishes every key in the pool. A rotation adds a
key, waits until relying parties have it, makes it the signing key, and
removes the old key once nothing it signed can still be presented.
[actor-jwts.md](actor-jwts.md#signing-keys-and-rotation) describes the pool.

The commands below use the default Secret. Set `ISSUER` to your
`actorJWT.issuer`:

```sh
ISSUER=https://idp.example.com
POOL="--name actor-id-jwt-pool --secret-namespace ate-system"
```

## How long each step takes

- **Reaching the pods.** A change to the Secret reaches the ate-api-server and
  ate-idp-server pods when the kubelet next syncs the mounted Secret, and each
  process rereads the pool within a minute after that. Allow a few minutes.
- **Reaching relying parties.** Each relying party caches the key set for its
  own period, and Google Cloud doesn't document its period. Unless every
  relying party documents a shorter one, wait a day.
- **Tokens in flight.** A token is valid for up to an hour. The egress gateway
  reuses a token until a third of its lifetime is left, so a token signed just
  before the new key took over can still be presented, and has to verify, for
  up to an hour after that.

## Routine rotation

1. Note the active key:

   ```sh
   kubectl ate admin list-jwt-keys $POOL
   ```

2. Add the new key. It is published but signs nothing yet. Pass `--alg RS256`
   to change the algorithm; see [Changing the algorithm](#changing-the-algorithm).

   ```sh
   kubectl ate admin add-jwt-key $POOL
   ```

3. Wait until the key set lists the new key ID:

   ```sh
   curl -s "$ISSUER/openid/v1/jwks" | jq -r '.keys[].kid'
   ```

   Then wait out the longest key set cache among your relying parties.

4. Make the new key the signing key:

   ```sh
   kubectl ate admin activate-jwt-key $POOL --key-id <new>
   ```

5. Wait at least an hour, plus a few minutes for the change to reach every
   ate-api-server pod.

6. Remove the old key:

   ```sh
   kubectl ate admin remove-jwt-key $POOL --key-id <old>
   ```

## Changing the algorithm

Follow the same steps and pass `--alg` to `add-jwt-key`, for example to move
to RS256 for a relying party that doesn't accept ES256. Every relying party
has to accept the new algorithm before step 4. During the overlap the
discovery document's `id_token_signing_alg_values_supported` lists both.

The `actorJWT.algorithm` setting only affects a pool the installer creates, so
rotating is the way to change an existing install.

## Replacing a compromised key

Skip the waits:

1. Add a new key and activate it right away. Relying parties that haven't
   fetched the new key reject tokens it signs until they refetch the key set.
2. Once the activation has reached every ate-api-server pod, remove the
   compromised key.
3. Restart the egress gateway so it drops the tokens it cached:

   ```sh
   kubectl rollout restart deployment/atenet-egress -n ate-system
   ```

A relying party keeps accepting the compromised key until it refetches the key
set. Google Cloud doesn't document a way to make it refetch.

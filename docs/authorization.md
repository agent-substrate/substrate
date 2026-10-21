# ate-api Authorization

> [!WARNING]
> **Do not keep `ate-client` as the bootstrap owner outside development.**
> `ate-setup` makes the shared `ate-client` ServiceAccount the default
> bootstrap owner so the demos, benchmarks and e2e tests work. Anyone who can
> mint a token for it, including every `kubectl ate` user who doesn't pass
> `--token-file`, is then a global owner. For a shared or production install,
> set [bootstrap owners](#bootstrap-owners) to your operators' own principals.

`ate-api-server` checks every API call against an
[OpenFGA](https://openfga.dev) model before running it. OpenFGA is embedded in
`ate-api-server` and stores its data in the same PostgreSQL database. For how
callers authenticate, see [authentication.md](authentication.md).

## How a call is checked

1. **Authentication** turns the credential into a principal ID, or rejects it
   with `Unauthenticated`.
2. **Authorization** allows the call if the principal is a bootstrap owner,
   holds a role from an access policy that permits it, or is a Substrate
   component with the system role it needs. Otherwise, or if the RPC has no
   rule, it returns `PermissionDenied`.

## Settings

Authorization is always enforced. `ate-setup` reads the setting below and
passes it to `ate-api-server` as the `--authz-bootstrap-owners` flag on its
container; an install that doesn't use `ate-setup` sets that flag in its
manifest. `ate-api-server` won't start without a bootstrap owner. A change
rolls `ate-api-server`.

| Setting | Environment variable | Example |
|---|---|---|
| `ateapi.authz.bootstrapOwners` | `ATE_API_AUTHZ_BOOTSTRAP_OWNERS` | `alice@example.com,bob@example.com` |

If you don't set `ateapi.authz.bootstrapOwners`, `ate-setup` uses the shared
`ate-client` ServiceAccount (`system:serviceaccount:<namespace>:ate-client`),
so the demos, benchmarks and e2e tests work. See the warning at the top of
this page.

## Principals

A principal is the ID authentication produces. Access policies name it as
`user:<id>`; bootstrap owners list the bare `<id>`.

| Caller | `<id>` |
|---|---|
| Kubernetes ServiceAccount token | `system:serviceaccount:<namespace>:<name>` |
| Other JWT provider, such as Google | The token's `sub` (for Google, a numeric account ID, not the email) |
| Pod certificate (mTLS) | Its SPIFFE ID, e.g. `spiffe://cluster.local/ns/benchmarking/sa/default` |

A denied call names the principal in the error with `:` percent-encoded
(`user:system%3Aserviceaccount%3A...`); decode it before using it.

## Bootstrap owners

Bootstrap owners always hold `owner` on the global scope, whatever the access
policies say. They write an install's first access policies and recover access
if those break.

- They come from configuration, not the API. `ate-api-server` reads them at
  startup and exits if the list is empty. Restarting
  it with a new list takes effect immediately.
- Any principal `ate-api-server` can authenticate can be one.
- `ate-setup` warns whenever `ate-client` is one.
- Keep the list short. Grant day-to-day admins `owner` in the global access
  policy instead, where it can be revoked through the API.

For example, with a JWT provider whose tokens carry the user's email as `sub`:

```sh
ATE_API_AUTHZ_BOOTSTRAP_OWNERS=alice@example.com,bob@example.com hack/install-ate.sh --deploy-ate-system
```

To make your Google account a bootstrap owner, use its numeric `sub`:

```sh
gcloud auth print-identity-token | cut -d. -f2 | base64 -d 2>/dev/null | jq -r .sub   # your sub
ATE_API_AUTHZ_BOOTSTRAP_OWNERS=<sub>,system:serviceaccount:ate-system:ate-client \
  hack/install-ate.sh --deploy-ate-system   # keep ate-client for demos and benchmarks
```

Then add the Google provider to the `ate-api-authentication` ConfigMap and
restart `ate-api-server`
([authentication.md](authentication.md#google-cloud-cli-tokens)), and act as
yourself:

```sh
gcloud auth print-identity-token > me.token
kubectl ate --token-file me.token get atespaces
```

## `kubectl ate` and `ate-setup` identities

`kubectl ate` sends the token from `--token-file` (`-` for stdin). Without
one, it uses your kubeconfig to mint a token for `ate-client` (or
`ATE_CLIENT_SERVICE_ACCOUNT`) and warns that it is acting as a shared
identity. It still needs Kubernetes rights to port-forward to `ate-api-server`
unless you pass `--endpoint`.

`ate-setup` calls the API only to deploy and delete demos and benchmarks, as
`ate-client`. If you leave `ate-client` out of the bootstrap owners, grant it
access in a policy or those steps are denied.

## Roles

Roles are granted on the **global** scope, which covers every atespace, or on
one **atespace**.

| Permission | Global owner | Global viewer | Atespace owner | Atespace editor | Atespace viewer |
|---|:-:|:-:|:-:|:-:|:-:|
| Create atespaces; list all atespaces | ✅ | list only | | | |
| Get the atespace | ✅ | ✅ | ✅ | ✅ | ✅ |
| Delete the atespace | ✅ | | ✅ | | |
| Get access policies | ✅ | ✅ | ✅ (its own) | ✅ (its own) | ✅ (its own) |
| Create, update, delete access policies | ✅ | | ✅ (its own) | | |
| Get and list actors, actor templates, tags, egress policies | ✅ | ✅ | ✅ | ✅ | ✅ |
| Create, update, delete actors and actor templates | ✅ | | ✅ | ✅ | |
| Pause, resume, suspend, revert actors | ✅ | | ✅ | ✅ | |
| Create, update, delete egress policies | ✅ | | ✅ | ✅ | |
| Create, publish, delete tags | ✅ | | ✅ | ✅ | |
| Create an actor from a tag (`sourceTag`) | ✅ | | ✅ | ✅ | |
| Workers: get, list, list assignments | ✅ | ✅ | | | |
| Workers: create, update, delete, drain | ✅ | | | | |

### System roles

Substrate's components hold roles that access policies cannot grant, and only
through their pod certificates. `ate-api-server`'s `--controller-spiffe-id`,
`--egress-gateway-spiffe-id`, `--ingress-router-spiffe-id` and
`--api-server-sidecar-spiffe-id` flags bind them, defaulting to the install's;
`atelet`'s comes from `--atelet-service-account`.

| Role | Held by | Allows |
|---|---|---|
| `controller` | `ate-controller` | Create, get, list, update, delete and drain workers |
| `egress_gateway` | `atenet-egress` | Get actors and their egress policies; list actors across all atespaces; mint actor JWTs and certificates |
| `ingress_router` | `atenet-router` | Resume actors; list actors across all atespaces |
| `atelet` | `atelet` | Get actors' egress policies, for the worker that enforces them |
| `api_server_sidecar` | the `ate-api-server` pod identity, which its sidecars share, today the objectstore control plugin | Get actors, to derive the snapshot prefixes a per-actor storage credential may cover |

Only `egress_gateway` can mint actor credentials: not even a global owner can,
since a credential lets its holder act as the actor.

## Checks per RPC

A gRPC interceptor makes every check before the handler runs
([`registry.go`](../cmd/ateapi/internal/authz/registry.go),
[`model.fga`](../cmd/ateapi/internal/authz/model.fga)). When an RPC lists
several checks, all must pass.

Roles inherit: an owner is also an editor and a viewer, an editor is also a
viewer, and a global role holds the same role in every atespace. So
"atespace editor" below also means atespace owner, global owner, and so on.

### Atespaces and access policies

| RPC | Permission, on | Allowed for |
|---|---|---|
| `CreateAtespace` | `can_create_atespace`, global | Global owner |
| `ListAtespaces` | `can_list_atespaces`, global | Global viewer |
| `GetAtespace` | `can_get`, the atespace | Atespace viewer |
| `DeleteAtespace` | `can_delete`, the atespace | Atespace owner |
| `GetGlobalAccessPolicy` | `can_get_access_policy`, global | Global viewer |
| `CreateGlobalAccessPolicy`, `UpdateGlobalAccessPolicy` | `can_create_access_policy` / `can_update_access_policy`, global | Global owner |
| `GetAtespaceAccessPolicy` | `can_get_access_policy`, the atespace | Atespace viewer |
| `CreateAtespaceAccessPolicy`, `UpdateAtespaceAccessPolicy`, `DeleteAtespaceAccessPolicy` | `can_create_access_policy` / `can_update_access_policy` / `can_delete_access_policy`, the atespace | Atespace owner |

### Actor templates and actors

| RPC | Permission, on | Allowed for |
|---|---|---|
| `CreateActorTemplate` | `can_create_actor_template`, `metadata.atespace` | Atespace editor |
| `GetActorTemplate` | `can_get`, the template | Viewer of its atespace |
| `DeleteActorTemplate` | `can_delete`, the template | Editor of its atespace |
| `ListActorTemplates` | `can_list_actor_templates`, the atespace, or global if none is given | Atespace viewer; global viewer for all atespaces |
| `CreateActor` | 1. `can_create_actor`, the actor's atespace<br>2. `can_use_template`, the actor template<br>3. `can_use_tag`, the `sourceTag`, if one is given | 1. Atespace editor<br>2. Viewer of the template's atespace, which may be another one<br>3. Editor of the tag's atespace. Golden tags skip this check: the handler only accepts one built from the actor's own template. |
| `UpdateActor` | 1. `can_update`, the actor<br>2. `can_use_template`, the template in the request | 1. Editor of its atespace<br>2. Viewer of the template's atespace, so an actor can't be switched to a template its editor can't use |
| `GetActor` | `can_get`, the actor | Viewer of its atespace; `atenet-egress`; the objectstore control plugin |
| `DeleteActor` | `can_delete`, the actor | Editor of its atespace |
| `ListActors` | `can_list_actors`, the atespace, or global if none is given | Atespace viewer; for all atespaces, global viewer, `atenet-router` or `atenet-egress` |
| `SuspendActor`, `RevertActor` | `can_suspend` / `can_revert`, the actor | Editor of its atespace |
| `ResumeActor` | `can_resume`, the actor | Editor of its atespace; `atenet-router` |
| `GetActorEgressPolicy` | `can_get_egress_policy`, the actor | Viewer of its atespace; `atenet-egress`; `atelet` |
| `CreateActorEgressPolicy`, `UpdateActorEgressPolicy`, `DeleteActorEgressPolicy` | `can_create_egress_policy` / `can_update_egress_policy` / `can_delete_egress_policy`, the actor | Editor of its atespace |
| `MintActorJwt`, `MintActorCertificate` | `can_mint_actor_jwt` / `can_mint_actor_certificate`, the actor | `atenet-egress` only |

### Tags

| RPC | Permission, on | Allowed for |
|---|---|---|
| `CreateTag` | `can_create_tag`, the source actor's atespace, where the tag is stored | Atespace editor |
| `GetTag` | `can_get`, the tag | Viewer of its atespace |
| `UpdateTag` (including publishing) | `can_update`, the tag | Editor of its atespace |
| `DeleteTag` | `can_delete`, the tag | Editor of its atespace |
| `ListTags` | `can_list_tags`, the atespace, or global if none is given | Atespace viewer; global viewer for all atespaces |

### Workers

Workers have no atespace; their permissions come from the global scope.

| RPC | Permission, on | Allowed for |
|---|---|---|
| `CreateWorker` | `can_create_worker`, global | Global owner; `ate-controller` |
| `ListWorkers` | `can_list_workers`, global | Global viewer; `ate-controller` |
| `GetWorker` | `can_get`, the worker | Global viewer; `ate-controller` |
| `UpdateWorker`, `MarkWorkerAvailable`, `MarkWorkerUnavailable` | `can_update`, the worker | Global owner; `ate-controller` |
| `DeleteWorker` | `can_delete`, the worker | Global owner; `ate-controller` |
| `DrainWorker` | `can_drain`, the worker | Global owner; `ate-controller` |
| `ListWorkerActorAssignments` | `can_list_actor_assignments`, the worker | Global viewer |

### WorkerService

The interceptor skips these; each handler requires `atelet`'s pod
certificate.

| RPC | The caller must be |
|---|---|
| `RegisterWorker` | `atelet` on the worker's node |
| `RequestActorSuspend` | `atelet` on the node of the worker hosting the actor |
| `MintWorkerActorJwt` | `atelet` on the node hosting the actor |
| `MintWorkerActorCertificate` | `atelet` on the node of the worker the actor is assigned to |
| `ReportActorSnapshotState` | `atelet` on the node the actor is placed on or holds a local snapshot of it |

### Every RPC

- An RPC with no rule is denied, and a test fails the build if a served RPC
  has none.
- An unauthenticated caller gets `Unauthenticated` before its request is read.
- A missing or invalid resource name returns `InvalidArgument`; no check is
  skipped.

## Managing access

Each scope has one access policy, named `default`, managed with `kubectl ate`
using `--global` or `-a <atespace>`:

```yaml
# policy.yaml
metadata:
  name: default
bindings:
- role: editor
  members:
  - user:system:serviceaccount:team-a:alice
- role: viewer
  members:
  - user:system:serviceaccount:team-a:bob
```

```sh
kubectl ate --token-file me.token create access-policy -a team-a -f policy.yaml
kubectl ate --token-file me.token get access-policy -a team-a -o yaml > current.yaml
kubectl ate --token-file me.token update access-policy -a team-a -f current.yaml
kubectl ate --token-file me.token delete access-policy -a team-a
```

- Global policies accept `owner` and `viewer`; atespace policies also accept
  `editor`.
- `update` replaces the whole policy and needs the `uid` and `version` from
  `get -o yaml`. It fails if the policy changed, or was deleted and recreated,
  since.
- The global policy can't be deleted; update it with no bindings instead.
- Deleting an atespace deletes its policy, so a recreated atespace starts
  with none.
- Changes take effect on the next call.

## Recovering access

If no principal anyone can authenticate as holds `owner` (for example, a
Google account listed by email instead of its numeric `sub`), anyone who can
redeploy `ate-api-server` can recover: redeploy with a bootstrap owner they
can use, fix the policies, then redeploy with the intended owners.

```sh
ATE_API_AUTHZ_BOOTSTRAP_OWNERS=alice@example.com hack/install-ate.sh --deploy-ate-system
```

## Production readiness

For a shared cluster, beyond replacing the default bootstrap owner:

- **Give people their own identities** in access policies, and have them pass
  `--token-file` with a token from a trusted JWT provider or their own
  ServiceAccount. To let a user mint tokens only for their own ServiceAccount:

  ```yaml
  apiVersion: rbac.authorization.k8s.io/v1
  kind: Role
  metadata: {name: mint-alice-token, namespace: team-a}
  rules:
  - apiGroups: [""]
    resources: ["serviceaccounts/token"]
    resourceNames: ["alice"]
    verbs: ["create"]
  ```

  ```sh
  kubectl create token -n team-a alice --audience api.ate-system.svc > alice.token
  kubectl ate --token-file alice.token get actors -a team-a
  ```

- **Limit who can act as a ServiceAccount bootstrap owner.** Anyone who can
  create `serviceaccounts/token`, pods or Secrets in its namespace, or change
  RBAC there, can get its token. A
  `kubernetes.io/service-account-token` Secret gets one that never expires.
  Check who can, and alert on the audit events:

  ```sh
  kubectl auth can-i create serviceaccounts/ate-client --subresource=token -n ate-system --as alice@example.com
  ```

### Not yet supported

- **Signing in.** `kubectl ate` has no login flow or credential plugin; people
  obtain tokens themselves.
- **JWT providers at install time.** `ate-setup` configures only the cluster's
  ServiceAccount issuer; add others to the ConfigMap afterwards.
- **Groups.** Policy members are individual principals.

## Known limitations

- **Tags can copy an actor's data.** A tag is a durable copy of an actor's
  memory and disk, including secrets, and outlives the actor. Any atespace
  editor can tag, publish and clone any actor in the atespace.
- **Principals are not qualified by issuer.** If `ate-api-server` trusts more
  than one JWT provider, any of them that can issue a token whose `sub` is
  another principal's ID, a bootstrap owner's included, acts as that principal.
  System roles are unaffected: they need the component's pod certificate.
- **The atespace is the only boundary.** There are no per-actor or
  per-template grants.
- **Any atelet can read any actor's egress policy.** Every node's atelet
  shares one identity, so the `atelet` role isn't limited to actors on its
  node ([#1564](https://github.com/agent-substrate/substrate/issues/1564)).

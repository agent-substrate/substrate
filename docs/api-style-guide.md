# Substrate gRPC API Style Guide

This document is the authoritative style guide for Substrate public APIs. Today this only includes `ate-apiserver` API.

This guide is derived from [Google's AIP (API Improvement Proposals)](https://google.aip.dev/) and adopts the large majority of its conventions. Divergences are called out explicitly with rationale.

---

## 1. Resource-Oriented Design

*Follows [AIP-121](https://google.aip.dev/121).*

APIs are structured around **resources** (nouns) and a small set of standard **methods** (verbs). Standard methods are Get, List, Create, Update, and Delete. **Custom methods** handle operations that don't fit these patterns (e.g., Suspend, Resume, Pause for Actors).

Rules:
- Every primary noun the API exposes is a resource.
- Standard methods are strongly preferred. Custom methods are the exception, not the norm.
- The resource schema must be identical across all standard methods that reference it (i.e., Get, Create, Update, and Delete all return the same `Actor` message).

---

## 2. Resource Naming and Identity

**Diverges from [AIP-122](https://google.aip.dev/122).**

AIP-122 identifies resources by a single opaque path string (e.g., `publishers/123/books/les-miserables`). Substrate uses a **two-field identity** instead: an `atespace` (namespace) and a `name`. This is analogous to how Kubernetes identifies objects and avoids the ambiguity of parsing hierarchical path strings.

Resources are either **atespace-scoped** or **global-scoped**. Scope is a fixed property of the resource type, not of individual instances. For example, `Actor` resources
are **atespace-scoped**, whereas `Atespace` resources are naturally **global-scoped**.

### 2.1 Identity and scope

* Atespace-scoped resources belong to an atespace. Their identity is `(atespace, name)`, unique within the resource type.
* Global-scoped resources are global across the entire deployment and do not belong to any atespace. For these, the identity is `name` alone.

In both cases, a `metadata` field contains both `atespace` and `name`. For global resources, the `atespace` must always be empty.

```proto
message Actor {
  // Common resource metadata: atespace, name, and other standard fields (see section #6).
  ResourceMetadata metadata = 1;

  // ... other fields
}


message ResourceMetadata {
  // The atespace this resource belongs to. Empty if the resource has global-scope.
  string atespace = 1;
  // The name of this resource, unique within its atespace (or globally, for global-scoped resources).
  string name = 2;

  // ... other common resource fields
}
```

- All resources in Substrate must have a `ResourceMetadata metadata = 1` field to hold common fields, which includes both `atespace` and `name`.
- If the resource type has global-scope, the `atespace` field must be always empty.

### 2.2 Character constraints

Both `atespace` and `name` must be valid resource names.
A valid resource name must comply with the following rules:

- Lowercase alphanumeric characters and hyphens only.
- Must start with a lowercase alphanumeric character.
- Must end with a lowercase alphanumeric character.
- Maximum 63 characters.
- Regex: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`

Resource names are valid RFC-1123 DNS labels.

### 2.3 `ObjectRef` — reference type

The `ObjectRef` message represents a *pointer* to a Substrate resource.

```proto
message ObjectRef {
  string atespace = 1;
  string name     = 2;
}
```

Use `ObjectRef` in places where you need to reference a resource. For example:

**1. Request messages** — to identify which specific resource to act on:

```proto
message GetActorRequest {
  ObjectRef actor = 1;
}

message DeleteActorRequest {
  ObjectRef actor   = 1;

  // ... other fields
}
```

**2. Cross-resource references** — when one resource's fields refer to another atespace-scoped resource:

```proto
message Actor {
  ResourceMetadata metadata = 1;

  // The ActorTemplate this actor was derived from.
  ObjectRef actor_template = 2;

  // ... other fields
}
```

The field name is the logical name of the reference (e.g., `actor_template`), not `actor_template_name` or `actor_template_ref`.
Note that this assumes that ActorTemplates are also resources in the substrate gRPC API (not in KRM).

- Do not embed the full resource message as a reference field.
- Do not use a single combined string like `"atespace/name"`. Callers would have to parse it.
- Do not use a plain `string {resource}_name` field for global-scoped references — use `ObjectRef` for consistency and type safety.

TODO: Decide the convention for cross-references that point to a resource in the *same* atespace as the referrer: whether the caller must fill in `atespace` explicitly, or leaves it empty and the server resolves/validates it against the referrer's atespace. Current leaning is to require it be filled in.

---

## 3. Standard Methods

The following sections cover each standard method. The primary adaptation from AIP-13x is how resources are identified in requests: an `ObjectRef` field instead of a `name` path string.

### 3.1 Get

*Follows [AIP-131](https://google.aip.dev/131).*

```proto
rpc GetActor(GetActorRequest) returns (Actor) {}

message GetActorRequest {
  ObjectRef actor = 1;
}
```

Rules:
- RPC name **must** begin with `Get` followed by the singular resource name.
- Request message name **must** match the RPC name with a `Request` suffix.
- Response **must** be the resource itself — not a `GetActorResponse` wrapper.
- Request **must** identify the resource with a single `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- That field **must** be named after the resource's own snake_case type name (e.g. `actor` for `Actor`, `actor_snapshot` for `ActorSnapshot`), not a generic name like `name` or `ref`.
- If the resource does not exist: return `NOT_FOUND`.

### 3.2 List

*Follows [AIP-132](https://google.aip.dev/132).*

```proto
rpc ListActors(ListActorsRequest) returns (ListActorsResponse) {}

message ListActorsRequest {
  // The atespace to list actors from.
  string atespace = 1;

  // Maximum number of actors to return. The server may return fewer.
  // If unspecified, defaults to a server-chosen value.
  // The maximum value is 1000; values above 1000 are coerced to 1000.
  int32 page_size = 2;

  // Pagination token from a previous ListActors response.
  // Omit or leave empty for the first request.
  string page_token = 3;
}

message ListActorsResponse {
  repeated Actor actors = 1;

  // Pagination token for the next page.
  // Empty if this is the last page.
  string next_page_token = 2;
}
```

Rules:
- RPC name **must** begin with `List` followed by the **plural** resource name.
- Both the request and response message names **must** match the RPC name with `Request`/`Response` suffixes. (Unlike Get/Create/Update, List responses are not the resource itself.)
- `next_page_token` **must** be present on every List response message. It **must** be empty when there are no further pages.
- The repeated resource field **must** use the plural form of the resource name (e.g., `actors`, not `actor`).
- If a user provides a `page_size` above the maximum, coerce it silently. If a user provides a negative value, return `INVALID_ARGUMENT`.
- Sorting and filtering as specified in AIP-132 are not supported.
- Clients must iterate over all pages until an empty `next_page_token` is returned. Clients should not assume that an empty result (i.e. len(actors) == 0) means the end of the stream.

### 3.3 Create

*Adapted from [AIP-133](https://google.aip.dev/133).*

```proto
rpc CreateActor(CreateActorRequest) returns (Actor) {}

message CreateActorRequest {
  // The actor to create.
  // actor.metadata.atespace and actor.metadata.name together specify the resource's identity
  // and must both be set by the caller.
  Actor actor = 1;
}
```

Rules:
- RPC name **must** begin with `Create` followed by the singular resource name.
- Response **must** be the resource itself — not a `CreateActorResponse` wrapper.
- The embedded resource field **must** be named after the resource's own snake_case type name (e.g. `actor` for `Actor`), not a generic name like `resource` or `body`.
- `actor.metadata.atespace` and `actor.metadata.name` are **required** and caller-specified. The server does not generate them.
- Other meta fields such as `uid`, timestamps, `version`, etc, are server side generated, and ignored when specified.
- If a resource already exists with the same `(atespace, name)`: return `ALREADY_EXISTS`.
- `actor.metadata.atespace` must be specified iff the resource type is atespace-scoped, otherwise the service must return `INVALID_ARGUMENT`.
- Non-resource "control" fields (e.g. dry-run, idempotency token) belong in a shared `CreateOptions` message embedded as an `options` field, following the `DeleteOptions` pattern (section #3.5) — not as loose top-level fields on the request.

**Divergence from AIP-133:** AIP-133 separates `parent` + `{resource}_id` from the resource body because AIP-122 makes the resource `name` field output-only (constructed by the server from the parent path). In Substrate's model, `atespace` and `name` are directly caller-specified identity fields on the resource, so duplicating them at the top level of the request adds no information and creates ambiguity about which one wins. The embedded resource is the single source of truth for identity on create.

### 3.4 Update

*Follows [AIP-134](https://google.aip.dev/134). Diverges by replacing the resource instead of patching it.*

Updates use a **full-replacement model** (equivalent to HTTP PUT). The resource carried in the request *is* the resource the client wants to exist. Server-managed
fields are ignored.

```proto
rpc UpdateActor(UpdateActorRequest) returns (Actor) {}

message UpdateActorRequest {
  // The actor to update.
  // actor.metadata.atespace and actor.metadata.name identify which resource to update.
  // actor.metadata.uid and actor.metadata.version are the preconditions the
  // update is written against, and are required.
  //
  // Every other field of the actor is replaced with what the request carries.
  // A field the request leaves unset is cleared, so a field that is immutable
  // and unset is rejected rather than silently dropped.
  Actor actor = 1;
}
```

Clients update by read-modify-write: `Get` the resource, change the fields they mean to change, and send the whole thing back. The `uid` and `version` guards they echo back make that round trip safe against concurrent writers.

Rules:
- RPC name **must** begin with `Update` followed by the singular resource name.
- Response **must** be the resource itself — not an `UpdateActorResponse` wrapper.
  - **Output-only** fields — server-managed, never set by the client (`uid`, `version`, `create_time`, `update_time`, and the whole `status` submessage). Whatever the request carries in them is ignored and the server's own values are kept.
  - **Immutable** fields — caller-set at creation but fixed thereafter (`atespace`, `name`, and resource-specific ones such as an actor's `actor_template_name`). A request that changes one - including by omitting it, which would clear it - **must** return `INVALID_ARGUMENT` naming the field.
- The embedded resource field **must** be named after the resource's own snake_case type name (e.g. `actor` for `Actor`), not a generic name like `resource` or `body`.
- Unknown fields in the request are preserved, so a client built against a newer schema does not lose data by round-tripping through an older one.
- The resource's `atespace` and `name` identify the resource to update; they are not themselves updatable.
- If the resource does not exist: return `NOT_FOUND`.
- The `version` and `uid` fields in the embedded resource's `metadata` are **required** preconditions (see section #7). A request that leaves either unset is a blind write and **must** return `INVALID_ARGUMENT`. They are control fields the client echoes back as guards, not fields it edits.

### 3.5 Delete

*Follows [AIP-135](https://google.aip.dev/135). Diverges on return type.*

```proto
rpc DeleteActor(DeleteActorRequest) returns (Actor) {}

message DeleteActorRequest {
  ObjectRef actor = 1;

  // Optional per-delete options. Reused across every Delete<Type>Request.
  DeleteOptions options = 2;
}

// DeleteOptions carries per-delete controls. Today it holds optional
// preconditions that guard against acting on a resource that is not in the
// state the caller expects (see section #7). Future delete-specific controls
// (e.g. dry-run) should be added here.
message DeleteOptions {
  // If non-zero, delete only if the server's current version matches.
  int64 version = 1;
  // If non-empty, delete only if the server's current uid matches. Guards
  // against name reuse across lifecycles (see section #7).
  string uid = 2;
}
```

Rules:
- RPC name **must** begin with `Delete` followed by the singular resource name.
- Response **must** be the deleted resource.
- Request **must** identify the resource with an `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- That field **must** be named after the resource's own snake_case type name (e.g. `actor` for `Actor`, `actor_snapshot_tag` for `ActorSnapshotTag`), not a generic name like `name` or `ref`.
- If the resource does not exist: return `NOT_FOUND`.
- `version` and `uid` preconditions are honored via a `DeleteOptions` field (see section #7). Both are optional; the zero value skips the check.
- Further non-resource "control" fields (e.g. dry-run) belong in `DeleteOptions`, not as loose top-level fields on the request.


TODO: Delete operations are synchronous, but might revisit this and introduce the concept of soft-deletion to 
allow external controllers and upstream systems / automations to react to resource deletion (e.g. via some mechanism like k8s finalizers).

---

## 4. Custom Methods

*Follows [AIP-136](https://google.aip.dev/136).*

Custom methods are for operations that don't map cleanly to CRUD: lifecycle transitions (Suspend, Resume, Pause), long-running actions, or commands with side effects that standard Update semantics would misrepresent.

```proto
rpc SuspendActor(SuspendActorRequest) returns (Actor) {}

message SuspendActorRequest {
  ObjectRef actor = 1;
}
```

Rules:
- RPC name **must** be a verb phrase: `{Verb}{Resource}` (e.g., `SuspendActor`, `ResumeActor`).
- Request message name **must** match the RPC name with a `Request` suffix.
- The request **must** identify the target resource using an `ObjectRef` field (for both atespace-scoped and global-scoped resources).
- Custom methods should return a response message matching the RPC name, with a Response suffix. When operating on a specific resource, a custom method may return the resource itself.

---

## 5. Field Naming

*Follows [AIP-140](https://google.aip.dev/140) and [AIP-149](https://google.aip.dev/149).*

- Field definitions in proto files **must** use `lower_snake_case`.
- Boolean fields **must** omit the `is_` prefix: use `disabled`, not `is_disabled`. Exception: use `is_` when the bare word would be a reserved keyword in common languages.
- Repeated fields **must** use the plural noun form: `containers`, not `container`.
- Non-repeated fields **must** use the singular form: `container`, not `containers`.
- Field names **must** be nouns, not verbs: `worker_selector`, not `select_workers`.
- Use standard abbreviations where well-established: `config`, `spec`, `id`, `info`, `stats`.
- Adjectives come before the noun: `suspended_actors`, not `actors_suspended`.
- Avoid prepositions in field names: `error_reason`, not `reason_for_error`.

### 5.1 Enum naming

*Follows [AIP-126](https://google.aip.dev/126). Diverges by requiring two of AIP-126's recommendations.*

- Enum type names **must** use `PascalCase`, like message names: `ActorState`.
- Enum values **must** use `UPPER_SNAKE_CASE`.
- Package-level enum values **must** be prefixed with the enum name. Some languages (including C++) hoist enum values into the parent namespace, which can cause conflicts between enums in the same proto package. (Values of a *nested* enum **must not** be prefixed.)
- The zero value **must** be `{ENUM_NAME}_UNSPECIFIED` and mean "not set."

```proto
enum ActorState {
  ACTOR_STATE_UNSPECIFIED = 0;
  ACTOR_STATE_RUNNING     = 1;
  ACTOR_STATE_SUSPENDED   = 2;
}
```

**Divergence from AIP-126:** AIP-126 makes the enum-name prefix and the `_UNSPECIFIED` zero value *recommended*; Substrate requires both.

### 5.2 Field presence (`optional`)

Use the `optional` keyword on a scalar field **only** when null and the zero value (`false`, `0`, `""`) are semantically distinct for that field's meaning. Do not use `optional` universally or as a workaround for update semantics — updates replace the whole resource (section #3.4), so an unset field already has an unambiguous meaning.

```proto
// Only if "no priority" is meaningfully different from "priority 0":
optional int32 priority = 5;

// No optional needed — false and unset mean the same thing here:
bool cordoned = 6;
```

Because an update carries the whole resource, the server always knows what the client intends the resource to look like. `optional` is reserved for cases where the resource itself has a three-state semantic (set-to-zero, set-to-nonzero, not-set).

---

## 6. Standard Fields

*Follows [AIP-148](https://google.aip.dev/148) and [AIP-142](https://google.aip.dev/142). Diverges in that a shared message contains all standard fields.*

All resources in Substrate must have a `ResourceMetadata metadata = 1` field to hold common fields.

```proto
message ResourceMetadata {
  // atespace is the namespace the resource belongs to. Empty for global-scoped
  // resources. Caller-specified at creation and immutable thereafter.
  string atespace = 1;

  // name is the resource's name, unique within its atespace (or globally, for
  // global-scoped resources). Caller-specified at creation and immutable thereafter.
  string name = 2;

  // uid is a server-assigned, globally unique identifier for this resource.
  // Immutable throughout the lifecycle of the resource.
  string uid = 3;

  // version is increased on every mutation.
  int64 version = 4;

  // create_time is the time the resource was created.
  google.protobuf.Timestamp create_time = 5;

  // update_time is the time the resource was last updated by a user action.
  google.protobuf.Timestamp update_time = 6;
}
```

### 6.1 `atespace`

- Type: `string`.
- The atespace (namespace) the resource belongs to. Part of the resource's identity (see section #2).
- Caller-specified at creation; immutable thereafter.
- Must be a valid resource name (see section #2.2).
- Must be non-empty for atespace-scoped resources and empty for global-scoped resources.

### 6.2 `name`

- Type: `string`.
- The resource's name — unique within its `atespace` for atespace-scoped resources, or globally for global-scoped resources.
- Caller-specified at creation; immutable thereafter.
- Must be a valid resource name (see section #2.2).
- Together, `(atespace, name)` identify a resource at a point in time. `uid` (see section #6.3) identifies it across time, distinguishing lifecycles that reuse the same `(atespace, name)`.

### 6.3 `uid`

- Type: `string`.
- A server-assigned [UUID4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)).
- Useful for correlation across logs, events, and audit trails where the resource `name` may not be available. Also useful
for controllers that need to do bookkeeping and track state associated with a resource.
- Required on Update requests, where it guards the lifecycle the write is for. See section #7.

### 6.4 `version`

- Type: `int64`.
- Increased on every mutation; the increment amount is not part of the contract. Allows clients to do optimistic locking
on resource updates. Also establishes a total order on "snapshots" of a given resource. See section #7.
- Required on Update requests, where it guards the revision the write is against.

### 6.5 `create_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was created.
- Set once at creation; never updated.

### 6.6 `update_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was last modified by a user action (Create, Update, or a custom mutating method).
- Updated on every mutation. Internal state changes made by the system (e.g., a scheduler assigning a worker) **may** also update this field, but are not required to.

---

## 7. Resource Freshness and Optimistic Concurrency

*Inspired by [AIP-154](https://google.aip.dev/154). Diverges in field name and type.*

When two clients update the same resource concurrently, the second write may silently overwrite the first. Freshness validation lets a client prove it is operating on the state it thinks it is, so the server can reject stale writes.

Substrate uses a field named **`version`** of type `int64` for this. AIP-154 uses an opaque `etag` string; we diverge for a concrete reason: Substrate maintains an in-memory worker cache that guards against applying stale watch events over newer cached state using a numeric `>=` comparison. An opaque string cannot serve this role — the ordering guarantee IS the implementation contract, so the field type should reflect it.

The name `version` is intentional: it increases on every write (like Kubernetes's `resourceVersion`) and is a transparent, comparable integer (like Kubernetes's `generation`). It serves both roles in one field, so neither Kubernetes name fits cleanly.

### 7.1 The `version` field

`version` is a standard output-only field on every resource:

```proto
message ResourceMetadata {

  // ... other fields

  // version is increased on every mutation.
  int64 version = 4;
}
```

- Type: `int64`.
- Output-only: the server sets it. Assigned on creation and strictly increased on every mutation. The magnitude of the increase is **not** part of the contract, and clients must not assume consecutive versions differ by exactly `1`.
- Monotonically increasing. A higher value is always newer.
- Updated on every mutation — both user-visible changes and system-internal ones (e.g. the scheduler binding a worker).

### 7.2 Using `version` and `uid` to guard writes

A client guards a mutation against acting on unexpected state by echoing back the `version` and `uid` it last observed. The server rejects the request if either value no longer matches.

The two guards protect against different things, and neither substitutes for the other:

- **`version`** guards against *concurrent modification*. It changes on every write, so echoing it back detects that someone else wrote in between (the lost-update problem). A `version` guard can legitimately reject an otherwise-valid read-modify-write — that is the point.
- **`uid`** guards against *name reuse across lifecycles*. `(atespace, name)` is unique only at a point in time; `uid` is unique across time. Because `uid` is immutable within a lifecycle, it never causes a spurious rejection within that lifecycle — it only fires when the name has been deleted and recreated as a different resource. This catches the ABA case that `version` alone cannot: `version` restarts at `1` for each new lifecycle, so a stale write's `version` can happen to match the *new* resource.

**Update: both guards are required.** They are specified in the embedded resource's `metadata`:

Because only a prior read supplies the guards, **every update is a read-modify-write**:

```go
// 1. Read.
actor, err := client.GetActor(ctx, &GetActorRequest{
    Actor: &ObjectRef{Atespace: "my-space", Name: "my-actor"},
})

// 2. Modify the message you just read, in place (or clone it). 
// But, do not build a fresh Actor.
actor.WorkerSelector = &Selector{MatchLabels: map[string]string{"tier": "paid"}}

// 3. Write it back. actor.metadata already carries the required fields.
updated, err := client.UpdateActor(ctx, &UpdateActorRequest{
    Actor: actor,
})
```

- `version` and `uid` are control fields managed by the server, not mutable fields. A client echoes them back as guards, it never edits them.
- Both are **required**: an update that leaves `version` (0) or `uid` ("") unset is rejected with `INVALID_ARGUMENT`. There is no blind by-name update. A client that has not read the resource cannot update it; a client that has read it already holds both values.
- The requirement is on Update only. Delete's guards stay optional (below), and Create assigns both fields rather than checking them.

- Modify what you read, do not reconstruct it: step 2 above mutates the message the server returned rather than assembling a new `Actor`. 
   - This matters when the server is newer than the client: a reconstructed message does not carry the fields the client does not recognize, and an update that omits an immutable field is rejected rather than silently rewriting it.
- Do not round-trip a resource through JSON between read and write. `protojson` cannot represent unknown fields. Marshalling silently drops them.    
- Do not retry `ABORTED` updates at the server side. `ABORTED` means someone else wrote something you have not seen. A blind retry reapplies your change on top of a concurrent change nobody reviewed. Surface the conflict instead.

**Delete:** the guards are specified in a `DeleteOptions` message, reused across every `Delete<Type>Request`.:

```proto
message DeleteActorRequest {
  ObjectRef     actor   = 1;
  DeleteOptions options = 2;
}

message DeleteOptions {
  int64  version = 1; // 0  = skip
  string uid     = 2; // "" = skip
}
```

**Server behavior (applies to `version` and `uid` alike):**
- If the client provides a `version` or `uid` that does not match the server's current value: return `ABORTED`.
- On Update, if the client omits either guard: return `INVALID_ARGUMENT`. The request is malformed, so this is decided before the resource is read.
- On Delete, if the client omits a guard: skip that check.

So Update never gives last-writer-wins, while Delete still does by default.

---

## 8. Resource Status: Output-Only Fields

Every resource separates **user-controlled** fields from **server-managed (output-only)** fields at the top level. User-controlled fields are set by the caller at `Create` and/or `Update` time and stay directly on the resource message, alongside `metadata`.
Output-only fields **must** be grouped into a single nested message field named `status`.

```proto
message Actor {
  // Common resource metadata: atespace, name, and other standard fields (see section #6).
  ResourceMetadata metadata = 1;

  // worker_selector is caller-specified: user-controlled, stays top-level.
  Selector worker_selector = 5;

  // Server-managed state. Absent from Create/Update request payloads.
  ActorStatus status = 7;
}

enum ActorState {
  ACTOR_STATE_UNSPECIFIED = 0;
  ACTOR_STATE_RUNNING = 1;
  ACTOR_STATE_SUSPENDED = 2;
  // ...
}

message ActorStatus {
  ActorState state = 1;

  // worker_assignment points at the worker currently hosting this Actor.
  WorkerAssignment worker_assignment = 2;

  // ... other server-managed fields
}
```

Rules:
- Output-only fields **must not** appear as top-level fields on the resource. They **must** be grouped under a single `status` field of type `{Resource}Status`.
- A resource with no output-only fields needs no `status` field.
- Fields inside `status` follow the same naming rules as any other field (section #5).
- `ResourceMetadata` (section #6) is exempt from this split. It mixes caller-specified identity (`atespace`, `name`) with server-managed fields (`uid`, `version`, timestamps).

## 9. Union Types

A **union** is a set of fields of which exactly one is set. Substrate expresses a union
as a group of sibling fields, each tagged `+k8s:unionMember`.

```proto
message Volume {
  // +k8s:required
  // +k8s:format=k8s-short-name
  string name = 1;

  // Exactly one of durable_dir / external_volume_template / image must be set.
  //
  // +k8s:optional
  // +k8s:unionMember
  DurableDirVolumeSource durable_dir = 2;

  // +k8s:optional
  // +k8s:unionMember
  ExternalVolumeTemplate external_volume_template = 3;

  // +k8s:optional
  // +k8s:unionMember
  ImageVolumeSource image = 4;
}
```

Rules:
- Unions **must not** use protobuf's `oneof` keyword. `oneof` does not work with declarative validation.
- Each member **must** be tagged `+k8s:optional` and `+k8s:unionMember`. The generated validation enforces that exactly one member is set.
- A member that is genuinely a primitive **must** use the `optional` keyword, so that presence is explicit on the wire. Protobuf elides zero-valued primitives, so without `optional` the server cannot tell a member set to `0`, `false`, or `""` from an unset one. Use with care, as a primitive type cannot be augmented with more fields.
- A `map` or `repeated` member takes no `optional` keyword and needs none, but it has no presence either: validation treats it as set when it is non-empty. A client cannot select such a member by sending it empty. If "empty" has to be a meaningful choice, wrap the map or list in a message, which does have presence.
- Unions **must not** carry a discriminator field (a `type` or `kind` enum naming which member is set). The member that is set is the discriminator. A separate discriminator is a second source of truth that can contradict the payload, and it forces every client to keep the two in sync for no gain.

---

## 10. Validation

All fields of all APIs must be validated.  We use
[validation-gen](https://github.com/kubernetes/code-generator/tree/master/cmd/validation-gen) to generate validation code for our APIs.  See [the guidelines for validation](api-validation.md) for more information.

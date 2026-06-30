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
- The resource schema must be identical across all standard methods that reference it (i.e., Get, Create, and Update all return the same `Actor` message).

---

## 2. Resource Naming and Identity

**Diverges from [AIP-122](https://google.aip.dev/122).**

AIP-122 identifies resources by a single opaque path string (e.g., `publishers/123/books/les-miserables`). Substrate uses a **two-field identity** instead: an `atespace` (namespace) and a `name`. This is analogous to how Kubernetes identifies objects and avoids the ambiguity of parsing hierarchical path strings.

Resources are either **atespace-scoped** or **global-scoped**. Scope is a fixed property of the resource type, not of individual instances.

### 2.1 Atespace-scoped resources

Atespace-scoped resources belong to an atespace. Their identity is `(atespace, name)`, unique within the resource type. The first two fields **must** be:

```proto
message Actor {
  // The atespace this actor belongs to.
  string atespace = 1;
  // The name of this actor, unique within its atespace.
  string name = 2;

  // ... other fields
}
```

- `atespace` is the Substrate namespace for the resource.
- `name` is the resource name within the atespace. It is not a path; it is a short identifier.

### 2.2 Global-scoped resources

Some resources are global across the entire deployment and do not belong to any atespace. For these, the identity is `name` alone. The first field **must** be:

```proto
message Worker {
  // The name of this worker, globally unique (e.g. the node name).
  string name = 1;

  // ... other fields
}
```

- There is no `atespace` field as the resource is global-scoped.
- `name` must be globally unique within the resource type, across all atespaces.

### 2.3 Character constraints

Both `atespace` and `name` must be valid resource names.
A valid resource name must comply the following rules:

- Lowercase alphanumeric characters and hyphens only.
- Must start with a lowercase alphanumeric character.
- Must end with a lowercase alphanumeric character.
- Maximum 63 characters.
- Regex: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`

Resource names are valid RFC-1123 DNS labels.

### 2.4 `{Resource}Ref` — compound identity type

Every atespace-scoped resource type **must** have a corresponding `{Resource}Ref` message that bundles `atespace` + `name` as a single, typed unit:

```proto
message ActorRef {
  string atespace = 1;
  string name     = 2;
}
```

Use `{Resource}Ref` in places where you need to reference a resource. For example:

**1. Request messages** — to identify which specific resource to act on:

```proto
message GetActorRequest {
  ActorRef actor = 1;
}

message DeleteActorRequest {
  ActorRef actor   = 1;
  int64     version = 2;
}
```

**2. Cross-resource references** — when one resource's fields refer to another atespace-scoped resource:

```proto
message Actor {
  string atespace = 1;
  string name     = 2;

  // The ActorTemplate this actor was derived from.
  ActorTemplateRef actor_template = 3;

  // ... other fields
}
```

The field name is the logical name of the reference (e.g., `actor_template`), not `actor_template_name` or `actor_template_ref`.
Note that this assumes that ActorTemplates are also resources in the substrate gRPC API (not in KRM).

**Global-scoped resources** have no atespace, but still **must** have a corresponding `{Resource}Ref` message — with only a `name` field:

```proto
message WorkerRef {
  string name = 1;
}
```

Use it the same way as atespace-scoped `{Resource}Ref` messages — in request messages and cross-resource references:

```proto
message Actor {
  // The Worker assigned to this actor.
  WorkerRef worker = 10;
}
```

* Do not embed the full resource message as a reference field.
- Do not use a single combined string like `"atespace/name"`. Callers would have to parse it.
- Do not use a plain `string {resource}_name` field for global-scoped references — use `{Resource}Ref` for consistency and type safety.

---

## 3. Standard Methods

The following sections cover each standard method. The primary adaptation from AIP-13x is how resources are identified in requests: two fields (`atespace` + `name`) instead of a single `name` path string.

### 3.1 Get

*Follows [AIP-131](https://google.aip.dev/131).*

```proto
rpc GetActor(GetActorRequest) returns (Actor) {}

message GetActorRequest {
  ActorRef actor = 1;
}
```

Rules:
- RPC name **must** begin with `Get` followed by the singular resource name.
- Request message name **must** match the RPC name with a `Request` suffix.
- Response **must** be the resource itself — not a `GetActorResponse` wrapper.
- Request **must** identify the resource with a single `{Resource}Ref` field (for both atespace-scoped and global-scoped resources).
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
- The `page_size` and `page_token` fields **must be defined** on every List request message (even though the token will be empty on the first call).
- `next_page_token` **must** be present on every List response message. It **must** be empty when there are no further pages.
- The repeated resource field **must** use the plural form of the resource name (e.g., `actors`, not `actor`).
- If a user provides a `page_size` above the maximum, coerce it silently. If a user provides a negative value, return `INVALID_ARGUMENT`.
- Sorting and filtering as specified in AIP-132 are not supported.

### 3.3 Create

*Adapted from [AIP-133](https://google.aip.dev/133).*

```proto
rpc CreateActor(CreateActorRequest) returns (Actor) {}

message CreateActorRequest {
  // The actor to create.
  // actor.atespace and actor.name together specify the resource's identity
  // and must both be set by the caller.
  Actor actor = 1;
}
```

Rules:
- RPC name **must** begin with `Create` followed by the singular resource name.
- Response **must** be the resource itself — not a `CreateActorResponse` wrapper.
- `actor.atespace` and `actor.name` are **required** and caller-specified. The server does not generate them.
- If a resource already exists with the same `(atespace, name)`: return `ALREADY_EXISTS`.

**Divergence from AIP-133:** AIP-133 separates `parent` + `{resource}_id` from the resource body because AIP-122 makes the resource `name` field output-only (constructed by the server from the parent path). In Substrate's model, `atespace` and `name` are directly caller-specified identity fields on the resource, so duplicating them at the top level of the request adds no information and creates ambiguity about which one wins. The embedded resource is the single source of truth for identity on create.

### 3.4 Update

*Follows [AIP-134](https://google.aip.dev/134). Diverges on `update_mask` requirement and `*` support.*

Updates use a **partial-update model** (equivalent to HTTP PATCH). The mask is always required.

```proto
rpc UpdateActor(UpdateActorRequest) returns (Actor) {}

message UpdateActorRequest {
  // The actor to update.
  // The actor's atespace and name fields identify which resource to update.
  Actor actor = 1;

  // The set of fields to update. Required.
  //
  // Field paths are relative to the Actor message (e.g., "worker_selector").
  google.protobuf.FieldMask update_mask = 2;
}
```

Rules:
- RPC name **must** begin with `Update` followed by the singular resource name.
- Response **must** be the resource itself — not an `UpdateActorResponse` wrapper.
- `update_mask` **must** be of type `google.protobuf.FieldMask` and **must** be named `update_mask`.
- `update_mask` is **required**. An absent or empty mask **must** return `INVALID_ARGUMENT`.
- The special value `*` is **not supported**. Clients must enumerate the exact fields to update.
- The resource's `atespace` and `name` identify the resource to update; they are not themselves updatable.
- If the resource does not exist: return `NOT_FOUND`.

**Divergence from AIP-134:** AIP-134 makes `update_mask` optional (omission implies updating all populated fields) and requires support for `*`. Substrate requires an explicit mask.

### 3.5 Delete

*Follows [AIP-135](https://google.aip.dev/135).*

```proto
rpc DeleteActor(DeleteActorRequest) returns (google.protobuf.Empty) {}

message DeleteActorRequest {
  ActorRef actor   = 1;
  int64     version = 2; // optional freshness guard; 0 = skip check
}
```

Rules:
- RPC name **must** begin with `Delete` followed by the singular resource name.
- Response **must** be `google.protobuf.Empty` (no `DeleteActorResponse` wrapper).
- Request **must** identify the resource with a `{Resource}Ref` field (for both atespace-scoped and global-scoped resources).
- If the resource does not exist: return `NOT_FOUND`.

---

## 4. Custom Methods

*Follows [AIP-136](https://google.aip.dev/136).*

Custom methods are for operations that don't map cleanly to CRUD: lifecycle transitions (Suspend, Resume, Pause), long-running actions, or commands with side effects that standard Update semantics would misrepresent.

```proto
rpc SuspendActor(SuspendActorRequest) returns (Actor) {}

message SuspendActorRequest {
  ActorRef actor = 1;
}
```

Rules:
- RPC name **must** be a verb phrase: `{Verb}{Resource}` (e.g., `SuspendActor`, `ResumeActor`).
- Request message name **must** match the RPC name with a `Request` suffix.
- The request **must** identify the target resource using a `{Resource}Ref` field (for both atespace-scoped and global-scoped resources).
- The response **should** return the updated resource when the operation mutates it (e.g., all Actor lifecycle methods return the updated `Actor`).

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
- Do not use `name` for any purpose other than a resource's own name field (see section 2.1). Use specific names: `display_name`, `actor_template_name`, etc.

### 5.1 Field presence (`optional`)

Use the `optional` keyword on a scalar field **only** when null and the zero value (`false`, `0`, `""`) are semantically distinct for that field's meaning. Do not use `optional` universally or as a workaround for update semantics — the required `update_mask` (§3.4) handles that.

```proto
// Only if "unrated" is meaningfully different from "rated 0":
optional int32 priority = 5;

// No optional needed — false and unset mean the same thing here:
bool cordoned = 6;
```

Because `update_mask` is required, the server always knows which fields the client intends to change. `optional` is reserved for cases where the resource itself has a three-state semantic (set-to-zero, set-to-nonzero, not-set).

---

## 6. Standard Fields

*Follows [AIP-148](https://google.aip.dev/148) and [AIP-142](https://google.aip.dev/142).*

Certain fields appear on nearly every resource and must use the exact names and types below. All standard fields are **output-only**: the server sets them and the client must not send them on create or update (the server ignores any value provided).

The recommended field ordering within a resource message is: identity fields (`atespace`, `name`), then `uid`, then timestamps, then `version`, then resource-specific fields.

```proto
message Actor {
  string atespace = 1;
  string name     = 2;

  // uid is a server-assigned, globally unique identifier for this resource.
  // It is stable across renames and updates. Distinct from name.
  string uid = 3;

  // create_time is the time the resource was created.
  google.protobuf.Timestamp create_time = 4;

  // update_time is the time the resource was last updated by a user action.
  google.protobuf.Timestamp update_time = 5;

  // delete_time is set when the resource is soft-deleted.
  // Only present on resources that support soft delete.
  google.protobuf.Timestamp delete_time = 6;

  // version is incremented on every mutation. See §7.
  int64 version = 7;

  // ... resource-specific fields
}
```

### 6.1 `uid`

- Type: `string`.
- A server-assigned [UUID4](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_4_(random)).
- Useful for correlation across logs, events, and audit trails where the resource `name` may not be available. Also useful
for controllers that need to do bookeeping and track state associated with the resource.

### 6.2 `create_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was created.
- Set once at creation; never updated.
- All public resources **must** include `create_time`.

### 6.3 `update_time`

- Type: `google.protobuf.Timestamp`.
- Records when the resource was last modified by a user action (Create, Update, or a custom mutating method).
- Updated on every mutation. Internal state changes made by the system (e.g., a scheduler assigning a worker) **may** also update this field, but are not required to.
- All public resources **must** include `update_time`.

### 6.4 `delete_time`

- Type: `google.protobuf.Timestamp`.
- Only relevant for resources that support **soft delete** (marking as deleted without immediately purging).
- Set when the resource is soft-deleted; absent (zero value) when the resource is live.
- Resources that do not support soft delete **must not** include this field.
- Substrate's current resources use hard delete; add `delete_time` only if soft delete is introduced for a specific resource type.

---

## 7. Resource Freshness and Optimistic Concurrency

*Inspired by [AIP-154](https://google.aip.dev/154). Diverges in field name and type.*

When two clients update the same resource concurrently, the second write may silently overwrite the first. Freshness validation lets a client prove it is operating on the state it thinks it is, so the server can reject stale writes.

Substrate uses a field named **`version`** of type `int64` for this. AIP-154 uses an opaque `etag` string; we diverge for a concrete reason: Substrate maintains an in-memory worker cache that guards against applying stale watch events over newer cached state using a numeric `>=` comparison. An opaque string cannot serve this role — the ordering guarantee IS the implementation contract, so the field type should reflect it.

The name `version` is intentional: it increments on every write (like Kubernetes's `resourceVersion`) and is a transparent, comparable integer (like Kubernetes's `generation`). It serves both roles in one field, so neither Kubernetes name fits cleanly.

### 7.1 The `version` field

`version` is a standard output-only field on every resource:

```proto
message Actor {
  string atespace = 1;
  string name     = 2;
  string uid      = 3;
  google.protobuf.Timestamp create_time = 4;
  google.protobuf.Timestamp update_time = 5;
  int64 version = 6;

  // ... resource-specific fields
}
```

- Type: `int64`.
- Output-only: the server sets it. Starts at `1` on creation, incremented by `1` on every mutation.
- **Monotonically increasing:** safe to compare numerically. A higher value is always newer.
- Updated on every mutation — both user-visible changes and system-internal ones (e.g. the scheduler binding a worker).
- The field **must** be named `version`.
- All public resources **must** include `version`.

### 7.2 Using `version` to guard writes

A client that wants to guard against concurrent modification echoes back the `version` it last observed. The server rejects the request if the value no longer matches.

**Update:** `version` rides in the embedded resource body:

```proto
// Client read actor at version 5, now updating:
UpdateActorRequest {
  actor: Actor {
    atespace: "my-space"
    name:     "my-actor"
    version:  5            // guard: fail if server is not at 5
    worker_selector: ...
  }
  update_mask: "worker_selector"
}
```

**Delete and custom methods:** add an optional `int64 version` field to the request:

```proto
message DeleteActorRequest {
  ActorRef actor  = 1;
  // Optional. If non-zero, the deletion is rejected with ABORTED if the
  // server's current version does not match.
  int64 version   = 2;
}
```

**Server behavior:**
- If the client provides a non-zero `version` that does not match the server's current value: return `ABORTED`.
- If the client omits `version` (zero value, the proto3 default): skip the freshness check and proceed.
- The freshness check is always optional from the client's perspective.

# Composing per-container limits with actor-level sandbox sizing

Date: 2026-08-15
Status: approved, not yet implemented
Affects: PR #859 (per-container limits), builds on #679 (actor sandbox right-sizing, merged 2026-08-14)

## Context

`ActorTemplate` now has two resource knobs that both write the same OCI field.

`spec.resources.limits` arrived with #679. It sizes the sandbox itself: for a
micro-VM the guest RAM and vCPU count, for gVisor the sentry. Guest RAM is the
declared limit minus a VMM reserve (`--vmm-mem-reserve-mib`, default 256Mi) held
back for cloud-hypervisor and virtiofsd, which share the worker pod's cgroup with
the guest's memfd-backed RAM.

`spec.containers[].resources.limits` is PR #859. It caps an individual container
inside the sandbox so it cannot starve or OOM its siblings. It is micro-VM only:
gVisor runs one sentry for every container in the actor, so a per-container cgroup
there is created and stays empty (google/gvisor#190).

The two collide. `sizing.ApplyToOCISpec` writes `linux.resources` unconditionally:

```go
spec.Linux.Resources.Memory.Limit = &limit
spec.Linux.Resources.CPU.Quota    = &quota
```

and `ensureKataCompatibleSpec` calls it once per container with the actor-level
size. atelet has already written each container's own declared limits into that
same field, so the actor-level value overwrites them. Merged naively, #859
compiles, passes its tests, and silently does nothing: every container ends up
with the actor's size.

## Decisions

**Composition model.** Per-container limits subdivide a declared actor envelope.
An actor declares its total size; containers may carve it up.

**Precedence.** A container gets a cgroup limit if and only if it declares one.
There is no inheritance: a container that declares nothing is bounded by the guest
as a whole, not by a stamped copy of the actor total. On micro-VM a cap equal to
the whole guest could never bind anyway, so stamping it is noise.

**Exception: the gVisor sandbox root.** On gVisor the `pause` container keeps the
actor-level size. That stamp is the entire enforcement mechanism there: every
sandbox process lives in the pause leaf while the per-container leaves stay empty,
so it is the only cgroup that binds. atelet deliberately passes no limits for
pause, so a literal "only stamp what was declared" rule would remove all resource
enforcement from gVisor actors. pause is infrastructure, not a user container, so
this does not violate the no-inheritance rule.

The exception is gVisor-only. On micro-VM no container needs the stamp, including
pause: guest RAM is the hard bound and is sized from the actor-level limit when the
VM is created.

**Validation stays at runtime.** Over-subscription is rejected by
`checkResourceEnvelope` when the actor starts, not at `kubectl apply`. See
"Rejected alternatives".

## Semantics

| actor `spec.resources` | container declares | micro-VM container cgroup | micro-VM guest size |
| :--- | :--- | :--- | :--- |
| set | yes | the container's own limit | declared − reserve |
| set | no | unset (bounded by guest RAM) | declared − reserve |
| unset | yes | the container's own limit | SandboxConfig `default_memory` |
| unset | no | unset | SandboxConfig `default_memory` |

gVisor is unchanged in every row: per-container limits are rejected at admission,
and the actor-level limit binds at the sandbox via pause.

## Implementation

Approach: stop stamping user containers on the micro-VM path.

Remove the `size.ApplyToOCISpec(&spec)` call from `ensureKataCompatibleSpec`
(`cmd/ateom-microvm/spec.go`). Declared per-container limits are already in
`config.json` from atelet; undeclared containers get nothing. Nothing else in the
micro-VM sizing path changes: `resolveGuestMemMiB` and `buildVMConfig` still size
the VM from the actor-level limit.

This is a deletion. `internal/sizing` is untouched, so gVisor keeps its stamp
(`cmd/ateom-gvisor/runsc.go`) and chw120's just-merged shared code keeps its
current semantics for its only remaining consumer.

Ownership becomes clean: atelet decides per-container limits, ateom decides the VM
envelope.

The micro-VM carrier is unaffected — `stripWorkloadLimits` already removes workload
limits from it, since the carrier is created and never started.

## Validation and error messages

`checkResourceEnvelope` already runs after `buildActorContainers`, by which point
`memMiB` has been through `resolveGuestMemMiB`. It therefore enforces
`sum(container limits) <= declared − reserve`, which is stricter and more accurate
than any admission-time approximation could be, since CEL cannot know the reserve.

Two changes:

- Extend the error to name the declared actor limit and the reserve alongside the
  guest MiB, so the message explains why the ceiling is what it is.
- Add a test asserting the envelope is measured against the post-reserve guest,
  not the SandboxConfig default. This composition is currently implicit in the
  ordering of `run.go` and nothing pins it.

## Documentation

`docs/api-guide.md` currently states that a micro-VM actor's guest size "comes from
the pool's `SandboxConfig`, not from the actor". After #679 that is backwards:
guest RAM is the declared actor limit minus the reserve, with `SandboxConfig` as
the fallback when no limit is declared. The existing error text advising users to
"use a `SandboxConfig` with a larger guest" is likewise incomplete.

Replace the two separate sections with one that presents both layers: actor-level
sizes the sandbox, per-container subdivides it, and per-container is micro-VM only.
State explicitly that over-subscription is caught at actor start rather than at
apply time, so nobody assumes admission covers it.

## Testing

- Unit: a container's declared limit survives into `config.json` unmodified when
  the actor also declares actor-level limits.
- Unit: a container that declares nothing has no memory or CPU limit in its spec.
- Unit: `checkResourceEnvelope` measures against the post-reserve guest size.
- Existing: the micro-VM envelope and `stripWorkloadLimits` tests carry over.
- e2e: `internal/e2e/suites/sizing` is unaffected. It asserts on `num_cpu`
  (`runtime.NumCPU()`) and `mem_total_bytes` (`/proc/meminfo`), both of which come
  from VM sizing. It only logs `memory_max` and `cpu_max`, which the probe reports
  best-effort. Verified against the merged test before choosing this approach.

## Rejected alternatives

**Admission-time sum check (CEL).** Rejected on evidence, not judgement. A rule
summing container memory limits and comparing to the actor limit does compile —
CEL has `sum()`, and `quantity(...).asInteger()` chains — but the API server
rejects it:

```
x-kubernetes-validations[5].rule: Forbidden: estimated rule cost exceeds budget
by factor of more than 100x
```

Worse, it exceeds the schema-wide budget and takes the existing per-container
rules down with it, making the CRD uninstallable:

```
properties[containers].items.properties[resources].x-kubernetes-validations[1..3]:
  Forbidden: contributed to estimated rule cost total exceeding cost limit
```

The cost estimator assumes worst case for unbounded strings. `resource.Quantity`
serializes as a string with no `MaxLength`, and bounding it would mean replacing
`Quantity` with a custom type, changing the serialized API and losing its parsing.
Everything we own is already bounded (`MaxItems=10` on containers,
`MaxProperties=2` on the limits map), which is why the current rules fit.

Recorded for future work: this CRD is already near the schema-wide CEL cost
ceiling. One additional list-iterating `quantity()` rule tips it over. A cheaper
per-container variant using `.all()` was not attempted, as it iterates the same
containers with the same unbounded-string calls.

**Make `ApplyToOCISpec` preserve pre-set values.** Fixes the clobber for declared
containers but still stamps undeclared ones, which is the behaviour we are removing.
Also changes shared semantics for gVisor.

**Resolve a `SandboxSize` per container in `buildActorContainers`.** Same end state
as the chosen approach, but adds code to re-derive per-container values that atelet
has already encoded in `config.json`, duplicating the source of truth.

## Coordination

#679 merged on 2026-08-14; PR #859 is currently `CONFLICTING` and 47 commits behind
main. Implementation starts with a rebase onto main, which will surface conflicts in
the nine files the two changes share.

Removing the micro-VM `ApplyToOCISpec` call narrows the behaviour of a feature that
landed the previous day. It should be raised with @chw120 on #859 rather than
carried silently, framed as: the actor-level limit sizes the box, per-container
subdivides it, and stamping the box size onto every container was a no-op on
micro-VM that also destroyed the per-container values.

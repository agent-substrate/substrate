# Rethinking `SnapshotConfig.minimum_fidelity`

## Summary

- `minimum_fidelity` on the ActorTemplate promises "a Resume never restores below this level".
- Substrate cannot keep that promise in most of the cases where memory is lost, so the promise turns into "the actor is stuck forever".
- Proposal: drop the template field, keep reporting the actual fidelity on `ResumeActorResponse`, and move any "wait rather than downgrade" choice to the request, where the caller can always lift it.

## Problems with the current design

### Actors cannot move to a template with `minimum_fidelity > VOLUMES`

- `durableRestoreFidelity` always restores at `VOLUMES` when `TemplateReplaced` is true.
- The check compares that against the new template's `minimum_fidelity`.
- So a template with `minimum_fidelity = ROOTFS` or `MEMORY` only works for actors created on it. Any existing actor switched to it fails every Resume with `FAILED_PRECONDITION`, and retrying never helps.

### A bad snapshot has no recovery path

- `preferred_fidelity` is best effort at capture time. If the memory layer fails to upload, the snapshot is stored at a lower fidelity.
- Nothing checks `minimum_fidelity` at capture time, so a snapshot below the floor can be written.
- Every later Resume of that actor fails.
- Templates are immutable, so the only way out is to create a new template with `minimum_fidelity = VOLUMES` and switch the actor to it. That accepts the downgrade anyway, just indirectly.

### The guarantee is inconsistent

- An actor with no snapshot cold boots, and the floor is not checked.
- An actor with a volumes-only snapshot is refused, even though it carries more state than a cold boot.
- An app that truly cannot survive losing memory also cannot survive a crash and restart, and crashes happen regardless of this setting.

## Root cause: one field covering different situations

Memory is lost for several reasons. Each has a different decision maker, and waiting helps in only one of them.

| Cause | Triggered by | Does waiting help? | Where the decision belongs |
|---|---|---|---|
| No runtime-compatible worker has capacity | Scheduling, transient | Yes | The Resume caller: wait, or downgrade now |
| Template replaced | User updating the actor's template | Never | The template update itself is the consent |
| gVisor or hardware upgrade | Platform operator | Never | The operator's drain or maintenance flow |
| Memory layer not captured | System, best effort | Never | Suspend time, while the actor is still running |

- Only the first row is a real choice: latency against fidelity.
- That choice belongs to the caller of a specific request, not to the workload definition.
- For the other three rows, a floor can only refuse, and the refusal never clears.

## Proposal

- Remove `SnapshotConfig.minimum_fidelity` from the ActorTemplate, together with its validation, default, and the Resume checks.
- Keep `ResumeActorResponse.fidelity`.
  - Substrate reports what level the actor was restored at.
  - The client decides how to react. For example, an agent that gets `VOLUMES` reloads its context from its own persisted state.
- If callers need "wait rather than downgrade", add it to `ResumeActorRequest`, for example a per-request `minimum_fidelity` or an `allow_fidelity_downgrade` flag.
  - The caller that is refused can always unblock itself by lowering the floor and retrying. No actor gets stuck.
  - When waiting cannot help (template replaced, runtime upgraded, snapshot captured low), return an error that says so, rather than one that looks retryable.
- If a workload needs memory to be captured, enforce it at Suspend.
  - If the snapshot cannot reach the required level, fail the Suspend and leave the actor running.
  - This is the last point where the state can still be saved.

## How to explain it to users

- Fidelity is best effort from end to end.
- Substrate tells you what you got.
- If you would rather wait than get less, say so on the request.

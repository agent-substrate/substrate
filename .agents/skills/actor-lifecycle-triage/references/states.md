# Actor states

Each table starts from what the user sees. Run the Confirm check before you
name the root cause. Ask the user to confirm before running any command that
changes actor state.

In the commands, `<worker-namespace>`, `<worker-pod>`, and `<node>` come from
the actor's `status.workerAssignment` and `status.assignedNode`. A crash clears
`status.workerAssignment` while keeping `status.assignedNode`: for a CRASHED
actor, get `<worker>` (the Worker name and pod UID) from `ate-api-server` logs
near `status.crash.crashTime` (`Releasing actor from a worker whose pod is
gone`, `Releasing actor from a restarted worker`, or `Worker reported an actor
crash`), then resolve the pod with `kubectl ate get worker <worker> -o yaml`
when the Worker still exists. The atelet pod is the one from `kubectl -n
ate-system get pods -l app=atelet -o wide --field-selector
spec.nodeName=<node>`.

## Stuck in a transition

Lifecycle calls move an actor into RESUMING, SUSPENDING, REVERTING, or DELETING
while in progress. If a step fails with a retriable error, the actor stays in
that state and retrying the same call resumes from where it stopped.

| Symptom | Root cause | Confirm | Fix |
|---|---|---|---|
| RESUMING or SUSPENDING, and the call returned `Unavailable` or `Internal` | A call to atelet failed with a retriable error, for example while atelet or the worker restarted | The `Handle RPC` line for the call in ate-api-server's log has `while calling atelet` or `no atelet pod found on node` in its error; the atelet pod on the node isn't Ready, or is new | Once atelet is Ready, retry the call |
| RESUMING, SUSPENDING or REVERTING, and the worker pod is gone | The worker pod was deleted. ate-controller then deletes its Worker, which moves the actor to CRASHED | `kubectl -n <worker-namespace> get pod <worker-pod>` returns NotFound | Wait for CRASHED with `worker pod went away while hosting the actor`, then follow that row |
| DELETING for minutes | A delete step failed, such as stopping the sandbox on atelet or detaching volumes | ate-api-server log for the actor; the delete call returned an error | Retry delete |

## CRASHED

`status.crash.message` gives the reason. When a crash happens during a
lifecycle operation, `ate-api-server` prefixes the message with `<operation>
failed: ` (for example `resume failed: assigned worker is draining` or `suspend
failed: atelet Checkpoint: ...`). Crashes while RUNNING have no prefix. Match
the table below on the text after any `<operation> failed: ` prefix.

Mitigation is `kubectl ate revert actor <actor> -a <atespace>` followed by
resume. Revert resets the actor to SUSPENDED at its last uploaded snapshot,
discarding any work after that snapshot, and can disrupt the actor's expected
behavior. Always confirm with the user instead of running `revert` or `resume`
automatically.

| Symptom (`status.crash.message`, after any `<op> failed: ` prefix) | Root cause | Confirm | Mitigation (confirm first) |
|---|---|---|---|
| `worker pod went away while hosting the actor` | The worker pod was deleted or evicted while it hosted the actor: a WorkerPool rollout, node upgrade, scale-down or Spot preemption | `ate-api-server` log `Releasing actor from a worker whose pod is gone` near `crashTime` for `<worker>`; `kubectl get events -A --field-selector involvedObject.uid=<worker>` | Revert, then resume. To keep state through an eviction, the actor needs a SIGTERM handler that gets it suspended within 30 minutes; see [Eviction](../../../../docs/reference/api-guide.md#eviction) |
| `worker restarted while hosting the actor` | The worker process restarted, so its sandboxes are gone | `ate-api-server` log `Releasing actor from a restarted worker` for `<worker>`, `kubectl ate get worker <worker> -o yaml` for its pod, then RESTARTS and `kubectl -n <worker-namespace> logs <worker-pod> --previous` | Revert, then resume. Use the previous log to find why the worker exited |
| `killed for exceeding its memory limit` | The actor's gVisor sandbox or microVM exceeded its memory limit and the worker killed it | `ate-api-server` log `Worker reported an actor crash` near `crashTime` | Raise the actor's memory limit or investigate memory growth, then revert and resume |
| `gVisor sandbox exited`, or starts with `cloud-hypervisor exited:` or `virtiofsd exited:` | The sandbox or microVM process exited while the actor was RUNNING | `ate-api-server` log `Worker reported an actor crash` for `<worker>`; worker pod logs (`Sandbox died` on gVisor, `Micro-VM died` on microVM) | Inspect the worker logs for why the sandbox exited, then revert and resume |
| `assigned worker is draining` | A resume was retried after its worker started draining | The worker from `ate-api-server` logs near `crashTime` is DRAINING in `kubectl ate get workers` | Revert, then resume. It will be assigned to another worker |
| `assigned worker no longer satisfies the actor's placement constraints` | The actor's worker selector changed between a failed resume and its retry | Compare the actor's worker selector with the eligible WorkerPools | Revert, then resume |
| Starts with `node holding the actor's latest snapshot no longer exists` | The last suspend was async, and the node holding its snapshot went away before the upload finished | `kubectl get node <node>` returns NotFound | Revert, then resume. The async snapshot is lost. Use `kubectl ate suspend actor --mode=sync` when a snapshot has to survive its node |
| `node holding the actor's local snapshot is unknown`, `actor has no local snapshot`, `actor has no durable snapshot`, or `atelet's snapshot state conflicts with the actor's record` | The actor's snapshot record is in an inconsistent state that retries cannot fix | ate-api-server log for the actor near `crashTime` | Revert, then resume. File an issue with the log, as this should not happen in normal operation |
| Starts with `atelet <RPC>:`, such as `atelet Restore:` | atelet or the worker returned a non-retriable error for that call | ate-api-server log `Setting Actor to crashed due to Atelet error`; `kubectl -n ate-system logs <atelet-pod> -c atelet --since=1h \| grep '<actor>'` | Check the atelet error, then revert and resume. If it crashes the same way again, file an issue with the atelet log |

## Call refused

`ate-api-server` checks the current state before running a call. Resuming a
RUNNING actor or suspending a SUSPENDED one is a no-op.

| Symptom | Root cause | Confirm | Fix |
|---|---|---|---|
| Resume fails with `AssignWorker prerequisite not met ... want ACTOR_STATE_SUSPENDED` | Actor must be SUSPENDED to resume (it is CRASHED or in a transition) | `status.state` | CRASHED: confirm with the user, then revert first. In a transition: wait, or see [Stuck in a transition](#stuck-in-a-transition) |
| Resume fails with `the actor is larger than every eligible worker` | The actor's resource limits exceed the capacity of every worker matching its selector | Compare the actor's resource limits with eligible WorkerPools | Lower the actor's resource limits, or add a WorkerPool with larger workers |
| Suspend fails with `MarkSuspending prerequisite not met ... want ACTOR_STATE_RUNNING` | Actor must be RUNNING to suspend | `status.state` | RESUMING: wait for RUNNING. CRASHED: confirm with the user, then revert (which leaves it SUSPENDED) |
| Revert fails with `is not in a revertable state` | Revert accepts RUNNING, CRASHED, or SUSPENDED with a snapshot that is still only on the node | `status.state` | If the actor is SUSPENDED and its snapshot is already uploaded, there is nothing to revert |
| Tag fails with `must be ACTOR_STATE_SUSPENDED to be tagged` | Actor must be SUSPENDED to tag | `status.state` | Suspend the actor first |

## RUNNING but not working

| Symptom | Root cause | Confirm | Fix |
|---|---|---|---|
| Requests fail or hang, and the actor's processes are gone | An app process exited while the sandbox stayed up (#2972), or a process inside the sandbox or guest was OOM-killed (#3615); on gVisor, an unreported dead sandbox logs a warning (#2211) | `kubectl ate logs actor <actor> -a <atespace>` for app exit or errors; `kubectl -n <worker-namespace> logs <worker-pod>` for `Sandbox has no processes left while the actor is hosted` | Share the logs with the actor's owner; confirm before running revert and resume |
| Requests reach the actor, and it returns errors | Error in the actor's application | `kubectl ate logs actor <actor> -a <atespace>` shows the errors | Give the logs to the actor's owner |
| Requests fail before they reach the actor | Networking | The router's response or the client error | Go back to [troubleshoot](../../troubleshoot/SKILL.md) step 2 |

# Actor states

Each table starts from what the user sees. Run the Confirm check before you
name the root cause.

In the commands, `<worker-namespace>`, `<worker-pod>` and `<node>` come from
the actor's `status.workerAssignment` and `status.assignedNode`. The atelet pod
is the one from `kubectl -n ate-system get pods -l app=atelet -o wide
--field-selector spec.nodeName=<node>`.

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

`status.crash.message` gives the reason. To recover, run
`kubectl ate revert actor <actor> -a <atespace>` and then resume it. Revert
resets the actor to SUSPENDED at its last uploaded snapshot, discarding any
work after that snapshot.

| Symptom (`status.crash.message`) | Root cause | Confirm | Fix |
|---|---|---|---|
| `worker pod went away while hosting the actor` | The worker pod was deleted or evicted while it hosted the actor: a WorkerPool rollout, node upgrade, scale-down or Spot preemption | `kubectl get events -n <worker-namespace>` for the pod, near `crashTime` | Revert, then resume. To keep state through an eviction, the actor needs a SIGTERM handler that gets it suspended within 30 minutes; see [Eviction](../../../../docs/reference/api-guide.md#eviction) |
| `worker restarted while hosting the actor` | The worker process restarted, so its sandboxes are gone | RESTARTS on the worker pod; `kubectl -n <worker-namespace> logs <worker-pod> --previous` | Revert, then resume. Use the previous log to find why the worker exited |
| `assigned worker is draining` | A resume was retried after its worker started draining | The worker is DRAINING in `kubectl ate get workers` | Revert, then resume. It will be assigned to another worker |
| `assigned worker no longer satisfies the actor's placement constraints` | The actor's worker selector changed between a failed resume and its retry | Compare the actor's worker selector with the worker's pool | Revert, then resume |
| Starts with `node holding the actor's latest snapshot no longer exists` | The last suspend was async, and the node holding its snapshot went away before the upload finished | `kubectl get node <node>` returns NotFound | Revert, then resume. The async snapshot is lost. Use `kubectl ate suspend actor --mode=sync` when a snapshot has to survive its node |
| `node holding the actor's local snapshot is unknown`, `actor has no local snapshot`, `actor has no durable snapshot`, or `atelet's snapshot state conflicts with the actor's record` | The actor's snapshot record is in an inconsistent state that retries cannot fix | ate-api-server log for the actor near `crashTime` | Revert, then resume. File an issue with the log, as this should not happen in normal operation |
| Starts with `atelet <RPC>:`, such as `atelet Restore:` | atelet or the worker returned a non-retriable error for that call | ate-api-server log `Setting Actor to crashed due to Atelet error`; `kubectl -n ate-system logs <atelet-pod> -c atelet --since=1h \| grep '<actor>'` | Check the atelet error, then revert and resume. If it crashes the same way again, file an issue with the atelet log |

## Call refused

`ate-api-server` checks the current state before running a call. Resuming a
RUNNING actor or suspending a SUSPENDED one is a no-op.

| Symptom | Root cause | Confirm | Fix |
|---|---|---|---|
| Resume fails with `AssignWorker prerequisite not met ... want ACTOR_STATE_SUSPENDED` | Actor must be SUSPENDED to resume (it is CRASHED or in a transition) | `status.state` | CRASHED: revert first. In a transition: wait, or see [Stuck in a transition](#stuck-in-a-transition) |
| Suspend fails with `MarkSuspending prerequisite not met ... want ACTOR_STATE_RUNNING` | Actor must be RUNNING to suspend | `status.state` | RESUMING: wait for RUNNING. CRASHED: revert, which already leaves it SUSPENDED |
| Revert fails with `is not in a revertable state` | Revert accepts RUNNING, CRASHED, or SUSPENDED with a snapshot that is still only on the node | `status.state` | If the actor is SUSPENDED and its snapshot is already uploaded, there is nothing to revert |
| Tag fails with `must be ACTOR_STATE_SUSPENDED to be tagged` | Actor must be SUSPENDED to tag | `status.state` | Suspend the actor first |

## RUNNING but not working

| Symptom | Root cause | Confirm | Fix |
|---|---|---|---|
| Requests fail or hang, and the actor's processes are gone | On gVisor, the sandbox died (for example, from OOM) while the actor stays RUNNING (#2211) | `kubectl -n <worker-namespace> logs <worker-pod>` shows `Sandbox has no processes left while the actor is hosted` | Revert, then resume |
| Requests reach the actor, and it returns errors | Error in the actor's application | `kubectl ate logs actor <actor> -a <atespace>` shows the errors | Give the logs to the actor's owner |
| Requests fail before they reach the actor | Networking | The router's response or the client error | Go back to [troubleshoot](../../troubleshoot/SKILL.md) step 2 |

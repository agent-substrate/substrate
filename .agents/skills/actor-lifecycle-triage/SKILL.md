---
name: actor-lifecycle-triage
description: Finds why an Agent Substrate actor is stuck in a state, CRASHED, refuses a lifecycle call, or is RUNNING but not answering. Use after the troubleshoot skill picks the actor lifecycle area, or directly when one actor's state looks wrong. Reads the actor, its worker and atelet, matches the symptom against the reference tables, and reports the root cause with evidence. Read-only.
---

# Actor lifecycle triage

Follow the rules, fact collection, and report format in the
[troubleshoot](../troubleshoot/SKILL.md) skill. This skill covers one actor.

Actor states are RESUMING, RUNNING, SUSPENDING, SUSPENDED, CRASHED, DELETING,
and REVERTING. New actors start SUSPENDED. A resume call (or an incoming
request through `atenet-router`) moves an actor to RUNNING, and it stays
RUNNING until explicitly suspended. The only way out of CRASHED is
`kubectl ate revert actor`, which resets the actor to SUSPENDED at its last
uploaded snapshot.

## 1. Read the actor

```bash
kubectl ate get actor <actor> -a <atespace> -o yaml
```

Note:

- `status.state`, and how long it has stayed there.
- `status.crash.message` and `status.crash.crashTime`, if CRASHED.
- `status.workerAssignment`: the worker pod, its namespace and pool.
- `status.assignedNode`.

## 2. Check the worker and atelet

A resume, suspend, or revert calls `atelet` on the assigned node, which calls
the worker. Check both:

```bash
kubectl -n <worker-namespace> get pod <worker-pod> -o wide
kubectl -n ate-system get pods -l app=atelet -o wide --field-selector spec.nodeName=<node>
kubectl ate get workers
```

A missing worker pod or an unready `atelet` on the node causes most stuck and
crashed actors. Note what you find for step 3.

## 3. Match the symptom

Pick the section of [references/states.md](references/states.md) that matches
the actor:

| Symptom | Section |
|---|---|
| Stuck in RESUMING, SUSPENDING, REVERTING, or DELETING | [Stuck in a transition](references/states.md#stuck-in-a-transition) |
| CRASHED | [CRASHED](references/states.md#crashed), by `status.crash.message` |
| Call refused with `FailedPrecondition` | [Call refused](references/states.md#call-refused) |
| RUNNING, but requests or logs show it is not working | [RUNNING but not working](references/states.md#running-but-not-working) |

Run the Confirm check before calling a root cause. These logs are useful across
all cases:

```bash
# ate-api-server runs every lifecycle call and logs each one as `Handle RPC`,
# with the request and the error. It runs more than one replica.
kubectl -n ate-system logs -l app=ate-api-server -c ate-api-server \
  --since=1h --tail=-1 --prefix | grep '<actor>'
# The actor's lifecycle events on its current worker. Works only while RUNNING.
kubectl ate logs actor <actor> -a <atespace> --source=lifecycle
```

## 4. Known gaps

- On gVisor, an actor stays RUNNING even after its sandbox dies (such as from
  OOM, see #2211). Nothing marks it CRASHED.
- `ate-api-server` does not retry failed lifecycle calls. After a retriable
  error, the actor stays in its transition state until the caller retries.
- During an `atelet` rollout, a resume or suspend hitting that node fails with
  `Unavailable` (or `Internal` with `no atelet pod found on node` while no pod
  exists). Running actors are not affected.
- `kubectl ate logs actor` streams only while the actor is RUNNING. For
  earlier logs, use the cluster's log backend.

## 5. Report

Use the report format in [troubleshoot](../troubleshoot/SKILL.md#4-report),
with Area: actor lifecycle.

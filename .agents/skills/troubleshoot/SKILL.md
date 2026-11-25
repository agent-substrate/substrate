---
name: troubleshoot
description: Starting point for troubleshooting an Agent Substrate installation. Use when an operator or support engineer reports a problem with Substrate, such as an actor that is stuck or CRASHED, failing requests, or a failed install or upgrade. Collects the facts every area needs, picks the area skill to follow, checks the repo's issues for a known match, and writes the report. Read-only.
---

# Troubleshoot

Find the area the problem is in, follow that area's skill, and report the root
cause with evidence.

Rules:

1. **Read-only.** For cluster and cloud commands, run only read-only queries
   (`kubectl` `get`/`describe`/`logs`/`top`, `kubectl ate` `get`/`logs`, and
   `gcloud` `describe`/`list`). Ask before anything that changes cluster or
   actor state, including restarts and `kubectl ate` lifecycle commands
   (`resume`, `suspend`, `revert`, `delete`).
2. **No `kubectl ate` access.** `ate-api-server` enforces the installation's
   access policy on every call. If `kubectl ate` fails with `permission denied`,
   give the user the commands to hand to someone with access and keep going
   with what `kubectl` can read.
3. **Cite evidence.** Back each conclusion with the user's error, a log line,
   or an object field. Say when you are guessing.
4. **Protect customer data.** The upstream repo is public. Replace atespace and
   actor names, IPs, and hostnames before drafting a GitHub issue.

## 1. Collect facts

Ask the user for what's missing:

- The exact error text, or the state that looks wrong.
- Atespace and actor (if one is involved) and the time window.
- Scope: one actor, one WorkerPool, or every actor.
- What changed right before it started (install, upgrade, node pool change, or
  new policy).

Then check the cluster:

```bash
# Substrate version, from the image tags. A from-source install has
# digest-only images and no tag.
kubectl -n ate-system get deploy,ds \
  -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[*].image'
kubectl version
kubectl get nodes -o wide
kubectl -n ate-system get pods -o wide
kubectl -n podcertificate-controller-system get pods
kubectl get workerpool -A
kubectl ate get workers
```

## 2. Pick the area

| What the user sees | Skill |
|---|---|
| An actor is stuck in a state, CRASHED, refuses a lifecycle call, or is RUNNING but not answering | [actor-lifecycle-triage](../actor-lifecycle-triage/SKILL.md) |

There is no dedicated skill yet for other areas (install, upgrade, networking,
auth, storage, runtimes, or observability). For those, check that the pods in
`ate-system` are Running and Ready, read their logs for the time window, and
continue to step 3.

## 3. Check known issues

Search the repo's issues and merged PRs with the exact error text or log line,
then with the symptom in plain words:

```bash
gh issue list --repo agent-substrate/substrate --state all --search '"<error text>"'
gh pr list --repo agent-substrate/substrate --state merged --search '"<error text>"' \
  --json number,title,mergeCommit
gh issue view <number> --repo agent-substrate/substrate --comments
```

Classify what you find:

- **Match:** same symptom and root cause. If a merged PR fixes it, fetch
  upstream tags, run
  `git tag --list 'v*' --contains <merge-sha> --sort=v:refname | head -1`
  to find the first release containing the fix, and compare it to the installed
  version.
- **Similar:** same symptom, but a different trigger, version, or root cause.
  Note what differs.
- **None:** if nothing matches, list the searches you ran.

Don't comment on or open issues without approval.

## 4. Report

```
Area:        <the area skill you followed, or none>
Root cause:  <one sentence>
User saw:    <error text, or the state that looked wrong>
Cluster:     <platform and Kubernetes version, nodes, WorkerPools>, Substrate <tag>
Evidence:    log lines and object fields, with their source
Ruled out:   what you checked and cleared
Known issue: #<number> match | similar | none found; fixed in <tag>, installed <tag>
Confidence:  high | medium | low
Next steps:  commands or a workaround, and who has to run them
```

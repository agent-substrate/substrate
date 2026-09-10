---
name: triage-issues
description: Triages open GitHub issues by applying the correct labels. Use when asked to triage issues, label issues, or organize the issue backlog. Processes unlabeled issues first, then reviews labeled issues for correctness. Proposes all changes for approval before applying anything, and writes an undo script for each run.
---

# Triage Issues

Triage means: read each issue, decide the right labels from the taxonomy below, and
apply them with `gh issue edit`. Do not remove labels a human has already added unless
they are clearly wrong. Do not add priority labels unless the issue body or discussion
makes the priority obvious.

Two hard rules govern every run:

1. **Propose before applying.** Classify everything first and present the full set of
   proposed changes as a summary for the user to validate. Apply nothing — no labels,
   no label creation — until the user approves.
2. **Record an undo.** While applying approved changes, build an undo script that
   reverses exactly what this run did, and hand the user the command to run it.

## Label taxonomy

Every issue should have at least one `kind/` label and at least one `area/` label.
Priority and status labels are optional.

### `kind/` — what the issue is

| Label | When to apply |
|---|---|
| `kind/bug` | Something is broken or behaves incorrectly. The current behavior is wrong. |
| `kind/feature` | A new capability that does not exist today. Includes enhancements to existing features. |
| `kind/cleanup` | Code quality improvements that do not change behavior: refactors, naming, dead code removal, small inconsistencies. |
| `kind/docs` | Documentation only — missing, outdated, or incorrect docs. |
| `kind/design` | Design discussions, investigations, research, and open questions that must be resolved before implementation. Includes "Design discussion:", "Identify what X should be", "Measure/benchmark X", "Explore options". |

If any label in this taxonomy does not exist on the repo yet, include its creation in
the proposal and create it during the apply step with:
```bash
gh label create "<label>" \
  --repo agent-substrate/substrate \
  --description "<short description matching the taxonomy table>" \
  --color "c5def5"
```

### `area/` — which part of the codebase

Apply all areas that are touched. Most issues need one or two; a few need more.

| Label | Scope |
|---|---|
| `area/api` | User-facing Substrate API (`pkg/proto/ateapipb/`, proto definitions, API semantics, versioning) |
| `area/api-machinery` | API server internals: RPC handlers, storage layer, syncer, controllers (`cmd/ateapi/`, `cmd/atecontroller/`) |
| `area/network` | Networking: atenet-router, Envoy, ext_proc, DNS, xDS, ingress (`cmd/atenet/`) |
| `area/node` | Node agent and worker lifecycle: atelet, sandbox launch, OCI, cgroups (`cmd/atelet/`) |
| `area/gvisor` | gVisor sandbox specifics: runsc integration, GPU in gVisor, gVisor OCI (`cmd/ateom-gvisor/`) |
| `area/microVM` | Micro-VM sandbox specifics: cloud-hypervisor, kata, snapshot/restore (`cmd/ateom-microvm/`) |
| `area/storage` | Snapshot storage, image cache, GCS/S3 backends, retention (`internal/imagecache/`, ActorSnapshot) |
| `area/scheduling` | WorkerPool sizing, actor placement, HPA, resource allocation |
| `area/observability` | Metrics, tracing, logging, OTLP, OTel SDK integration |
| `area/security` | Auth, mTLS, RBAC, threat detection, credential management, PodCertificate |
| `area/identity` | Actor identity, token issuance, JWT auth, ActorIdentity extension |
| `area/dev-infra` | CI/CD, GitHub Actions, presubmit checks, tooling, proto generation |
| `area/tests` | Test coverage, flaky tests, test infrastructure (not a bug in production code) |
| `area/cli` | `kubectl-ate` CLI plugin |
| `area/demos` | Demo applications in `demos/` |
| `area/benchmarking` | Performance benchmarks and capacity measurements (`benchmarking/`) |
| `area/reliability` | Reliability, stability, error recovery, and uptime concerns |

### `prio/` — priority

Only apply when the issue body or a maintainer comment makes the priority evident.
Do not guess.

| Label | Meaning |
|---|---|
| `prio/P0` | Highest priority; required for next milestone or blocking ongoing work |
| `prio/P1` | Important but not blocking; should land in the near term |
| `prio/P2` | Real issue but not urgent; can wait |

### Status labels

Apply only when clearly appropriate. Never add `wontfix`, `duplicate`, or `invalid`
yourself — those are maintainer decisions.

| Label | When |
|---|---|
| `good first issue` | Well-scoped, self-contained, does not require deep system knowledge |
| `help wanted` | Maintainers actively want external contributions |

---

## Classification guide

Work through these questions in order:

**1. Is something broken?**
If the current behavior is incorrect, it is `kind/bug`. Security holes and data
loss are bugs, not features.

**2. Does it ask for new capability?**
If the system would need to do something it cannot do today, it is `kind/feature`.
Apply `kind/feature` even if the issue is framed as "support X" or "add Y".

**3. Is it a design question or investigation?**
If the issue is asking *how* to do something, comparing approaches, measuring
capacity, or researching unknowns before any code is written, it is `kind/design`.
Keywords: "Design discussion", "Investigate", "Explore options", "Identify what",
"Measure", "Research".

The clearest way to distinguish `kind/design` from `kind/feature`:
- **`kind/feature`**: you could open a PR today — the *what* and *how* are known.
- **`kind/design`**: a decision or investigation must happen first; a PR cannot be written yet.

When in doubt, ask: *"Could someone reasonably start coding this issue right now?"*
If yes → `kind/feature`. If no → `kind/design`.

**4. Is it a code quality improvement with no behavior change?**
Refactoring, renaming, consolidating duplicated code, removing dead code → `kind/cleanup`.

**5. Is it documentation only?**
Docs fixes, missing API guide sections, README updates → `kind/docs`.

For `area/` labels, use the component the issue is *about*, not where the fix will
necessarily land. A bug in actor scheduling reported via the API is `area/api` +
`area/scheduling`, not `area/api-machinery`.

---

## Process

Steps 1–3 are read-only. Nothing is written to GitHub until the user approves the
proposal in Step 3.

### Step 1 — Check for missing labels

List the repo's labels and compare against the taxonomy above:

```bash
gh label list --repo agent-substrate/substrate --limit 100 --json name --jq '.[].name'
```

Do not create anything yet. If any taxonomy label is missing, list its creation in
the proposal.

### Step 2 — Fetch and classify issues

Fetch unlabeled issues first:

```bash
gh issue list --repo agent-substrate/substrate \
  --state open --limit 500 \
  --json number,title,labels,body \
  --jq '[.[] | select(.labels | length == 0)]'
```

Classify each one using the guide above. Then scan already-labeled issues for
obvious gaps:
- Issues with a `kind/` label but no `area/` label
- Issues with `area/` labels but no `kind/` label
- Issues that look like `kind/design` but are filed as `kind/feature`

Do not propose changes to labels that look reasonable even if you might have chosen
differently. Only correct clear mismatches (for example, a bug filed as
`kind/feature`, or a design discussion with no `kind/` label at all).

Only propose labels an issue does not already have, so the undo record in Step 4
removes exactly what this run added and nothing a human applied earlier.

### Step 3 — Present the proposal and wait for approval

Output a summary of every change you intend to make, then **stop and wait**. Do not
apply anything in the same turn as the proposal.

```
## Proposed changes

Labels to create: kind/design, area/benchmarking

| Issue | Title (truncated) | Labels to add |
|---|---|---|
| #900 | Implement chain of authenticator... | kind/feature, area/identity, area/api-machinery |
| #874 | Design discussion: WorkerPool vs... | kind/design, area/scheduling |

### Uncertain (best guess included above, please confirm)
- #812 — could be kind/bug or kind/design: unclear whether the behavior is intended.

Reply to approve all, or list the issue numbers to change or skip.
```

List every uncertain issue with the question you could not resolve from the text
alone. Do not leave those out of the proposal — include your best guess and flag it.

If the user asks for corrections, update the proposal and confirm again. Apply only
what the user approved.

### Step 4 — Apply approved changes and record the undo

Create any approved missing labels with the `gh label create` command above, then
apply labels to each approved issue:

```bash
gh issue edit <number> \
  --repo agent-substrate/substrate \
  --add-label "kind/bug,area/network"
```

Use `--add-label`, not `--label`, so you don't overwrite existing labels.

As you apply, build an undo script at `/tmp/triage-issues-undo-<YYYYMMDD-HHMMSS>.sh`
containing one line per successfully applied change, reversing it:

```bash
#!/usr/bin/env bash
# Undo record for triage-issues run on 2026-09-04 14:03 UTC.
# Removes only the labels this run added; labels applied by humans are untouched.
set -euo pipefail
gh issue edit 900 --repo agent-substrate/substrate --remove-label "kind/feature,area/identity,area/api-machinery"
gh issue edit 874 --repo agent-substrate/substrate --remove-label "kind/design,area/scheduling"
# Label deletions must come last: deleting a label strips it from all issues.
# Only present if this run created the label.
gh label delete "kind/design" --repo agent-substrate/substrate --yes
```

Rules for the undo script:
- Record only changes that actually succeeded, not the full proposal.
- Include a `--remove-label` line per issue listing exactly the labels this run added.
- Include `gh label delete` only for labels this run created, and place those lines
  after all `--remove-label` lines.
- Make it executable: `chmod +x <script>`.

### Step 5 — Report

After applying, output a table of what was actually done:

```
| Issue | Title (truncated) | Labels applied |
|---|---|---|
| #900 | Implement chain of authenticator... | kind/feature, area/identity, area/api-machinery |
```

Note any approved changes that failed to apply. Then point at the undo record:

```
To undo everything this run changed:
  bash /tmp/triage-issues-undo-20260904-140312.sh
```

---

## Classification examples

These illustrate the taxonomy applied to real issues in this repo:

| Title pattern | `kind/` | `area/` |
|---|---|---|
| "X is broken / returns wrong value / panics" | bug | affected component |
| "Add support for X / Implement Y" | feature | affected component |
| "Design discussion: should X or Y" | design | affected component |
| "Identify what metrics to use for Z" | design | observability, scheduling |
| "Measure / benchmark X capacity" | design | benchmarking, affected component |
| "Consolidate / Reorganize / Rework X" | cleanup | affected component |
| "Update docs / Add doc for X" | docs | (omit area unless specific) |
| "E2E test flaky: TestFoo" | bug | tests, affected component |
| "Presubmit doesn't catch X" | bug | dev-infra |

---

## Notes

- An issue can have multiple `area/` labels. Apply all that fit.
- A single `kind/` label per issue is the norm. Applying two is unusual.
- Do not add `prio/` labels to issues that don't clearly warrant one. Unlabeled
  priority is better than a wrong priority label.
- Never remove a label a human applied. If you think it is wrong, note it in your
  report and let a maintainer decide.

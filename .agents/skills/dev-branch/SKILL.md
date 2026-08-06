---
name: dev-branch
description: Parallel development using hack/dev-branch.sh to isolate worktrees and Kind test clusters per feature.
---

# Multi-Agent Parallel Development with `hack/dev-branch.sh`

This skill provides workflow guidance for executing concurrent development tasks,
working on separate features within the `agent-substrate/substrate` repository.

By isolating each feature inside its own **Git worktree** and **Kind Kubernetes
cluster**, multiple agents can build, test, and debug features in parallel
without step-on or port/cluster resource contention.

---

## Architecture & Principles

| Resource | Isolation Strategy | Purpose |
| :--- | :--- | :--- |
| **Source Code** | Git Worktree (`.worktrees/<branch>`) | Independent code branch & checkout per agent |
| **Kubernetes Cluster** | Kind Cluster (`kind-dev-<branch>`) | Dedicated cluster running `ate-system` per agent |
| **Container Registry** | Shared Docker Registry (`kind-registry:5001`) | Centralized image storage shared safely across clusters |
| **Environment Vars** | `.dev-env.sh` / `dev-branch.sh exec` | Auto-routes `kubectl` & E2E tests to the correct cluster |

---

## Step-by-Step Workflow

### 1. Provision Environment

When starting a feature or delegating a task to a subagent, provision an
isolated environment:

```bash
./hack/dev-branch.sh setup <branch-name>
```

- **Branch Name Format**: Use distinct, descriptive names (e.g.,
  `agent-router-fix`, `agent-valkey-backup`).
- **Path**: Created automatically under `.worktrees/<sanitized-branch>`.

### 2. Spawning Subagents (Parallel Execution)

When spawning parallel subagents using `invoke_subagent`:
- Pass the target worktree path (`.worktrees/<branch-name>`) in the prompt.
- Instruct subagents to use `./hack/dev-branch.sh exec <branch-name> --
  <command>` for running commands against their specific environment.

Example prompt fragment:

> You are assigned to implement feature X in branch `agent-feature-x`.
> Work inside the directory `.worktrees/agent-feature-x`.
> Run tests using `./hack/dev-branch.sh exec agent-feature-x -- ./hack/run-e2e-kind.sh`.

### 3. Executing Commands & Testing

Instead of manually setting environment variables in every subshell, use `exec`:

```bash
# Run unit tests inside the worktree
./hack/dev-branch.sh exec <branch-name> -- go test ./...

# Run E2E tests against the branch's dedicated Kind cluster
./hack/dev-branch.sh exec <branch-name> -- ./hack/run-e2e-kind.sh
```

Alternatively, `eval` the environment in the subshell:
```bash
cd .worktrees/<branch-name>
eval "$(/path/to/main/hack/dev-branch.sh env <branch-name>)"
./hack/run-e2e-kind.sh
```

### 4. Avoiding Resource & Port Conflicts

- **Kubernetes Contexts**: Contexts are automatically isolated per cluster
  (`kind-kind-dev-<branch-name>`).
- **Port Forwarding**: When port-forwarding services (e.g. `atenet-router`),
  select dynamic host ports or unique ports per agent to avoid collisions:

  ```bash
  # Use random available host port
  kubectl --context kind-kind-dev-<branch-name> port-forward -n ate-system svc/atenet-router :80
  ```

### 5. Listing Active Environments

Check all active parallel environments:

```bash
./hack/dev-branch.sh list
```

### 6. Teardown

When work on a feature is finished and merged/submitted:

```bash
./hack/dev-branch.sh teardown <branch-name>
```

- Deletes the `kind-dev-<branch-name>` cluster.
- Removes the `.worktrees/<branch-name>` directory.
- Preserves the shared `kind-registry` container if other agents' Kind clusters
  are still active.

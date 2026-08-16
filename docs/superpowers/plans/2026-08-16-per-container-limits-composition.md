# Per-Container Limits Composition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make per-container limits (PR #859) survive alongside actor-level sandbox sizing (#679, merged), so a declared container limit is what binds inside the guest.

**Architecture:** #679 stamps the actor-level size onto every container's OCI spec, overwriting the per-container limits atelet writes into the same field. We stop stamping user containers on the micro-VM path, remove the plumbing that becomes dead as a result, and leave the gVisor path untouched so its pause-container enforcement still works.

**Tech Stack:** Go 1.26, OCI runtime-spec, kata-agent over ttrpc, cloud-hypervisor, controller-gen/CEL for CRD validation, envtest for API-server-backed tests.

**Spec:** `docs/superpowers/specs/2026-08-15-per-container-limits-composition-design.md`

## Global Constraints

- Branch: `eliranw/microvm-container-resources`, PR #859 against `agent-substrate/substrate`.
- All micro-VM tests are `//go:build linux` and are **silently skipped on macOS**. Every test run in this plan must go through Docker: `docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache golang:1.26 go test <pkgs>`.
- `cmd/atelet` currently fails `TestUploadLocalCheckpointDir` on upstream `main`; that is pre-existing (PR #857) and not caused by this work. Do not attempt to fix it.
- Do not change `internal/sizing/sizing.go`. gVisor depends on its current behaviour.
- The comment convention in this repo explains current behaviour, never the change history. No "we removed X" comments.
- Every commit message body explains why, not what.

---

### Task 1: Rebase onto upstream/main

**Files:**
- Modify: whole branch (47 commits behind; 9 files shared with #679)

**Interfaces:**
- Consumes: nothing
- Produces: a branch based on `upstream/main` where `cmd/ateom-microvm/spec.go` contains both `mergeKataResources` (ours) and `size.ApplyToOCISpec(&spec)` (theirs), and `cmd/ateom-microvm/run.go` contains both `checkResourceEnvelope` (ours) and `resolveGuestMemMiB`/`guestSize` (theirs).

- [ ] **Step 1: Fetch upstream and record a recovery point**

```bash
git fetch upstream main
git branch -f backup/pre-679-rebase HEAD
git log --oneline upstream/main..HEAD | cat   # expect 7 commits
```

- [ ] **Step 2: Start the rebase**

```bash
git rebase upstream/main
```

Expect conflicts. Resolve them with these rules:

- `internal/proto/ateletpb/atelet.pb.go` — **do not hand-merge.** Take either side to clear the markers, then regenerate:
  ```bash
  git checkout --ours internal/proto/ateletpb/atelet.pb.go
  (cd internal/proto/ateletpb && go generate ./...)
  git add internal/proto/ateletpb/atelet.pb.go internal/proto/ateletpb/atelet.proto
  ```
  Verify the regenerated file contains both sides: `grep -c "ResourceLimits" internal/proto/ateletpb/atelet.pb.go` and `grep -c "UploadPausedCheckpoint" internal/proto/ateletpb/atelet.pb.go` must both be non-zero.

- `cmd/ateom-microvm/spec.go` — keep **both**: our `mergeKataResources(...)` call and their `size.ApplyToOCISpec(&spec)`. Task 2 removes the latter; do not remove it here, so that Task 2's test can be shown to fail first.

- `cmd/ateom-microvm/run.go` — keep **both**. The final order inside `RunWorkload` must be:
  1. `memMiB, vcpus, kparams, err := s.guestConfig(rr)`
  2. `sz := p.size` / vCPU override / `memMiB, err = resolveGuestMemMiB(...)`
  3. `guestSize, err := s.guestSize(sz)`
  4. `ctrs, err := s.buildActorContainers(actorUID, containers, guestSize)`
  5. `if err := checkResourceEnvelope(ctrs, memMiB, vcpus); err != nil { return err }`

  Our `checkResourceEnvelope` call must come **after** `resolveGuestMemMiB`, so `memMiB` is the post-reserve guest size.

- `pkg/api/v1alpha1/actortemplate_validation_test.go` — if our side re-adds `missing PauseImage` / `unpinned PauseImage` cases, delete them. Upstream removed `PauseImage` from `ActorTemplateSpec` in #848.

- [ ] **Step 3: Confirm the rebase completed**

```bash
git status --porcelain | grep -v '^??'    # expect empty
git log --oneline upstream/main..HEAD | cat
```

- [ ] **Step 4: Build and test on Linux**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go build ./...
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/... ./pkg/api/v1alpha1/... ./cmd/ateapi/...
```

Expected: build clean; all listed packages PASS. (`cmd/atelet` is excluded here because of the pre-existing failure noted in Global Constraints.)

- [ ] **Step 5: Push the rebase**

```bash
git push --force-with-lease origin eliranw/microvm-container-resources
```

---

### Task 2: Stop stamping user containers on the micro-VM path

**Files:**
- Modify: `cmd/ateom-microvm/spec.go` (remove the `size.ApplyToOCISpec(&spec)` call in `ensureKataCompatibleSpec`)
- Test: `cmd/ateom-microvm/spec_test.go`

**Interfaces:**
- Consumes: `ensureKataCompatibleSpec(bundle, id, netnsPath string, size sizing.SandboxSize) (*specs.Spec, error)` from Task 1
- Produces: same signature, unchanged for now (Task 3 removes the dead `size` parameter). After this task a container's declared `linux.resources` survives into the returned spec and the rewritten `config.json`.

- [ ] **Step 1: Write the failing test**

Add to `cmd/ateom-microvm/spec_test.go`. Add `"encoding/json"`, `"os"`, and `"path/filepath"` to the import block, and `"github.com/agent-substrate/substrate/internal/sizing"` to the third-party group.

```go
// A container's own declared limit is what must bind inside the guest. The
// actor-level size sizes the VM; stamping it over each container would replace
// the declared value with the actor total and silently unbound the container.
func TestEnsureKataCompatibleSpec_KeepsDeclaredContainerLimits(t *testing.T) {
	const declared = 64 * 1024 * 1024
	bundle := t.TempDir()
	in := specs.Spec{Linux: &specs.Linux{Resources: &specs.LinuxResources{
		Memory: &specs.LinuxMemory{Limit: ptr.To(int64(declared))},
	}}}
	b, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshaling input spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}

	// An actor-level size far larger than the container's own limit.
	got, err := ensureKataCompatibleSpec(bundle, "actor-uid", "/proc/1/ns/net",
		sizing.SandboxSize{MemoryBytes: 2048 * 1024 * 1024, MilliCPU: 4000})
	if err != nil {
		t.Fatalf("ensureKataCompatibleSpec() = %v", err)
	}

	if got.Linux.Resources.Memory == nil || got.Linux.Resources.Memory.Limit == nil {
		t.Fatal("memory limit = nil, want the declared 64Mi")
	}
	if v := *got.Linux.Resources.Memory.Limit; v != declared {
		t.Errorf("memory limit = %d, want %d (the container's own declared limit)", v, declared)
	}
}

// A container that declares nothing must stay unbounded inside the guest: guest
// RAM is the real ceiling, and a cap equal to the whole guest can never bind.
func TestEnsureKataCompatibleSpec_LeavesUndeclaredContainerUnlimited(t *testing.T) {
	bundle := t.TempDir()
	in := specs.Spec{Linux: &specs.Linux{}}
	b, err := json.Marshal(&in)
	if err != nil {
		t.Fatalf("marshaling input spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), b, 0o600); err != nil {
		t.Fatalf("writing config.json: %v", err)
	}

	got, err := ensureKataCompatibleSpec(bundle, "actor-uid", "/proc/1/ns/net",
		sizing.SandboxSize{MemoryBytes: 2048 * 1024 * 1024, MilliCPU: 4000})
	if err != nil {
		t.Fatalf("ensureKataCompatibleSpec() = %v", err)
	}

	if m := got.Linux.Resources.Memory; m != nil && m.Limit != nil && *m.Limit > 0 {
		t.Errorf("memory limit = %d, want unset for a container that declared none", *m.Limit)
	}
	if c := got.Linux.Resources.CPU; c != nil && c.Quota != nil && *c.Quota > 0 {
		t.Errorf("cpu quota = %d, want unset for a container that declared none", *c.Quota)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/ -run TestEnsureKataCompatibleSpec -v
```

Expected: both FAIL. The first reports `memory limit = 2147483648, want 67108864`; the second reports a non-zero limit and quota.

- [ ] **Step 3: Remove the stamp**

In `cmd/ateom-microvm/spec.go`, inside `ensureKataCompatibleSpec`, delete these five lines:

```go
	// Right-size the guest container cgroup to the actor's declared limits; the
	// kata-agent applies spec.Linux.Resources inside the VM. Shared with the gVisor
	// runtime via internal/sizing; overlays the device allowlist + CPU shares set
	// by defaultKataResources.
	size.ApplyToOCISpec(&spec)
```

Leave everything else, including the `size` parameter, untouched.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/... -v -run TestEnsureKataCompatibleSpec
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/...
```

Expected: both new tests PASS, whole package PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/ateom-microvm/spec.go cmd/ateom-microvm/spec_test.go
git commit -m "fix(ateom-microvm): let a container's own limit bind in the guest

The actor-level size sizes the VM. Stamping it onto every container replaced
each container's declared limit with the actor total, so a container that asked
for 64Mi got the whole guest and could never be OOM-killed on its own. A
container now gets a cgroup limit only when it declares one; an undeclared
container is bounded by guest RAM, which is the real ceiling."
```

---

### Task 3: Remove the sizing plumbing that is now dead

**Files:**
- Modify: `cmd/ateom-microvm/spec.go` (drop the `size` parameter)
- Modify: `cmd/ateom-microvm/run.go` (drop the `size` parameter from `buildActorContainers`; drop the `guestSize` call and the `guestSize` method)
- Modify: `cmd/ateom-microvm/restore.go` (drop the `guestSize` call)

**Interfaces:**
- Consumes: `ensureKataCompatibleSpec(bundle, id, netnsPath string, size sizing.SandboxSize)` and `buildActorContainers(actorUID string, containers []*ateompb.Container, size sizing.SandboxSize)` from Task 2
- Produces: `ensureKataCompatibleSpec(bundle, id, netnsPath string) (*specs.Spec, error)` and `(s *AteomService) buildActorContainers(actorUID string, containers []*ateompb.Container) ([]actorContainer, error)`

This task is mechanical and changes no behaviour. It is separate from Task 2 so a reviewer can accept the behaviour change and reject the cleanup, or vice versa.

- [ ] **Step 1: Drop the parameter from `ensureKataCompatibleSpec`**

In `cmd/ateom-microvm/spec.go`:

```go
func ensureKataCompatibleSpec(bundle, id, netnsPath string) (*specs.Spec, error) {
```

Remove the now-unused `"github.com/agent-substrate/substrate/internal/sizing"` import from this file if nothing else in it refers to `sizing`.

- [ ] **Step 2: Drop the parameter from `buildActorContainers`**

In `cmd/ateom-microvm/run.go`:

```go
func (s *AteomService) buildActorContainers(actorUID string, containers []*ateompb.Container) ([]actorContainer, error) {
```

and inside it:

```go
		spec, err := ensureKataCompatibleSpec(bundle, actorUID, netnsPath)
```

- [ ] **Step 3: Update the two call sites and delete the `guestSize` method**

In `cmd/ateom-microvm/run.go`, replace the `guestSize` call and the `buildActorContainers` call with:

```go
	ctrs, err := s.buildActorContainers(actorUID, containers)
	if err != nil {
		return err
	}
```

In `cmd/ateom-microvm/restore.go`, the same:

```go
	ctrs, err := s.buildActorContainers(actorUID, containers)
	if err != nil {
		return err
	}
```

Then delete the whole `func (s *AteomService) guestSize(sz sizing.SandboxSize) (sizing.SandboxSize, error)` method from `run.go`.

`resolveGuestMemMiB` stays: `RunWorkload` still calls it directly to size the VM, and that call is what rejects a declared limit too small to boot.

- [ ] **Step 4: Update the tests written in Task 2**

Those tests still pass a `sizing.SandboxSize` argument that no longer exists. In `cmd/ateom-microvm/spec_test.go`, change both calls to:

```go
	got, err := ensureKataCompatibleSpec(bundle, "actor-uid", "/proc/1/ns/net")
```

The two comments in those tests referring to "an actor-level size" should now read that the container's own limit is the only input, since there is no longer an actor-level size to pass. Remove the `"github.com/agent-substrate/substrate/internal/sizing"` import from `spec_test.go`.

- [ ] **Step 5: Build and test**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go build ./...
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/...
```

Expected: build clean, package PASS. If the compiler reports `sizing` imported and not used in `spec.go`, `run.go`, or `restore.go`, remove that import there too.

- [ ] **Step 6: Commit**

```bash
git add cmd/ateom-microvm/spec.go cmd/ateom-microvm/run.go cmd/ateom-microvm/restore.go cmd/ateom-microvm/spec_test.go
git commit -m "refactor(ateom-microvm): drop the container sizing parameter

Nothing applies the actor-level size to a container spec any more, so the size
threaded from RunWorkload and RestoreWorkload through buildActorContainers into
ensureKataCompatibleSpec had no reader. resolveGuestMemMiB still sizes the VM."
```

---

### Task 4: Make the envelope error explain the ceiling it enforces

**Files:**
- Modify: `cmd/ateom-microvm/spec.go` (`checkResourceEnvelope`)
- Modify: `cmd/ateom-microvm/run.go` (call site)
- Test: `cmd/ateom-microvm/spec_test.go`

**Interfaces:**
- Consumes: `checkResourceEnvelope(ctrs []actorContainer, memMiB, vcpus int) error`
- Produces: `checkResourceEnvelope(ctrs []actorContainer, env guestEnvelope) error` and `type guestEnvelope struct { memMiB, vcpus int; declaredBytes int64; reserveMiB int }`

After #679 the guest is `declared − reserve` when the actor declares limits, so telling the user to "use a SandboxConfig with a larger guest" is wrong advice in that case: they should raise `spec.resources.limits.memory`.

- [ ] **Step 1: Write the failing tests**

Add to `cmd/ateom-microvm/spec_test.go`:

```go
// When the actor declared its own size, the guest ceiling comes from that limit
// minus the VMM reserve, so the error must point at the actor's limit rather
// than at the SandboxConfig the user cannot usefully change.
func TestCheckResourceEnvelope_ErrorNamesActorLimitWhenDeclared(t *testing.T) {
	const mib = 1024 * 1024
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(2048 * mib))}},
	}}}

	err := checkResourceEnvelope([]actorContainer{ctr}, guestEnvelope{
		memMiB: 768, vcpus: 1, declaredBytes: 1024 * mib, reserveMiB: 256,
	})
	if err == nil {
		t.Fatal("checkResourceEnvelope() = nil, want an error")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", got)
	}
	for _, want := range []string{"hog", "1024", "256", "spec.resources.limits.memory"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// The ceiling containers are measured against is the post-reserve guest, not the
// declared limit and not the SandboxConfig default. RunWorkload gets there by
// calling resolveGuestMemMiB before checkResourceEnvelope; this pins the
// arithmetic those two steps compose into, which nothing else asserts.
func TestCheckResourceEnvelope_MeasuresAgainstPostReserveGuest(t *testing.T) {
	const mib = 1024 * 1024
	const declaredMiB, reserveMiB = 1024, 256

	guestMiB, err := resolveGuestMemMiB(int64(declaredMiB)*mib, reserveMiB, 2048)
	if err != nil {
		t.Fatalf("resolveGuestMemMiB() = %v", err)
	}
	if guestMiB != declaredMiB-reserveMiB {
		t.Fatalf("guest = %dMiB, want %dMiB (declared minus reserve)", guestMiB, declaredMiB-reserveMiB)
	}

	// A container asking for the full declared limit does not fit the guest,
	// because the reserve is not the container's to spend.
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(declaredMiB) * mib)}},
	}}}
	env := guestEnvelope{memMiB: guestMiB, vcpus: 1, declaredBytes: int64(declaredMiB) * mib, reserveMiB: reserveMiB}
	if err := checkResourceEnvelope([]actorContainer{ctr}, env); err == nil {
		t.Error("checkResourceEnvelope() = nil, want an error: the declared limit does not fit once the reserve is held back")
	}
}

// With no actor-level limit the guest is the SandboxConfig default, so that
// remains the right thing to point at.
func TestCheckResourceEnvelope_ErrorNamesSandboxConfigWhenUndeclared(t *testing.T) {
	const mib = 1024 * 1024
	ctr := actorContainer{name: "hog", spec: &specs.Spec{Linux: &specs.Linux{
		Resources: &specs.LinuxResources{Memory: &specs.LinuxMemory{Limit: ptr.To(int64(4096 * mib))}},
	}}}

	err := checkResourceEnvelope([]actorContainer{ctr}, guestEnvelope{memMiB: 2048, vcpus: 1})
	if err == nil {
		t.Fatal("checkResourceEnvelope() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "SandboxConfig") {
		t.Errorf("error %q does not mention SandboxConfig", err.Error())
	}
}
```

Update the existing `TestCheckResourceEnvelope` call from
`checkResourceEnvelope(tc.ctrs, 2048, 1)` to
`checkResourceEnvelope(tc.ctrs, guestEnvelope{memMiB: 2048, vcpus: 1})`.

- [ ] **Step 2: Run the tests to verify they fail**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/ -run TestCheckResourceEnvelope -v
```

Expected: compile error, `undefined: guestEnvelope`.

- [ ] **Step 3: Implement**

In `cmd/ateom-microvm/spec.go`, add above `checkResourceEnvelope`:

```go
// guestEnvelope is the ceiling an actor's container limits must fit inside.
// declaredBytes is the actor-level memory limit (0 when the template declares
// none), and reserveMiB the guest RAM held back for the VMM; together they
// explain where memMiB came from, so the error can name the field the user can
// actually raise.
type guestEnvelope struct {
	memMiB        int
	vcpus         int
	declaredBytes int64
	reserveMiB    int
}

// remedy names the field that raises the ceiling.
func (e guestEnvelope) remedy() string {
	if e.declaredBytes > 0 {
		return fmt.Sprintf("raise spec.resources.limits.memory (declared %d bytes, less the %dMiB VMM reserve) or lower the container limits",
			e.declaredBytes, e.reserveMiB)
	}
	return "lower the limits or use a SandboxConfig with a larger guest"
}
```

Change the signature to `func checkResourceEnvelope(ctrs []actorContainer, env guestEnvelope) error`, replace `memMiB` with `env.memMiB` and `vcpus` with `env.vcpus` throughout its body, and end each of the four `status.Errorf` messages with `%s` fed by `env.remedy()` in place of the current trailing advice. For example the per-container memory error becomes:

```go
			return status.Errorf(codes.InvalidArgument,
				"container %q asks for %d bytes of memory but the guest has %d MiB; %s",
				c.name, limit, env.memMiB, env.remedy())
```

Add `"fmt"` to the imports if it is not already there.

- [ ] **Step 4: Update the call site**

In `cmd/ateom-microvm/run.go`:

```go
	if err := checkResourceEnvelope(ctrs, guestEnvelope{
		memMiB:        memMiB,
		vcpus:         vcpus,
		declaredBytes: sz.MemoryBytes,
		reserveMiB:    s.memReserveMiB,
	}); err != nil {
		return err
	}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/...
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cmd/ateom-microvm/spec.go cmd/ateom-microvm/run.go cmd/ateom-microvm/spec_test.go
git commit -m "fix(ateom-microvm): point the envelope error at the field that raises the ceiling

When the actor declares its own size the guest is that limit minus the VMM
reserve, so advising a larger SandboxConfig sends the user to a knob that has no
effect. The error now names spec.resources.limits.memory and the reserve when
the actor declared a limit, and keeps the SandboxConfig advice when it did not."
```

---

### Task 5: Document both layers in the API guide

**Files:**
- Modify: `docs/api-guide.md` (the `### Per-container limits` section)

**Interfaces:**
- Consumes: behaviour established in Tasks 2-4
- Produces: no code

The current text states the guest size "comes from the pool's `SandboxConfig`, not from the actor". After #679 that is backwards.

- [ ] **Step 1: Replace the "Bounded by the guest" paragraph**

In `docs/api-guide.md`, replace the paragraph beginning `**Bounded by the guest.**` with:

```markdown
**Two layers.** `spec.resources` sizes the sandbox itself: for a micro-VM the guest's RAM and vCPU count, for gVisor the sentry. Per-container limits subdivide that sandbox. A container gets a cgroup limit only when it declares one; a container that declares none is bounded by the guest as a whole, not by a copy of the actor's total.

**Bounded by the guest.** A micro-VM guest is sized from `spec.resources.limits.memory` minus a small reserve held back for the VMM, or from the pool's [`SandboxConfig`](#3-sandboxconfig-the-sandbox-itself) when the template declares no actor-level limit. A container limit above that ceiling, or limits summing above it across the actor's containers, can never bind, so the actor fails to start with an error naming both the limit and the ceiling.

**Over-subscription is caught at actor start, not at apply.** Admission validates each limit on its own; the sum is checked when the actor first runs, against the real guest size. A template whose container limits do not fit is accepted by the API server and fails on its first actor.
```

- [ ] **Step 2: Verify the anchor resolves**

```bash
grep -n "sandboxconfig-the-sandbox-itself" docs/api-guide.md
grep -nE "^#{2,4} .*SandboxConfig" docs/api-guide.md
```

Expected: the link target matches a real heading (`## 3. SandboxConfig: The Sandbox Itself` → `#3-sandboxconfig-the-sandbox-itself`).

- [ ] **Step 3: Commit**

```bash
git add docs/api-guide.md
git commit -m "docs: describe actor-level and per-container limits as two layers

The guide said a micro-VM guest is sized by the SandboxConfig and not by the
actor, which is backwards now that the guest is sized from the declared actor
limit. Also states where over-subscription is caught, so nobody assumes
admission rejects it."
```

---

### Task 6: Update PR #859 and raise the narrowing with the #679 author

**Files:**
- Modify: `.pr-body.md`

**Interfaces:**
- Consumes: all prior tasks
- Produces: an updated PR ready for review

- [ ] **Step 1: Run the full test suite one last time**

```bash
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go build ./...
docker run --rm -v "$PWD":/src -w /src -e GOFLAGS=-mod=vendor -e GOCACHE=/tmp/gocache \
  golang:1.26 go test ./cmd/ateom-microvm/... ./cmd/ateapi/... ./pkg/api/v1alpha1/...
```

Expected: build clean, all PASS.

- [ ] **Step 2: Replace the "Relationship to #679" section in `.pr-body.md`**

```markdown
## Relationship to #679

#679 (merged) adds actor-level sizing via `ActorTemplate.spec.resources`, which sizes the sandbox: guest RAM and vCPUs for a micro-VM, the sentry for gVisor. This PR adds per-container limits, which subdivide that sandbox. A container gets a cgroup limit only when it declares one.

#679 applied the actor-level size to every container's OCI spec, which overwrote the per-container limits this PR writes into the same field — a 64Mi container silently received the whole guest. This PR removes that call on the micro-VM path, so each container keeps its own declared limit and an undeclared container stays bounded by guest RAM. On micro-VM the stamp was close to a no-op anyway: a cap equal to the whole guest can never bind.

gVisor is untouched. There the actor-level size is applied to the `pause` container, and since one sentry backs every container in the actor, that is the only cgroup that binds. Per-container limits stay rejected at admission for gVisor.

The `sizing.SandboxSize` threaded into `buildActorContainers` / `ensureKataCompatibleSpec` had no remaining reader once the call was removed, so it and the `guestSize` helper are gone. `resolveGuestMemMiB` still sizes the VM and still rejects a declared limit too small to boot.

`internal/e2e/suites/sizing` is unaffected: it asserts on `num_cpu` and `mem_total_bytes`, both of which come from VM sizing, and only logs the cgroup files.
```

- [ ] **Step 3: Push and update the PR**

```bash
git push --force-with-lease origin eliranw/microvm-container-resources
gh pr edit 859 --repo agent-substrate/substrate --body-file .pr-body.md
```

- [ ] **Step 4: Post the coordination comment**

Do not post without the user's approval of the text — they post their own GitHub prose. Present this draft and ask:

```markdown
@chw120 heads-up: this PR narrows one thing #679 introduced.

`ensureKataCompatibleSpec` applied the actor-level `SandboxSize` to every container's OCI spec. atelet writes each container's own declared limits into that same field, so the actor-level value overwrote them and a container asking for 64Mi ended up with the whole guest.

This PR removes that call on the micro-VM path only. The actor-level limit still sizes the VM through `resolveGuestMemMiB`, and gVisor is untouched — there the stamp on `pause` is the cgroup that actually binds, so it stays. On micro-VM the per-container stamp was close to a no-op regardless, since a cap equal to the whole guest can never bind.

That left `guestSize` and the `SandboxSize` parameter on `buildActorContainers` with no reader, so they are removed too. `internal/sizing` is unchanged.
```

- [ ] **Step 5: Verify the PR shows what you expect**

```bash
gh pr view 859 --repo agent-substrate/substrate --json state,mergeable,commits,files \
  --jq '{state, mergeable, commits: (.commits|length), files: (.files|length)}'
```

Expected: `MERGEABLE`, roughly 11-12 commits, and no files outside `cmd/ateom-microvm/`, `cmd/atelet/`, `cmd/ateapi/`, `pkg/api/v1alpha1/`, `internal/proto/ateletpb/`, `manifests/`, `docs/`.

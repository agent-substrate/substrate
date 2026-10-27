# Preview gates

A preview gate lets a new feature ship in a release while staying off by
default. This gives users an opportunity to try things but protects them from
accidentally using things that we are not confident in.  Operators enable gates
explicitly on the server side, and API clients must also say they mean to use
preview features. Gates are coarse switches; they are not access control.

There are only two states: a feature is either **preview** (gated, off unless
enabled) or **GA** (always on). There is no alpha or beta.

## How a gate is enabled

Two independent switches must both be on before a request can use a gated
feature:

1. **Server side:** every substrate binary takes `--preview=<gates>`, a
   comma-separated list of gate names, or `ALL` for all of them. Install with
   ate-setup's `preview` setting (`ATE_PREVIEW`, `--preview=<gates>`), which
   passes the same value to every substrate container. atecontroller passes its
   own gates on to the ateworker containers of each WorkerPool.
2. **Client side:** a gRPC request to ate-api must send the
   `Ate-Preview-Allowed: true` metadata header (`preview.Header`).
   `kubectl ate --preview` sends it. A request without it is treated as if
   every gate were off.

Substrate's own components (atelet, atenet, atecontroller) dial ate-api with
`preview.DialOptionsWithOptIn()`, so they are never blocked by the header
check. ate-setup does the same when it is run with preview gates.

## Adding a gate

Declare the gate in `internal/preview/preview.go`:

```go
const (
	GateExternalVolumes Gate = "ExternalVolumes"
	GateMyFeature       Gate = "MyFeature"
)

var realKnownGates = []Gate{
	GateExternalVolumes,
	GateMyFeature,
}
```

Gate should always be named in UpperCamelCase.

## Gating API fields

Gate the fields a **user** sets with a declarative validation tag in the
`.proto`, then regenerate (`hack/update/codegen.sh`):

```proto
  // +ate:preview=MyFeature
  // +k8s:ifDisabled(MyFeature)=+k8s:forbidden
  // +k8s:optional
  MyFeatureConfig my_feature = 7;
```

- `+ate:preview=<Gate>` marks the field as part of a preview feature, for
  readers and docs. It does not enforce anything.
- `+k8s:ifDisabled(<Gate>)=+k8s:forbidden` rejects the field when the gate is
  off for this request.

The gate state reaches the validators through `apivalidation.MakeCreateOp(ctx)`
and `apivalidation.MakeUpdateOp(ctx)`. Every `Validate*` wrapper must build its
`operation.Operation` with one of these, not with a bare
`operation.Operation{...}` literal; otherwise the field is never gated.

Unchanged values are not re-validated on update, so an object created while a
gate was on stays usable after the gate is turned off.

**Do not gate fields the server writes**, such as `status`. The server writes
them while doing system work, such as deleting an actor, and gating them can
leave objects that cannot be cleaned up after a gate is turned off.  This is
subtle and will require careful thinking when implementing a gated feature.

## Gating behavior in code

For a check made while handling a user request, use the context-aware form:

```go
if !preview.IsEnabledFor(ctx, preview.GateMyFeature) {
	return nil, resources.ToAPIError(field.ErrorList{field.Forbidden(
		path, "requires the MyFeature preview gate and a client that opts in to "+
			"preview features (kubectl ate --preview, or the "+preview.Header+" header)")})
}
```

Use `preview.IsEnabled(gate)` only for checks that are about the process, not
the request, e.g. whether a component should start a feature at all.

Be deliberate about what happens when a gate is turned **off** while the
feature is in use. Prefer to keep cleanup paths (delete, suspend, detach)
working regardless of the gate, so that turning it off cannot strand
resources.

## Request context and workflows

ate-api's gRPC interceptor marks a request's context as opted out unless it
sent the header. Work that a request starts but that runs on the system's
behalf must not inherit that restriction. Each exported `ActorWorkflow` and
`WorkerWorkflow` method therefore begins with:

```go
ctx = preview.ContextWithOptIn(ctx)
```

`TestWorkflowsOptInToPreview` enforces this. Contexts that never went
through the interceptor, such as background reconcilers, are not opted out.

## Testing

Test gated code both with and without the gate.

- Unit tests: `preview.InitForTest(t, "MyFeature")`, or with no gate names for
  all-off. It restores the previous state when the test ends. Tests that use
  it cannot run in parallel.
- Tests of the gate mechanism itself use `preview.InitForTestFake` with fake
  gate names, so they don't depend on real gates.
- `apivalidation` tables: `assertValidate` takes `want` (gates off) and
  `wantWithPreview`, keyed by a `--preview` value such as `"ALL"`.
- Functional tests (`controlapi/functionaltest`): `setupPreviewTest` gives a
  client that sends the header, and `tc.noOptInClient` one that does not.
  Neither enables a gate; call `preview.InitForTest` for that.
- E2E tests: start a test of a preview feature with
  `e2e.RequirePreview(t, preview.GateMyFeature)`. It skips unless the suite
  was run with `--preview` naming that gate, which must match what the
  cluster was installed with:

  ```shell
  hack/run-e2e-kind.sh -v -args --preview=ALL
  ```

  CI runs every e2e lane with preview gates both off and on, and requires
  `--preview` to be passed (even as empty) so a lane cannot silently skip
  every preview test.

## Graduating or removing a gate

When the feature goes GA, or is removed, keep the gate name in
`realKnownGates` for a while so existing `--preview` values still parse, but
make the feature always on (GA) or always off (removed). Then delete the
`ifDisabled` tags, `IsEnabled`/`IsEnabledFor` checks, and `RequirePreview`
calls.

<!-- TODO: how do we gate a new RPC method or service? -->
<!-- TODO: how do we gate a new feature in the CLI? -->

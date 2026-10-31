# ate-api functional tests

Each test in this directory starts with a comment that describes, in plain
language, what it does and asserts. The test implements that description.
Every test of a resource lives in `<resource>_test.go`, named by the plural,
lowercase name of the resource it asserts on (`atespaces`, `actors`,
`workers`, ...).

## Test descriptions

- A description states how ate-api currently behaves. When that differs from
  how it should behave, describe today's behavior and add a `TODO:` paragraph
  saying what the server should change to. The test follows the description,
  not the TODO.
- An outcome ate-api reaches in the background, after the RPC returns, is
  described with "eventually". Every other outcome holds when the RPC returns.
- A call that can fail with ABORTED while another operation holds the
  resource's lease, such as a background reconciler, is described with
  "retrying while it fails with ABORTED".
- A resume that "retries while ate-api catches up" is retried while ate-api's
  scheduler and atelet lookup, which are caches, have not yet seen a worker
  the test just added or freed, or an atelet it stopped. Until they do, the
  resume fails with RESOURCE_EXHAUSTED, with INTERNAL saying no atelet runs on
  the node, or with UNAVAILABLE dialing the stopped atelet.
  Setup the harness does for a test, such as `h.RunningActor`, already waits
  for them, so only a resume under test is described this way.
- When you change a test, update its description to match.

## Finding gaps

When asked to find gaps, measure which server code the tests reach and report
what is missing. Do not change tests while doing so.

1. Run the suite with coverage of ate-api's server packages. ate-api runs
   inside the test process, so its code counts:

   ```bash
   go test -coverpkg=./cmd/ateapi/internal/controlapi,./cmd/ateapi/internal/workerservice,./cmd/ateapi/internal/server \
     -coverprofile=cover.out ./cmd/ateapi/internal/functionaltest/
   go tool cover -func=cover.out
   ```

2. Classify every uncovered RPC handler or branch:

   - **Missing test:** an RPC, or a branch a client can reach through the
     public API and the harness. Propose a test.
   - **Harness gap:** production code the harness skips, such as a fake
     plugged in below the code that would normally find it. Propose a harness
     change; it needs careful review.
   - **Not reachable black-box:** a branch only a store failure or a crash
     between two writes can reach. Note that it belongs in the in-package
     tests.

3. Report each proposed test as its name and a plain-language description.

Coverage measures what runs, not what is checked. Never propose a test only
to cover lines; every test describes behavior a client can observe.

## Writing tests

The tests are black-box: they drive ate-api only through its public gRPC API
and the harness in `harness/`, never through the store, `RPCService`, or any
other ate-api internal.

- Start each test with `t.Parallel()` and its own `h := harness.New(t)`. The
  harness starts an empty ate-api with its own database and fakes; create the
  atespaces, worker pools, workers and templates the test needs.
- Reach the state a description presupposes ("a paused actor", "an existing
  actor template") through the harness's fixtures, which wait as long as they
  need. Make each action or check it states ("deletes", "a later Get") as an
  RPC through `h.Control`, once unless its wording says otherwise. Never reach
  the method named by the test through a fixture.
- Write one test per case. Do not turn cases with different setup or recovery
  into a table of functions.
- Assert on whole resources: `cmp.Diff(want, got, protocmp.Transform(), ...)`.
  Ignore only fields ate-api assigns, with the options in `cmp_test.go`.
  - For a newly created resource, write `want` as a literal proto.
  - For an operation on an existing resource, read it right before the
    operation, `proto.Clone` it, and change only the fields the operation
    should change.
  - An operation that is refused must leave the resource exactly as it was:
    compare with nothing ignored.
- Assert on the fakes the same way: compare whole `Sandboxes()`,
  `Volumes.List()` or `ObjectStore` contents, not single entries.
- Check an RPC's error by its gRPC code: `status.Code(err) != codes.NotFound`.
  Assert on the error message only when the description explicitly asks for
  something in it, and then only that the message contains it:
  `strings.Contains(status.Convert(err).Message(), "actor.metadata.name")`
  for a description saying the error names the invalid field. Never compare
  the whole message.
- Inject failures through the fakes' `On(op)` faults (`FailNext`, `Block` and
  `HoldReply`), with the error presets in `harness/fault.go`.
- ate-api runs with every preview gate on, but a harness's clients do not opt
  in to preview features, so ate-api rejects preview fields as it does from a
  default client. A test that uses a preview feature, such as a template with
  an external volume, creates its harness with
  `harness.New(t, harness.WithPreviewClient())`: then `h.Control`,
  `h.ClientAs` and the fixtures opt in. In such a test,
  `h.ControlWithoutPreview()` checks what a client without the opt-in can do.
- Assert an RPC's outcome as soon as it returns: ate-api finishes an RPC's
  work before replying. Wait with `h.Eventually`, polling public RPCs, only
  for an outcome the description says happens eventually. If an assertion
  fails because the outcome arrives later, report it rather than adding a
  wait: whether the description should say "eventually" is a decision for a
  person.
- Retry a call with `harness.RetryAborted`, which stops at any result but
  ABORTED, only when the description says "retrying while it fails with
  ABORTED". Never retry a call the description expects to fail with ABORTED.
- Call `h.ResumeActor`, which retries while ate-api catches up, for a resume
  the description says "retries while ate-api catches up", and
  `h.Control.ResumeActor` once for every other resume.
- A test that fails because ate-api behaves differently from its description
  is a discrepancy to report, not a test to change: either the description is
  corrected or it gets a `TODO:` for the server.

## Authentication and authorization

ate-api runs with authentication and authorization enforced.
Control clients authenticate with a client certificate carrying
a SPIFFE ID, as in-cluster components do; ate-api accepts no bearer tokens.

- `h.Control` authenticates as a bootstrap owner, a global owner whatever the
  stored access policies say. Use it to set up a test, and for every call
  that is not about permissions.
- `h.ClientAs(name)` returns a Control client authenticating as
  `harness.ClientID(name)`. Access policies name it as the member
  `harness.Member(name)`. It holds no permissions until a policy grants them.
- To test a permission, grant a role to `harness.Member(name)` with an access
  policy created through `h.Control`, call the RPC through `h.ClientAs(name)`,
  and check the gRPC code: `OK`, or `PermissionDenied`.
- An `<Method>_Authorization` test checks one method's permission. Its
  description calls `h.Control` the admin and the `h.ClientAs` client the
  principal. Only the principal calls the method under test. The admin makes
  every grant, replacing the principal's single binding before each step, and
  makes every read that checks a refused call changed nothing.
- Only RPCs with a rule in `cmd/ateapi/internal/authz/registry.go` check
  permissions. Any authenticated principal may call the others, so a
  permission test only makes sense for an RPC with a rule.
- Atelets call WorkerService with their pod identity, through
  `Node.WorkerService()`. Access policies do not apply to those calls.
- `h.EgressGateway()` authenticates as atenet-egress, for RPCs only it may
  call, such as `MintActorJwt`.

## Acceptance

A change to these tests is done when all of these hold:

- `go vet ./...` is clean and `gofmt -l .` prints nothing.
- `go test -race -count=3 ./...` passes from this directory.
- Every new test was seen failing once: break one of its expected values, run
  it, and restore the value.

Running the tests needs Docker, for PostgreSQL, and the envtest binaries,
which `hack/run-tool.sh setup-envtest` downloads on first use.

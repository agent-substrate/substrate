# Networking cleanup

Branch `networking-cleanup`, based on upstream/main `6f3897ea` (2026-10-02). Everything is uncommitted for review.

**Size:** 85 tracked files changed, 6 files added, 1 deleted. About 1,330 net lines removed (1,478 added, 2,811 removed).

**Intent:** no behavior change. No flags, metrics, protos, wire formats, log messages, or `go.mod` were touched. Every deletion was checked for references across the repo first; every refactor is covered by an existing or new test.

## What changed

### Egress and ingress gateway (`cmd/atenet`, Envoy manifest)

- The by-address egress dial path is gone from `manifests/ate-install/atenet-egress.yaml`: three `dial: address` routes, three `ORIGINAL_DST` clusters, and the `original_dst_address` request attributes. The gateway has only answered `dial=name` since passthrough started dialing the resolved SNI, so none of it could match. Envoy validates the edited manifest offline (10 clusters before, 7 after).
- The constants that only served that path (`EgressDialAddress`, `OriginalDst*`) are deleted. Two manifest tests that always skipped are deleted; the remaining ones now assert strictly more.
- Dead code removed: the unread `extproc.Server.port`, `DefaultParkedRequestConfig`, `actorNotFoundErr`, a duplicate mock client, a hand-rolled `itoa`. Three identical error cases in `ingress/errors.go` are one case. The statusz template is parsed once instead of per request. `QueryRecorder.Get` is one loop and has its first test.
- Tests: `synctest` replaces real sleeps and polls (health timeout test 500ms to 20ms, resumer backoff, policy-cache cancel polls). A `publishedSnapshot` helper replaces 10 copies in `xds_test.go`; `t.Cleanup` replaces 9 copied defer blocks in `sdsmint`; loops over a one-element manifest list are collapsed.
- The router and Envoy READMEs no longer describe removed code (identity source, match order, attribute table, statusz port, Envoy image tag).

### atunnel and sandbox networking (`internal/atunnel`, `ateomnet`, `ateomtunnel`, `wakeupprobe`, `k8sresolver`, ateom wiring)

- Dead code removed: `ateomnet.SessionHolder`, `atunnel.DefaultConnectPort`, the `Server.newProxy` test seam, `wakeupprobe.HTTPClient`, an unreachable empty-host branch, and the `wakeupprobe.DialFunc` alias with its four call-site conversions.
- Simpler: `serveSandboxEgress` and `netns.Listen` take one port (callers only ever passed one). `CreateNamed` and `Do` share one locked-thread helper. `context.AfterFunc` replaces five select goroutines. The DNS TCP relay uses the existing `copyBothWays`.
- Tests: the IPv4 and IPv6 original-destination tests are one table, and their EPERM skips are gone, so the root tests fail closed as `docs/dev/best-practices/ci-fail-closed.md` asks. Sandbox tests share setup helpers. Poll loops use `waitFor`. Independent subtests run in parallel.
- Six hand-rolled test CAs (atunnel, ateomtunnel, router egress, router status) now use `internal/localca`, like the e2e suite already did.

### e2e harness and suites (`internal/e2e`, `hack`)

- New shared helpers, each with a unit test: `e2e.CreateActor` (create, then suspend and delete on cleanup with its own context), `e2e.WaitForActorState`, `e2e.ServicePortForward`, `RouterClient.GetJSON`, `KoApplyTemplate`, one `runCmd` behind the `RunCmd*` family, one parking-gauge poll, one metrics scrape. They replace private copies in ten suites.
- Dead code removed: `FColorf`/`FColorfln`, a duplicate trust-bundle ensure in egressmitm, a probe-ready wait and pod describer that duplicated `e2e.WaitForPodReady`, a hand-rolled pod port-forward in the demo suite, two `TestMain` wrappers.
- `hack/verify-egress-demo.sh` is deleted: nothing referenced it, and it has not worked since policy enforcement began because it never creates a policy. `demos/egress/test-egress.sh` covers the same path.

## Speed

- atenet unit tests: the sum of per-test time went from 1.12s to 0.28s. The health, resumer and statusz tests no longer sleep.
- atunnel, ateomnet, wakeupprobe, k8sresolver: 13.0s to 7.9s wall on macOS, mostly from parallel subtests and polls that stop as soon as their condition holds.
- e2e: shared helpers, no timing changes. Every wait keeps the timeout its call site had.

## Verification

- `gofmt`, `go build ./...` (darwin and linux), `go vet` and the full `hack/verify/golangci-lint.sh`: clean.
- `go test -race` on every touched package: pass.
- Linux root tests (atunnel, ateomnet, ateomtunnel, ateom-gvisor, ateom-microvm) in a privileged container: pass, zero skips.
- The new egress manifest passes `envoy --mode validate` and loads in a live atenet-egress with no config errors.
- e2e on kind: identity, egressmitm, egressauthz, multiactor and parking passed during the work. The final full run hit an overloaded single-node cluster (ateapi, atelet and the control plane restarting on probe timeouts while idle), so it needs a rerun on a fresh cluster. Of that run, egressmitm, egressauthz, metrics, capabilities and combinedvolumes passed.
- `hack/verify/boilerplate.sh` fails until the deletion of `hack/verify-egress-demo.sh` is staged.

## Found but not done

Each of these needs a judgment call, touches an in-flight PR, or is a design change rather than a cleanup.

- `cmd/ateom-gvisor/hosted.go` and `cmd/ateom-microvm/hosted.go` carry near-identical actor-map, admit, host, unhost and dialer code. Sharing it needs a type that owns the map, mutex and draining state, with per-runtime fields placed somewhere. Design change.
- `ateomtunnel.Start` takes an upstream that both callers pass as `ateomnet.ActorHTTPUpstream`. Dropping it couples ateomtunnel to ateomnet.
- `extproc` direction attribute and the agentgateway client-certificate attribute: no dataplane in the repo sets them, but agentgateway is a supported dataplane. Left alone.
- `drain_test.go` `TestDrainOnShutdownForceStopsAfterTimeout` still waits 100ms for real; `synctest` rejects it because the drain loop leaves a goroutine blocked in `GracefulStop`.
- e2e sleeps that need a judgment call: `demo_test.go` 5s after deleting a worker pod, `metrics_test.go` 2s and 3s scrape waits, `atunnel/dns_test.go` 100ms stabilization poll.
- `combinedvolumes_test.go` skips when `KO_DOCKER_REPO` or the StorageClass is missing. Per AGENTS.md those should fail in CI through a named predicate like `dockerenv.Required()`.
- About 12 e2e sites call `NewRouterClient`, check the error and defer `Close`; a `MustRouterClient` would collapse them but risks a double close.

## Reviewing

Per area: `git diff -- cmd/atenet manifests`, `git diff -- internal/atunnel internal/ateomnet internal/wakeupprobe`, `git diff -- internal/e2e hack`. New files are untracked: `internal/e2e/{actor,portforward}.go` with tests, `internal/e2e/run_test.go`, `cmd/atenet/internal/router/extproc/record_test.go`.

`hack/verify/boilerplate.sh` reports the deleted `hack/verify-egress-demo.sh` as missing until the deletion is staged.

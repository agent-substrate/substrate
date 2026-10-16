# Networking cleanup

Branch `networking-cleanup`, rebased onto upstream/main `05cb6f24` (2026-10-16). The first commit is the original cleanup with its conflicts resolved against upstream; the second round of cleanups below is uncommitted on top.

**Size, both rounds:** 139 files changed, 6 added, 1 deleted. About 1,700 net lines removed (2,024 added, 3,722 removed).

**Intent:** no behavior change. No flags, metrics, protos, wire formats or `go.mod` were touched; the only log line removed was unreachable outside tests. Every deletion was checked for references across the repo first; every refactor is covered by an existing or new test.

## What changed

### Egress and ingress gateway (`cmd/atenet`, Envoy manifest)

- The by-address egress dial path is gone from `manifests/ate-install/atenet-egress.yaml`: three `dial: address` routes, three `ORIGINAL_DST` clusters, and the `original_dst_address` request attributes. The gateway only ever answers `dial=name`, so none of it could match. Envoy validates the edited manifest offline (11 clusters before, 8 after).
- The constants that only served that path (`EgressDialAddress`, `OriginalDstIPAttribute`, `OriginalDstPortAttribute`) are deleted. `OriginalDstFilterStateKey` stays: the ingress router's upstream CONNECT sets it. The manifest test of the by-address clusters is deleted, and the passthrough test now rejects any `ORIGINAL_DST` cluster.
- Dead code removed: the unread `extproc.Server.port`, `extproc.Server.Queries`, `DefaultParkedRequestConfig`, `actorNotFoundErr`, a duplicate mock client, a credential-error case identical to the default. Three identical error cases in `ingress/errors.go` are one case. The statusz template is parsed once instead of per request. `QueryRecorder.Get` is one loop and has its first test.
- Simpler: the four ingress listener builders in `xds.go` share one `ingressListener` helper. `ParsePort` lives once, in `internal/atenet`, instead of in both atunnel packages, and the router no longer imports atunnel for it.
- Tests: `synctest` replaces real sleeps and polls (health timeout, resumer backoff, policy-cache cancel polls, drain ordering). `publishedSnapshot`, `currentSnapshot`, `listenerHCM` and `websocketDisabled` replace copies in the xDS and manifest tests; `t.Cleanup` replaces 9 copied defer blocks in `sdsmint`; loops over a one-element manifest list are collapsed; the manifest helpers reuse `manifestObject` and `routerManifestFlags`.
- Comments and READMEs no longer describe removed code: the ActorIdentity extension (the actor comes from the SPIFFE URI SAN), one pool per tunnel (pools are keyed on identity, authority and policy hash), injection on the MITM leg only and skipped without a provider (it runs on both request legs and is denied), `certauth.Signer`, an admin socket on `::`, a memory budget the egress manifest does not carry, the router's upstream path before upstream CONNECT, and an egress-only dataplane image.

### atunnel and sandbox networking (`internal/atunnel`, `internal/ateworker`, `internal/netns`, `internal/linuxsys`, `wakeupprobe`, `k8sresolver`, worker wiring)

- Dead code removed: `actornet.SessionHolder`, the ingress `Server.newProxy` test seam and its `credentialBundlePath` field, `wakeupprobe.HTTPClient`, an unreachable empty-host branch, the `wakeupprobe.DialFunc` alias with its four call-site conversions, the DNS server's egress-socket fallback that only tests reached, the test-only `allowQuery`, a duplicate microvm attribution method, and the egress policy cache's constant `ttl` field.
- Simpler: `serveSandboxEgress` and `netns.Listen` take one port (callers only ever passed one), and the sandbox session holds one egress listener. `CreateNamed` and `Do` share one locked-thread helper. `context.AfterFunc` replaces the select goroutines in the egress and ingress servers. The ingress `serve` always uses the server's TLS config. `WaitGroup.Go` replaces hand-rolled `Add`/`Done` pairs.
- Tests: the IPv4 and IPv6 original-destination tests are one table, and their EPERM skips are gone, so the root tests fail closed as `docs/dev/best-practices/ci-fail-closed.md` asks. One gateway TLS config and one test server constructor replace copies; `receiveWithin` and `waitFor` replace inline waits. Independent subtests run in parallel.
- Four hand-rolled test CAs (atunnel egress, atunnel ingress, router egress, router status) now use `internal/localca`, like the e2e suite already did. The worker tunnel test keeps its own: its self-signed worker cert carries a PodIdentity, which `localca.GenerateCA` cannot add.
- The DNS readme names functions and limits that exist.

### e2e harness and suites (`internal/e2e`, `hack`)

- New shared helpers, each with a unit test: `e2e.CreateActor` (create, then suspend and delete on cleanup with its own context, logging an actor it could not delete), `e2e.WaitForActorState`, `e2e.ServicePortForward` (and the pod variant `NewStatuszClient` needs), `RouterClient.GetJSON`, `KoApplyTemplate`, one `runCmd` behind the `RunCmd*` family, one parking-gauge poll, one metrics fetch, `PromLabelValue`. They replace private copies in fifteen suites, including the authz and security suites that landed upstream since.
- Dead code removed: `FColorf`/`FColorfln`, a duplicate trust-bundle ensure in egressmitm, a probe-ready wait and pod describer that duplicated `e2e.WaitForPodReady`, a hand-rolled pod port-forward in the demo suite, two `TestMain` wrappers, two fixture placeholders no manifest carries.
- `hack/verify-egress-demo.sh` is deleted: nothing referenced it, and it has not worked since policy enforcement began because it never creates a policy. `demos/egress/test-egress.sh` covers the same path.
- The security suite's runbook names the real container, labels and paths, and no longer says a failed run keeps its namespaces.

## Speed

- atenet unit tests: the health, resumer, drain-ordering and statusz tests no longer sleep.
- atunnel, actornet, wakeupprobe, k8sresolver: parallel subtests and polls that stop as soon as their condition holds.
- e2e: shared helpers, no timing changes. Every wait keeps the timeout its call site had.

## Verification

- `gofmt`, `go build ./...` (darwin and linux), `go vet` and the full `hack/verify/golangci-lint.sh`: clean.
- `go test -race` on every touched package, in a privileged Linux container: pass, zero skips on the root tests (original destination, sandbox egress, netns).
- The edited egress manifest passes `envoy --mode validate` in the current dataplane image (the one with the egress-policy module).
- e2e on kind ran on the first round before the rebase (identity, egressmitm, egressauthz, multiactor, parking, metrics, capabilities, combinedvolumes passed); it needs a rerun on the rebased branch.

## Found but not done

Each of these needs a judgment call, touches an in-flight PR, or is a design change rather than a cleanup.

- `cmd/ateworker-gvisor/hosted.go` and `cmd/ateworker-microvm/hosted.go` carry near-identical actor-map, admit, host, unhost and dialer code. Sharing it needs a type that owns the map, mutex and draining state, with per-runtime fields placed somewhere. Design change.
- `tunnel.Start` takes an upstream that both callers pass as `actornet.ActorHTTPUpstream`. Dropping it couples the tunnel package to actornet.
- The atunnel egress and ingress tests carry byte-identical test CA helpers. Sharing them means new exported test helpers in `internal/testcert`; `internal/ateletdial` has a third copy.
- `synctest` could replace real time in the atunnel egress renewal tests and in `wakeupprobe`'s give-up test (a 1s floor), with `-race -count` runs to prove them.
- `installEgressRules` in `actornet/sandbox.go` loops over a one-element list per family and writes the actor-subnet match twice. It is firewall code, so it is left for a focused change.
- `extproc` direction attribute and the agentgateway client-certificate attribute: no dataplane in the repo sets them, but agentgateway is a supported dataplane. Left alone.
- `drain_test.go` `TestDrainOnShutdownForceStopsAfterTimeout` still waits 100ms for real; `synctest` rejects it because the drain loop leaves a goroutine blocked in `GracefulStop`.
- e2e sleeps that need a judgment call: `demo_test.go` 5s after deleting a worker pod, `metrics_test.go` 2s and 3s scrape waits, `internal/atunnel/dns/relay_test.go` 100ms stabilization poll.
- `combinedvolumes_test.go` skips when `KO_DOCKER_REPO` or the StorageClass is missing. Per AGENTS.md those should fail in CI through a named predicate like `dockerenv.Required()`.
- Flag help text that no longer matches the code, left because flag text is user-facing: `--credential-provider-address` says "MITM-leg injector", and sdsmint's flags say the resource TTL is half the leaf TTL (it is notAfter minus a minute).

## Reviewing

Per area: `git diff upstream/main -- cmd/atenet manifests cmd/dataplane`, `git diff upstream/main -- internal/atunnel internal/atenet internal/ateworker internal/netns internal/linuxsys internal/wakeupprobe internal/k8sresolver cmd/ateworker-gvisor cmd/ateworker-microvm`, `git diff upstream/main -- internal/e2e hack`. Plain `git diff` shows only the second round.

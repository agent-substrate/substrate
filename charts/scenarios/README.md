# Chart scenarios

Each directory here is one supported configuration of the charts. A new configuration needs a new scenario.

- `<chart>-values.yaml` holds the values for one chart, for example `substrate-values.yaml`. A chart without a file uses its defaults.
- `rendered.yaml` holds the objects that the charts render with these values. It leaves out the CRDs.
- `ate-setup-flags` holds the `ate-setup` flags that install the same configuration, one flag on each line. Only a scenario that `ate-setup` can install has this file.
- `differences.yaml` lists how the `ate-setup` install and the chart install of the scenario differ, by object and field path, with the value from each install.

| Scenario | Configuration |
|---|---|
| `kind-envoy` | A kind cluster from `hack/create-kind-cluster.sh`, with the envoy data plane and the bundled PostgreSQL and RustFS |
| `kind-agentgateway` | A kind cluster from `hack/create-kind-cluster.sh`, with the agentgateway data plane and the bundled PostgreSQL and RustFS |
| `external-postgres` | The `kind-envoy` scenario with an external PostgreSQL, its own Secret names, roles and schema, and the login certificates of `substrate-podcert`. It has no `ate-setup-flags`, because `ate-setup` takes the connection strings from the environment, and the objects that the comparison test compares do not depend on them |
| `microvm` | The `kind-envoy` scenario with microVM sandboxes next to gVisor. `ate-setup` cannot install it in the comparison test, because it uploads the microVM assets |

`hack/install-charts-kind.sh` installs a scenario on a kind cluster. The `helm-e2e` workflow installs `kind-envoy` and `kind-agentgateway` with the values in `hack/e2e-chart-values` on top, which the e2e suites in `internal/e2e` need, and runs the suites against them. Its microVM jobs add the microVM settings of the `microvm` scenario to either data plane, and upload the assets with `ate-setup`.

After you change a template, a value or a scenario, run `hack/update/chart-golden.sh` to update the `rendered.yaml` files. `hack/verify/chart-golden.sh` fails when they are out of date.

The test in `cmd/ate-setup/internal/chartcompare` installs each scenario that has `ate-setup-flags` twice, each time on its own envtest API server: once with `ate-setup deploy ate-system`, and once from `rendered.yaml`. It fails when the installs differ in a way that `differences.yaml` does not list. It leaves out the objects that `ate-setup` creates in code and the charts do not render, the bundled PostgreSQL and RustFS, and the test fixtures of the kind overlay. To rewrite `differences.yaml` after a change, run:

```sh
go test ./cmd/ate-setup/internal/chartcompare -args -update-differences
```

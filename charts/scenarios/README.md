# Chart scenarios

Each directory here is one supported configuration of the charts. A new configuration needs a new scenario.

- `<chart>-values.yaml` holds the values for one chart, for example `substrate-values.yaml`. A chart without a file uses its defaults.
- `rendered.yaml` holds the objects that the charts render with these values. It leaves out the CRDs.
- `ate-setup-flags` holds the `ate-setup` flags that install the same configuration, one flag on each line. Only a scenario that `ate-setup` can install has this file.
- `differences.yaml` lists how the `ate-setup` install and the chart install of the scenario differ, by object and field path, with the value from each install.

| Scenario | Configuration |
|---|---|
| `default` | The chart defaults, with the required `atenetDataPlane` set to `envoy` |
| `kind` | A kind cluster from `hack/create-kind-cluster.sh`, with the envoy data plane and the bundled PostgreSQL and RustFS |
| `kind-agentgateway` | The `kind` scenario with the agentgateway data plane |
| `e2e` | The `kind` scenario with the settings that the e2e suites in `internal/e2e` need: the short atelet snapshot delays of `ate-setup --e2e-tuning`, and the router client of the suites |
| `microvm` | The `kind` scenario with microVM sandboxes next to gVisor. `ate-setup` cannot install it in the comparison test, because it uploads the microVM assets |

After you change a template, a value or a scenario, run `hack/update/chart-golden.sh` to update the `rendered.yaml` files. `hack/verify/chart-golden.sh` fails when they are out of date.

The test in `cmd/ate-setup/internal/chartcompare` installs each scenario that has `ate-setup-flags` twice, each time on its own envtest API server: once with `ate-setup deploy ate-system`, and once from `rendered.yaml`. It fails when the installs differ in a way that `differences.yaml` does not list. It leaves out the objects that `ate-setup` creates in code, the bundled PostgreSQL and RustFS, and the test fixtures of the kind overlay. To rewrite `differences.yaml` after a change, run:

```sh
go test ./cmd/ate-setup/internal/chartcompare -args -update-differences
```

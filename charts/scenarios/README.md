# Chart scenarios

Each directory here is one supported configuration of the charts. A new configuration needs a new scenario.

- `<chart>-values.yaml` holds the values for one chart, for example `substrate-values.yaml`. A chart without a file uses its defaults.
- `rendered.yaml` holds the objects that the charts render with these values. It leaves out the CRDs.

| Scenario | Configuration |
|---|---|
| `default` | The chart defaults |
| `kind` | A kind cluster from `hack/create-kind-cluster.sh`, with the bundled PostgreSQL and RustFS |

After you change a template, a value or a scenario, run `hack/update/chart-golden.sh` to update the `rendered.yaml` files. `hack/verify/chart-golden.sh` fails when they are out of date.

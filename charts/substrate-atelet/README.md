# substrate-atelet

Install the atelet DaemonSet as a release separate from the
[substrate](../substrate/README.md) chart, so it can be upgraded in the order
the [version skew policy](../../docs/reference/version-skew-policy.md) requires:
atelet before workers, and workers before ate-api-server.

## Install

Install this chart first, then the substrate chart in the same namespace.
Upgrade them in the same order. `values.yaml` holds the values of both
releases, including the object store settings: see "Object store" in the
[substrate chart README](../substrate/README.md#object-store).

```sh
kubectl create namespace ate-system
helm upgrade --install substrate-atelet ./charts/substrate-atelet \
  --namespace ate-system \
  -f values.yaml \
  --wait
helm upgrade --install substrate ./charts/substrate \
  --namespace ate-system \
  -f values.yaml
```

## Values

This chart reads the same value paths as the substrate chart for
`objectStorePlugin.image`, `objectStorePlugin.credentialsSecret`,
`objectStorePlugin.extraEnv`, `objectStorePlugin.extraVolumes`,
`objectStorePlugin.extraVolumeMounts`, `preview`, `otel`, `image`,
`imagePullSecrets` and `global`, so one values file can configure both
releases. The `atelet` and `objectStorePlugin.node` values are only in this
chart. `atelet.serviceAccount.annotations` and `atelet.podLabels` set
annotations on the atelet ServiceAccount and labels on the atelet pods, for
workload identity. `objectStorePlugin.node.args` and
`objectStorePlugin.node.startupProbe` replace the arguments and the startup
probe of the in-tree plugin, for another plugin.

Set `substrateReleaseName` to the substrate release's name. It defaults to
`substrate`. atelet's objects take their names from it, because the substrate
release grants permissions to the atelet ServiceAccount by name. atelet also
uses it to reach that release's API server. Names other than `substrate`
prefix each object with the release name, for example `team-atelet`.

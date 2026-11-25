# substrate-crds

Helm chart for installing the Agent Substrate CRDs.

Install this chart before the `substrate` and `substrate-atelet` charts:

```bash
helm upgrade --install substrate-crds ./charts/substrate-crds
```

Then install the other charts as described in the
[substrate chart README](../substrate/README.md).

The CRD YAMLs in `templates/` mirror `manifests/ate-install/generated/`.
Run `hack/verify/chart-generated.sh` to verify they are in sync.

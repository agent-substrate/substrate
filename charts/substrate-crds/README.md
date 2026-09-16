# substrate-crds

Helm chart for installing the Agent Substrate CRDs.

Install this chart before installing the main `substrate` chart:

```bash
helm upgrade --install substrate-crds ./charts/substrate-crds
helm upgrade --install substrate ./charts/substrate --namespace ate-system --create-namespace
```

The CRD YAMLs in `templates/` mirror `manifests/ate-install/generated/`.
Run `hack/verify/crd-chart.sh` to verify they are in sync.

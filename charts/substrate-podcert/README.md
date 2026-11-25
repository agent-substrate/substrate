# substrate-podcert

Install the Substrate certificate controller once per cluster, independently
of the application release. It issues service DNS and pod identity certificates
and publishes their cluster trust bundles. It does not require PostgreSQL or
a certificate issued by itself.

The cluster must enable `ClusterTrustBundle`, `ClusterTrustBundleProjection`,
and `PodCertificateRequest`, including the `certificates.k8s.io/v1beta1` API.
Create `service-dns-ca-pool`, `pod-identity-ca-pool`, and `postgres-ca-pool`
Secrets in the controller's namespace before installation. Each Secret must
contain a signing pool in its `pool` key. The existing setup command creates
these in the default namespace:

```sh
go run ./cmd/ate-setup create podcertificate-controller-cas
helm upgrade --install substrate-podcert ./charts/substrate-podcert \
  --namespace podcertificate-controller-system --create-namespace \
  --wait --timeout=5m
hack/install-postgres.sh --kind
```

Then prepare the application authentication resources and install the Substrate
application chart. The controller's namespace is the Helm release namespace;
if selecting another namespace, provision all CA-pool Secrets there.

Set `postgresClientNamespace` to the namespace containing the Substrate API
server. It defaults to `ate-system`. Set `postgresClientServiceAccount` to the API
server ServiceAccount name; it defaults to `ate-api-server`. Other application
release names prefix this name (for example, `team-ate-api-server`).

Use `postgresClients` to authorize additional service accounts to request fixed
PostgreSQL login identities. Entries use
`namespace/service-account=username[,username]` form. The controller refuses
the administrator login `postgres`.

Configure the controller image through `image.registry`, `image.repository`,
and `image.tag`. `global.imageRegistry` overrides the registry, and
`imagePullSecrets` and `global.imagePullSecrets` are merged for the controller pod.

Set `resources.requests` and `resources.limits` on this release to size the
controller container. Defaults are `10m` CPU and `64Mi` memory requests, with
`500m` CPU and `256Mi` memory limits. For example:

```yaml
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: "1"
    memory: 512Mi
```

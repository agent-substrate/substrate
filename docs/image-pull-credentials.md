# Image pull credentials

Actor container images are pulled by **atelet**, the node supervisor, not by
the kubelet. The kubelet's `imagePullSecrets` on the worker Pods therefore do
not apply to actor images. This guide covers how atelet authenticates to
registries and how to give it credentials for a private registry.

## How atelet resolves credentials

For each pull, atelet picks credentials in this order:

1. **GCP registries** (`gcr.io`, `*.gcr.io`, `pkg.dev`, `*.pkg.dev`): when
   atelet runs with `--gcp-auth-for-image-pulls=true` (the default in
   `manifests/ate-install/atelet.yaml`), it uses GCP application default
   credentials, i.e. the node's or Workload Identity's service account.
2. **Docker config file**: every other registry, and GCP registries when
   `--gcp-auth-for-image-pulls=false`, is resolved through the standard Docker
   credential keychain. It reads `$HOME/.docker/config.json` first, then
   `$DOCKER_CONFIG/config.json`, then Podman's `$REGISTRY_AUTH_FILE` or
   `$XDG_RUNTIME_DIR/containers/auth.json`.
3. **Anonymous**: when no config file exists or it has no entry for the
   registry.

The config file is re-read on every pull, so rotated credentials take effect
without restarting atelet.

## Pulling from a private registry

Create a `kubernetes.io/dockerconfigjson` Secret in `ate-system`, the same
kind of Secret the kubelet uses for `imagePullSecrets`:

```bash
kubectl -n ate-system create secret docker-registry actor-image-pull \
  --docker-server=registry.example.com \
  --docker-username="$USER" \
  --docker-password="$TOKEN"
```

Then mount it into the atelet DaemonSet and point `DOCKER_CONFIG` at the
mount. The Secret stores the file as `.dockerconfigjson`, so the volume must
project it under the name `config.json` the keychain looks for:

```yaml
# kustomize patch for manifests/ate-install/atelet.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: atelet
  namespace: ate-system
spec:
  template:
    spec:
      containers:
      - name: atelet
        env:
        - name: DOCKER_CONFIG
          value: /etc/atelet/docker
        volumeMounts:
        - name: docker-config
          mountPath: /etc/atelet/docker
          readOnly: true
      volumes:
      - name: docker-config
        secret:
          secretName: actor-image-pull
          items:
          - key: .dockerconfigjson
            path: config.json
```

Notes:

* One Secret can hold entries for several registries; each `auths` key is
  matched against the image's registry host.
* Short-lived tokens (for example ECR authorization tokens, which expire after
  12 hours) work if something refreshes the Secret. The kubelet propagates
  Secret updates to the mounted file within about a minute, and atelet picks
  up the new file on its next pull.
* `credHelpers` and `credsStore` entries in the config call an external
  helper binary. The atelet image does not ship any, so store the credentials
  inline in `auths` instead.
* An actor image is fetched onto the node once and shared by every actor on
  that node that references the same digest, so pull credentials grant access
  at the cluster level, not per actor.

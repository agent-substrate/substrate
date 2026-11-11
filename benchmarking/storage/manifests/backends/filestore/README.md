# Filestore agent volumes

This sets up [Filestore agent volumes](https://docs.cloud.google.com/filestore/docs/agent-substrate)
as the storage backend: a volume pool, the Filestore CSI driver and the
StorageClass that the scenarios use.

## Prerequisites

Volume pools are a preview, so ask the Filestore team to allowlist your
project before you create a pool. In a project that is not set up, a pool
reports `READY` but never gets volumes, and every first resume fails with
`429 no available volumes in the pool`.

You also need permission to create service accounts and IAM bindings for the
driver's Workload Identity, and the variables from
[Configure the environment](../../../README.md#configure-the-environment).

## 1. Turn on the Filestore API and Private Service Connect

```bash
gcloud services enable file.googleapis.com --project "${PROJECT_ID}"
gcloud network-connectivity service-connection-policies create filestore-agent-psc-policy \
  --project "${PROJECT_ID}" --region "${REGION}" --network "projects/${PROJECT_ID}/global/networks/default" \
  --subnets default --service-class google-cloud-filestore
```

## 2. Create a volume pool

Each volume is `defaultVolumeQuotaMib` in size, so the 1 GiB volumes need a
value of 1024:

```bash
curl -s -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "x-goog-user-project: ${PROJECT_ID}" \
  -H "Content-Type: application/json" \
  -d "{\"network\": \"projects/${PROJECT_ID}/global/networks/default\", \"defaultVolumeQuotaMib\": 1024}" \
  "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools?volumePoolId=${POOL}"
```

The reference results used a pool of up to 15 regional 1 TiB instances, each
with up to 1,000 volumes, and 15,000 spare volumes kept ready. The request
above does not set that layout. Ask the Filestore team for it to match the
reference first resume times.

Wait until the pool is `READY`:

```bash
curl -s -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "x-goog-user-project: ${PROJECT_ID}" \
  "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${POOL}" | jq -r .state
```

The density scenario uses one volume per actor, so the pool needs at least as
many free volumes as the most actors you run.

## 3. Install the Filestore CSI driver

Substrate needs its own copy of the driver. Turn off the GKE Filestore add-on,
then install the driver's Substrate overlay:

```bash
gcloud container clusters update "${CLUSTER}" --location "${ZONE}" --project "${PROJECT_ID}" \
  --update-addons GcpFilestoreCsiDriver=DISABLED --quiet
git clone https://github.com/kubernetes-sigs/gcp-filestore-csi-driver.git /tmp/gcp-filestore-csi-driver
/tmp/gcp-filestore-csi-driver/deploy/kubernetes/overlays/substrate/deploy.sh --project-id "${PROJECT_ID}"
kubectl get pods -n gcp-filestore-csi-driver
```

All pods should be `Running`. The overlay's
[README](https://github.com/kubernetes-sigs/gcp-filestore-csi-driver/tree/master/deploy/kubernetes/overlays/substrate)
explains the service account and the `CSIDriverConfig` it creates.

## 4. Create the StorageClass

```bash
envsubst < benchmarking/storage/manifests/backends/filestore/storage_class.yaml | kubectl apply -f -
```

## Cleanup

Delete the volume pool once no actor uses it:

```bash
curl -s -X DELETE -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "x-goog-user-project: ${PROJECT_ID}" \
  "https://file.googleapis.com/v1beta1/projects/${PROJECT_ID}/locations/${REGION}/volumePools/${POOL}"
kubectl delete storageclass "${STORAGE_CLASS}"
```

# Counter Demo

This is the microvm variant of examples/counter.  It has the same architecture, but the actor template is setup to use microvm encapsulation, not gVisor.

## How to Run on Agent Substrate

### 1. Build and Deploy

TODO: The one-shot script appears to have bitrotted to some extent.  We need to
get this supported in cmd/ate-setup.


```bash
KIND_CLUSTER_NAME=<cluster> ./hack/run-microvm-demo-kind.sh
```

Alternatively, you can use the install script to deploy the demo on an existing Substrate deployment, but you will need to use the install script's configuration variables to properly set the registry, storage backend, and other settings:
```
go run ./cmd/ate-setup deploy demo counter-microvm
```

### 2. Inspect the running demo

Now that the demo is deployed, you can see it in operation.

Read the logs from the driver pod:
```bash
kubectl logs -n ate-demo-counter-microvm -l app=counter-driver
```

You can see the driver pod creating the actor, then sitting in a loop calling the actor through the router (which wakes it up from suspended state), then putting the actor back to sleep.  Each time the driver pod calls the actor, it logs the response body that the actor sends, which shows that Substrate is preserving the counter (which is only kept in actor memory, not written to a database anywhere) across suspensions.  This works even if the actor is not resumed on the same machine where it was suspended.

### 3. Uninstall

```
go run ./cmd/ate-setup delete demo counter-microvm
```

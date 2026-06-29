# Accessing valkey directly

Valkey is the state store used by `ate-api-server` to track actor and worker records. Direct access is useful for debugging state issues.

> **Warning:** Avoid destructive commands (`FLUSHALL`, `DEL`, etc.) on a live cluster.

To open a `valkey-cli` session:

1. `kubectl exec -n=ate-system -it valkey-cluster-0 -- valkey-cli -h valkey-cluster-service -c --tls --cacert /etc/valkey-ca/ca.crt --cer
t /run/servicedns.podcert.ate.dev/credential-bundle.pem --key /run/servicedns.podcert.ate.dev/credential-bundle.pem`

# Mac-local Substrate lab

An experimental, single-user deployment: one persistent Apple container
machine runs systemd, k3s, PostgreSQL 18, ateapi and atecontroller. Native
macletd supervises disposable Virtualization.framework Mac guests. No Docker,
cloud credentials, or changes to Homebrew PostgreSQL, Lume or Tailscale.

## Prepare and run

Requires Apple Silicon, macOS 26+, Go 1.27, Swift/macOS SDK, `socat`, and enough
memory for an 8 GiB Linux machine plus a Mac guest. Use a trusted **powered-off**
Lume source bundle with HTTP `:8123/ready`. The smoke image is not a sanitized
production guest-agent image. Never use the active golden VM as the source.

From the repository root:

```sh
# Preserve existing exclude rules; add this line only if absent.
grep -qxF '/.amp/in/' "$(git rev-parse --git-path info/exclude)" ||
  printf '\n/.amp/in/\n' >> "$(git rev-parse --git-path info/exclude)"
bash hack/mac-lab/build.sh
export MAC_LAB_SOURCE="$PWD/.amp/in/e2e/source"
# Local fixture alias, not a registry download or disk checksum.
export MAC_LAB_IMAGE=local/maclet-e2e@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
bash hack/mac-lab/lab.sh deploy
bash hack/mac-lab/lab.sh start
bash hack/mac-lab/lab.sh health
.amp/in/mac-lab/smoke --pki "$PWD/.amp/in/mac-lab/pki" --image "$MAC_LAB_IMAGE"
```

`build.sh` extracts a checksum-pinned Apple container 1.5 package into
`.amp/in/mac-lab/dist`; it does not install over `/usr/local/bin/container`.
It refuses an unrelated running container runtime. Version 1.5's first-party
`k8s` plugin publishes on all host interfaces and lacks the needed certificate
feature-gate overrides, so this lab uses its persistent `machine` support.
Building an image does not upgrade an existing machine's operating system.

State, logs, binaries, seven-day ephemeral certificates, and the machine's
ext4 root live under `.amp/in/mac-lab`. PostgreSQL uses a Kubernetes local-path
PVC on that disk, **not a virtiofs-mounted Mac directory**. `deploy` retains
database credentials and authority pools. It refuses expired certificates;
certificate renewal is an explicit lab maintenance operation, not automatic.
Do not overwrite running binary mounts: stop the lab before rebuilding and
redeploying binaries. No automatic source-image provisioning is performed.

`start` registers a user-session launchd supervisor. systemd restarts k3s and
Kubernetes restarts the pods; launchd restarts native macletd and the loopback
forwards. After logging out or rebooting, restart the private container runtime
with `build.sh` (cached builds), then `lab.sh start`. This is not an unattended
production host service. `health` checks macletd, the Kubernetes API, and pod
rollouts; successful deployment is separately checked by the public API smoke.

## Endpoints and private networking

All addresses below are on **the Mac**, not the machine running your browser:

| Service | Address |
| --- | --- |
| Control API (mTLS gRPC) | `127.0.0.1:18443`, server name `api.ate-system.svc` |
| Kubernetes API | `127.0.0.1:16443` |
| macletd health/metrics | `127.0.0.1:19090` |
| HostRuntime (mTLS gRPC) | private container gateway `:9443` |
| Actor HTTP proxy | private container gateway, allocated port |

The gateway is detected from the machine route and checked against a real Mac
interface. It is `192.168.65.1` on the tested host. Only the ateapi pod gets a
`host.container.internal` hosts-file mapping to it. No global DNS, PF rules,
Tailscale settings, or public port forwards are installed. Native listeners
bind this private address, not `0.0.0.0`. Other locally hosted containers on
the same private network may reach the ports; mTLS protects HostRuntime, but
Actor HTTP is plaintext. This is a trusted single-user lab, not tenant isolation.

The Control API smoke registers an external Worker with explicit capacity,
creates an Atespace/ActorTemplate/Actor, cold boots it, probes its persisted
proxy endpoint, performs two pause/resume cycles with distinct local snapshot
receipts, then deletes everything and asserts NotFound. It never accesses the
store directly. To probe from Linux as well:

```sh
source hack/mac-lab/runtime.sh
COPYFILE_DISABLE=1 tar -C "$LAB/pki" -cf - operator.crt operator.key |
  machine tar -C /opt/substrate/tls -xf -
machine /opt/substrate/bin/mac-lab-smoke --endpoint 127.0.0.1:30443 \
  --pki /opt/substrate/tls --image "$MAC_LAB_IMAGE"
```

No Envoy/atenet ingress, Linux Actor workers, external checkpoint backend,
durable NFS, or cross-host failover are deployed/tested. Snapshot storage is
disabled; pause/resume uses host-local cold disk snapshots. Authentication is
enabled, but Substrate's experimental authorization flag remains off.

## Stop without destroying data

```sh
bash hack/mac-lab/lab.sh stop
```

Stop refuses while disposable Actor bundles remain: delete those Actors via
the Control API first. It unloads only this lab's supervisor and stops only
`substrate-lab`; it retains the disk/PVC/PKI and does not stop golden or unrelated
services. The private Apple container runtime remains available. Start again
with `lab.sh start`. If its private gateway changes, redeploy the networking
and certificates before resuming use.

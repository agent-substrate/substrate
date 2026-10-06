# maclet: one native Mac Actor per host process

This standalone Swift CLI owns a disposable macOS VM bundle and its
Virtualization.framework lifecycle. `cmd/macletd` wraps it with Substrate's
authenticated HostRuntime service; the Swift process remains the one owner of
each running VM. Neither command installs a host service or modifies Lume state.

## Prerequisites and build

- Apple Silicon, macOS 14+, Swift 6+, and the macOS SDK.
- APFS clonefile support, sufficient disk/RAM, and capacity for another macOS
  guest within the host's virtualization/licensing limits.
- A **powered-off**, trusted Lume macOS bundle containing `config.json`,
  `disk.img`, and `nvram.bin`. Keep it offline for the entire clone operation.
- A guest HTTP readiness endpoint and permission to reach the local VM network.

From this directory:

```sh
swift test
swift build -c release
MACLET="$(swift build -c release --show-bin-path)/maclet"
codesign --force --sign - --entitlements maclet.entitlements "$MACLET"
codesign --verify --strict "$MACLET"
```

No Go toolchain, external Swift dependencies, or administrator privileges are
needed for the CLI. Re-sign after each rebuild. Do not use `swift run` for the
VM lifecycle: it can replace the entitled executable.

## Create, cold boot, inspect, stop

```sh
# The destination parent must exist; the destination itself must not.
"$MACLET" create "$OFFLINE_LUME_BUNDLE" "$NEW_ACTOR_BUNDLE" actor-17

# Foreground supervisor: keep this process running while the guest runs.
# Arguments after BUNDLE: HTTP port, absolute path, readiness timeout in seconds.
"$MACLET" start "$NEW_ACTOR_BUNDLE" 8123 /ready 120

# In another terminal, using the same executable:
"$MACLET" status "$NEW_ACTOR_BUNDLE"
"$MACLET" stop "$NEW_ACTOR_BUNDLE"
```

`create` uses APFS copy-on-write clones, never hard links. It preserves the
installed image's hardware model and copies its required auxiliary boot storage
into a private `nvram.bin`; creating empty NVRAM is not a substitute for an
installed Mac's boot data. Each Actor gets a new `VZMacMachineIdentifier` and
locally administered unicast MAC, persisted in `actor.json` across cold boots.
Original guest users, SSH host keys, credentials, and disk identifiers remain
inside the copy: **this is not image sanitization**.

Source symlinks and visible open disk/NVRAM handles are rejected. This is a
best-effort `lsof` guard, not an interlock with Lume or other users' processes.
Never clone an active image or allow another program to start the source during
cloning. Destination directories are private and must not be shared with
untrusted users. Only `maclet` should open Actor bundles.

`start` holds an exclusive bundle lock for the VM's lifetime. A second owner is
rejected. It emits newline-delimited JSON and atomically updates `status.json`.
`status` reports `ready:false` when no owner remains, even after an unclean exit.
`stop` sends a run-specific request through `stop.json` and waits for the owner
to exit; an old request cannot stop a later boot. SIGINT/SIGTERM also request stop.
Maclet requests graceful shutdown, then **forces a power-off after 15 seconds**
if needed. This can lose unflushed guest writes; reliable graceful macOS shutdown
needs guest-agent cooperation.

## Readiness means an HTTP response from this guest

The CLI finds an unexpired IPv4 lease for the Actor's MAC in
`/var/db/dhcpd_leases`, then issues GET to that IP and the specified port/path.
Only 2xx responses pass. It bypasses HTTP proxies, rejects redirects, bounds
requests, and does not buffer the response body. No DHCP lease, SSH access, or
VZ `running` state alone makes an Actor ready. Timeout stops the guest and exits
nonzero. `ready:true` is the **cold-boot gate**, not a continuously refreshed
liveness check. The foreground process stays alive after readiness.

## Run as a Substrate Mac Worker

Build and sign `maclet` as above, then build the provider with Go 1.27+:

```sh
go build -o ./bin/macletd ./cmd/macletd
./bin/macletd \
  --listen-address=:9443 \
  --maclet-executable="$MACLET" \
  --state-directory="$PRIVATE_ACTOR_STATE" \
  --image="$DIGEST_PINNED_IMAGE" \
  --source-bundle="$OFFLINE_LUME_BUNDLE" \
  --advertise-host="$MAC_ADDRESS_REACHABLE_FROM_ATENET" \
  --proxy-listen-address=0.0.0.0:0 \
  --snapshot-provider-endpoint=unix:///var/run/ate-snapshot/provider.sock \
  --metrics-listen-address=:9090 \
  --tls-cert-file="$SERVER_CERT" \
  --tls-key-file="$SERVER_KEY" \
  --client-ca-file="$CONTROL_PLANE_CA"
```

Register an external Worker with sandbox class `macos-vz` and
`external_host.runtime_endpoint` set to the provider's TLS endpoint. The
control plane authenticates with its configured HostRuntime client certificate,
copies that endpoint into the Actor assignment. `macletd` accepts only its configured digest-pinned
image, serializes operations per Actor while allowing different Actors to boot
concurrently, and removes an Actor bundle only after termination succeeds.

## HostRuntime lifecycle semantics

- Plain `Activate` creates a personalized bundle from the offline image and
  cold-boots it. CPU millicores are rounded up to a VZ CPU count; CPU and memory
  are accepted together and checked by Virtualization.framework.
- `Pause(actor_uid, local_snapshot_name)` requests a graceful shutdown, force
  stops after the deadline if necessary, closes the proxy, retains the bundle,
  and records the name atomically. Repeating the name succeeds. A stopped VM
  rejects a conflicting name; after a successful local resume, the next pause
  advances the receipt to its newly minted name. `Activate` with that local
  name requires the matching retained bundle and cold-boots it.
- `Checkpoint(actor_uid, external_snapshot_uri)` stops the VM and captures a
  cold DISK snapshot (`disk.img`, `nvram.bin`, and personalized boot metadata).
  Data objects are uploaded before the checksummed manifest commit marker. The
  local bundle is removed only after commit. The durable URI receipt makes a
  retry idempotent and rejects another URI.
- `Activate` with an external URI fetches the manifest first, verifies schema,
  image digest, regular-file sizes, and SHA-256 sums, then creates a newly
  personalized bundle. `Discard` stops and removes bundle and receipts;
  `Terminate` is its compatibility alias. Both are idempotent.

These are filesystem/boot-state snapshots, not VZ live saves: guest memory and
process execution are not preserved. Snapshot staging is private and transient;
the Actor bundle is never external durable storage. The Go service exposes a
`snapshotProvider` injection seam implementing `objectstoresnapshot.v1.NodeProvider`.
Production startup dials that provider over the host-local Unix socket configured
by `--snapshot-provider-endpoint`; without one external checkpoint/restore fails
closed. The provider owns cloud credentials, compression, retries, and durable
storage. Data files land before `manifest.json`, which is the commit marker.

Durability currently begins only when `SUSPEND` commits that external DISK
snapshot. `RUNNING` and `PAUSED` root disks remain local to one Mac and can be
lost with that host; Mac template volumes are rejected rather than pretending
to offer independently durable storage. Workloads needing write-through
durability must use a network service from inside the guest until a managed
VirtioFS or network-volume contract is implemented.

The control-plane lifecycle maps to the host as follows:

| Actor state | Mac host state |
| --- | --- |
| `RUNNING` | VZ VM running; `macletd` publishes a reconciled workload proxy. |
| `PAUSED` | VM cold-stopped; bundle retained on, and placement pinned to, the same Mac Worker. |
| `SUSPENDED` | DISK snapshot committed externally; no VM bundle or Worker assignment remains. |
| `CRASHED` | Control plane records runtime loss; revert discards any local bundle and returns to the last external snapshot. |
| `DELETING` | Host discard, volume/snapshot cleanup, and Worker release are retried before the Actor row is removed. |

`macletd` persists published proxy ports and reconciles them before serving gRPC,
so a daemon upgrade does not invalidate the control plane's Actor endpoint while
the independent `maclet` VM-owner process remains running. New TLS handshakes reload the
server identity and client trust bundle, and the control-plane client likewise
reloads its identity and server trust. `/metrics`, `/healthz`, and `/readyz` are
served on `--metrics-listen-address`; gRPC client/server metrics cover every
HostRuntime and snapshot-provider operation.

The hardware-backed public-API test is `TestMacActorE2E` under
`cmd/ateapi/internal/controlapi/functionaltest`. Its `ATE_MAC_ACTOR_E2E_*`
environment variables identify the provider, image, and mTLS files; set
`ATE_MAC_ACTOR_E2E_REQUIRED=true` so missing configuration fails rather than
skips.

The port/path/timeout correspond to the API's `mac_vm.wakeup_probe`, but the CLI
does not consume ActorTemplate protobufs yet. The image must actually provide
the endpoint; maclet does not assume guest credentials or install an agent.

For a disposable smoke guest only, `Tests/Fixtures/readiness.py` serves `/ready`
on port 8123 using Python's standard library. Inspect the guest image to establish
the SSH user/key and pin its SSH host public key before copying/running this
fixture. Remove old provider/autostart agents **in the clone only** before its
first networked boot. A successful fixture probe demonstrates the host lifecycle
and network path, not Substrate guest-agent or workload readiness.

## Remaining production work

- Fetch and verify the digest-pinned OCI VM artifact; this CLI imports only
  a local offline Lume bundle.
- Produce a sanitized image with full Xcode and an authenticated guest agent;
  provision unique guest credentials and a real workload readiness endpoint.
- Replace shared VZ NAT/DHCP discovery with the Actor-isolated network/routing
  contract. NAT here does not enforce tenant isolation or egress policy.
- Add host-wide capacity management, service supervision, crash reconciliation,
  and durable image lifecycle.

`swift test` exercises configuration and identity validation, independent
disk/NVRAM clones, open-source refusal, ownership/stale readiness, exact DHCP
lease matching and expiry, and HTTP probe boundaries. It does not boot a VM.
Real boot testing requires the explicitly selected disposable image above.

## Local-first placement and EC2 warm capacity

Mac Workers should register with the same `macos-vz` sandbox class and a
provider label, for example `provider=local` or `provider=aws`. A Mac
`ActorTemplate` can give `provider=local` a worker-preference weight while
leaving AWS Workers eligible as overflow capacity. The scheduler uses EC2 only
when no preferred Worker with room exists.

`atecontroller` can optionally warm an existing EC2 Mac Auto Scaling Group:

```text
--ec2-mac-autoscaling-group=mac-workers
--ec2-mac-local-selector=provider=local
--ec2-mac-cloud-selector=provider=aws
--ec2-mac-stress-utilization=0.75
--ec2-mac-stress-samples=3
--ec2-mac-lead-time=20m
```

Warm provisioning is disabled unless `--ec2-mac-autoscaling-group` is set. It
scales up one Worker when local Actor-slot utilization remains above the
threshold or recent allocation growth projects exhaustion within the lead
time, provided ready EC2 Workers lack the configured headroom. The controller
uses the standard AWS credential chain and needs
`autoscaling:DescribeAutoScalingGroups` and `autoscaling:SetDesiredCapacity`.
The Auto Scaling Group launch configuration must bootstrap `macletd` and
register its external Worker with `provider=aws`.

Automatic scale-down is intentionally absent. An EC2 Mac Worker must first be
drained without assignments, and its Dedicated Host has a 24-hour minimum
allocation; reducing the Auto Scaling Group blindly could terminate an active
Actor or incur churn without saving money.

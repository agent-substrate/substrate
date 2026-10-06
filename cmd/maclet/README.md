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
  --tls-cert-file="$SERVER_CERT" \
  --tls-key-file="$SERVER_KEY" \
  --client-ca-file="$CONTROL_PLANE_CA"
```

Register an external Worker with sandbox class `macos-vz` and
`external_host.runtime_endpoint` set to the provider's TLS endpoint. The
control plane authenticates with its configured HostRuntime client certificate,
copies that endpoint into the Actor assignment, and calls `Activate` and
`Terminate` idempotently. `macletd` accepts only its configured digest-pinned
image, serializes operations per Actor while allowing different Actors to boot
concurrently, and removes an Actor bundle only after termination succeeds.

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
  FULL snapshot/resume, resource-limit enforcement, and durable image lifecycle.

`swift test` exercises configuration and identity validation, independent
disk/NVRAM clones, open-source refusal, ownership/stale readiness, exact DHCP
lease matching and expiry, and HTTP probe boundaries. It does not boot a VM.
Real boot testing requires the explicitly selected disposable image above.
